package business

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"

	constant "mongo-to-solr/Config"
	"mongo-to-solr/MongoService/models"

	"github.com/jackc/pgx/v5"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	mongoBatchSize = 500 // fetch from Mongo
	solrBatchSize  = 500 // send to Solr in bigger batches

	solrUsername = "admin"
	solrPassword = "DuSVIpjVO1"
	solrBaseURL  = "https://solr.uat.sitc.com.sa/solr/hotels_collection"
)

var (
	totalSent  int64
	httpClient = &http.Client{Timeout: 2 * time.Minute}
)

func HotelUpdateSolr() {
	start := time.Now()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Hour)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(constant.MongoConnectionString))
	if err != nil {
		log.Fatalf("MongoDB connection error: %v", err)
	}
	defer client.Disconnect(ctx)

	// Step 1: Fetch existing Solr IDs once
	log.Println("🔍 Fetching existing Solr hotel IDs...")
	solrIDs, err := fetchAllSolrIDs()
	if err != nil {
		log.Fatalf("Error fetching Solr IDs: %v", err)
	}
	log.Printf("✅ Loaded %d hotel IDs from Solr into memory", len(solrIDs))
	// hotelIDs, err := fetchTableHotelCuratedDataFromPostgresql()
	// if err != nil {
	// 	log.Fatalf("Error fetching Postgresql IDs: %v", err)
	// }
	// log.Printf("✅ Loaded %d hotel IDs from Postgresql into memory", len(hotelIDs))
	// hotelIDsMap := make(map[int]struct{})
	// for _, hotel := range hotelIDs {
	// 	hotelIDInt, err := strconv.Atoi(hotel.HotelID)
	// 	if err == nil {
	// 		hotelIDsMap[hotelIDInt] = struct{}{}
	// 	}
	// }

	// Setup Solr update URL (commit within 60s instead of commit=true)
	// solrUpdateURL := solrBaseURL + "/update?commitWithin=60000"
	// helper := NewPostgresCopyHelperHotelCuratedDataCacheSolr()

	// Stream Mongo and write directly without worker pool
	// err = streamMongoRecords(ctx, client, hotelIDsMap, func(batch []models.PostgresqlHotelCuratedDataCacheSolr) {
	// 	helper.WriteToDatabaseHotelCuratedDataCacheSolr(batch)
	// })
	// if err != nil {
	// 	log.Fatalf("Streaming error: %v", err)
	// }

	//Solr
	err = streamMongoRecords(ctx, client, solrIDs, func(batch []models.HotelMappingV2) {
		bulkSendToSolr(constant.SolrURL+"/"+constant.SolrHotelMappingV2+"/update?commitWithin=60000", batch)
	})
	if err != nil {
		log.Fatalf("Streaming error: %v", err)
	}

	log.Printf("🏁 Solr update complete in %s, total sent: %d", time.Since(start), totalSent)
}

// Fetch all hotel IDs from Solr once
func fetchAllSolrIDs() (map[int]struct{}, error) {
	found := make(map[int]struct{})
	cursorMark := "*"
	more := true
	page := 0

	for more {
		params := map[string]string{
			"q":          "*:*",
			"fl":         "id",
			"rows":       "1000", // bigger page size
			"wt":         "json",
			"cursorMark": cursorMark,
			"sort":       "id+asc",
		}

		solrQueryURL := fmt.Sprintf("%s/select?q=%s&fl=%s&rows=%s&wt=%s&cursorMark=%s&sort=%s",
			solrBaseURL,
			params["q"],
			params["fl"],
			params["rows"],
			params["wt"],
			params["cursorMark"],
			params["sort"],
		)

		req, err := http.NewRequest("GET", solrQueryURL, nil)
		if err != nil {
			return nil, err
		}
		req.SetBasicAuth(solrUsername, solrPassword)

		resp, err := httpClient.Do(req)
		if err != nil {
			return nil, err
		}
		if resp.StatusCode >= 400 {
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			return nil, fmt.Errorf("solr query error (%d): %s", resp.StatusCode, body)
		}

		var solrResp struct {
			Response struct {
				Docs []struct {
					ID string `json:"id"`
				} `json:"docs"`
			} `json:"response"`
			NextCursorMark string `json:"nextCursorMark"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&solrResp); err != nil {
			resp.Body.Close()
			return nil, fmt.Errorf("solr decode error: %v", err)
		}
		resp.Body.Close()

		for _, doc := range solrResp.Response.Docs {
			idInt, err := strconv.Atoi(doc.ID)
			if err == nil {
				found[idInt] = struct{}{}
			}
		}

		page++
		if solrResp.NextCursorMark == cursorMark || len(solrResp.Response.Docs) == 0 {
			more = false
		} else {
			cursorMark = solrResp.NextCursorMark
		}
	}

	return found, nil
}

// Stream Mongo records and send missing ones to handler
func streamMongoRecords(
	ctx context.Context,
	client *mongo.Client,
	solrIDs map[int]struct{},
	handler func(batch []models.HotelMappingV2),
) error {
	coll := client.Database("HotelStaticData").Collection("hotel_data_en")

	projection := bson.M{
		"hotel_id":            1,
		"name":                1,
		"address_line1":       1,
		"lat":                 1,
		"long":                1,
		"city_name":           1,
		"country_code":        1,
		"state_province_name": 1,
		"country_name":        1,
	}
	opts := options.Find().
		SetBatchSize(mongoBatchSize).
		SetProjection(projection).
		SetNoCursorTimeout(true)

	cursor, err := coll.Find(ctx, bson.M{}, opts)
	if err != nil {
		return fmt.Errorf("MongoDB query error: %v", err)
	}
	defer cursor.Close(ctx)

	var batch []models.HotelMappingV2
	var count, skipped, inserted int

	for cursor.Next(ctx) {
		var Solrdoc models.HotelMappingV2
		var doc models.HotelMappingV2
		if err := cursor.Decode(&Solrdoc); err != nil {
			log.Printf("Decode error: %v", err)
			continue
		}

		// Skip if already in Solr
		if _, exists := solrIDs[Solrdoc.HotelId]; exists {
			skipped++
			continue
		}

		doc.HotelId = Solrdoc.HotelId
		doc.Name = Solrdoc.Name
		// doc.CityID = Solrdoc.CityID
		doc.CityName = Solrdoc.CityName
		doc.CountryName = Solrdoc.CountryName
		doc.Latitude = Solrdoc.Latitude
		doc.Longitude = Solrdoc.Longitude
		doc.CountryCode = Solrdoc.CountryCode

		// Build search fields only when inserting
		doc.SearchQuery = doc.Name + " " + doc.CityName + " " + doc.StateName + " " + doc.CountryName
		doc.TextNGram = doc.SearchQuery
		doc.Location = fmt.Sprintf("%f,%f", doc.Latitude, doc.Longitude)

		// Mark this hotel_id as seen to avoid duplicates within the same run
		solrIDs[Solrdoc.HotelId] = struct{}{}

		batch = append(batch, doc)
		if len(batch) >= solrBatchSize {
			handler(batch)
			inserted += len(batch)
			batch = batch[:0]
		}

		count++
	}

	// final flush
	if len(batch) > 0 {
		handler(batch)
		inserted += len(batch)
	}

	log.Printf("✅ Processed: %d, inserted: %d, skipped: %d", count, inserted, skipped)
	return nil
}

// Bulk send to Solr
func bulkSendToSolr(solrURL string, batch []models.HotelMappingV2) error {
	jsonData, err := json.Marshal(batch)
	if err != nil {
		return fmt.Errorf("Marshal error: %v", err)
	}

	req, err := http.NewRequest("POST", solrURL, bytes.NewBuffer(jsonData))
	if err != nil {
		return fmt.Errorf("Request error: %v", err)
	}
	req.SetBasicAuth(solrUsername, solrPassword)
	req.Header.Set("Content-Type", "application/json")

	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("Solr HTTP error: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("Solr error (%d): %s", resp.StatusCode, body)
	}

	atomic.AddInt64(&totalSent, int64(len(batch)))
	log.Printf("📦 Sent batch to Solr (size: %d), total sent: %d", len(batch), atomic.LoadInt64(&totalSent))
	return nil
}

// Posgresql Insert

// Postgresql Hotel Curated Data Cache Solr Fetch
func fetchTableHotelCuratedDataFromPostgresql() ([]models.PostgresqlHotelCuratedDataCacheSolr, error) {
	var hotels []models.PostgresqlHotelCuratedDataCacheSolr
	conn, err := pgx.Connect(context.Background(), "host=35.200.163.147 port=6432 user=postgres password=4VUeFC6Yav dbname=TMC_DEV sslmode=disable")
	if err != nil {
		return nil, fmt.Errorf("❌ PostgreSQL connection error: %v", err)
	}
	defer conn.Close(context.Background())

	rows, err := conn.Query(context.Background(), "SELECT hotel_id FROM hotel.hotel_curated_data_cache_solr_v2")
	if err != nil {
		return nil, fmt.Errorf("❌ Query error: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var hotel models.PostgresqlHotelCuratedDataCacheSolr
		err := rows.Scan(
			&hotel.HotelID,
		)
		if err != nil {
			return nil, fmt.Errorf("❌ Scan error: %v", err)
		}
		// (Optionally) You could store priority, isActive, synonyms if desired
		hotels = append(hotels, hotel)
	}
	return hotels, nil
}

// WriteToDatabase performs a bulk insert using COPY FROM
func (h *PostgresCopyHelper) WriteToDatabaseHotelCuratedDataCacheSolr(entities []models.PostgresqlHotelCuratedDataCacheSolr) {
	if len(entities) == 0 {
		fmt.Println("No records to insert.")
		return
	}

	conn, err := pgx.Connect(context.Background(), h.connStr)
	if err != nil {
		log.Fatalf("❌ PostgreSQL connection error: %v", err)
	}
	defer conn.Close(context.Background())

	// Deduplicate by hotel_id within this batch to avoid duplicate COPY rows
	seen := make(map[string]struct{}, len(entities))
	rows := make([][]interface{}, 0, len(entities))
	for _, e := range entities {
		if e.HotelID == "" {
			continue
		}
		if _, ok := seen[e.HotelID]; ok {
			continue
		}
		seen[e.HotelID] = struct{}{}
		rows = append(rows, []interface{}{
			e.HotelID,
			e.HotelNameEn,
			e.CityID,
			e.CityNameEn,
			e.CountryNameEn,
			e.Latitude,
			e.Longitude,
			e.CountryCode,
		})
	}

	copyCount, err := conn.CopyFrom(
		context.Background(),
		pgx.Identifier{h.schema, h.tableName}, // table name
		[]string{
			"hotel_id",
			"hotel_name_en",
			"city_id",
			"city_name_en",
			"country_name_en",
			"latitude",
			"longitude",
			"country_code",
		},
		pgx.CopyFromRows(rows),
	)

	if err != nil {
		log.Fatalf("❌ Bulk insert failed: %v", err)
	}

	fmt.Printf("✅ Successfully inserted %d records using COPY!\n", copyCount)
}

// NewPostgresCopyHelper initializes a new helper with connection info
func NewPostgresCopyHelperHotelCuratedDataCacheSolr() *PostgresCopyHelper {
	connStr := "host=35.200.163.147 port=6432 user=postgres password=4VUeFC6Yav dbname=TMC_DEV sslmode=disable"
	return &PostgresCopyHelper{
		connStr:   connStr,
		schema:    "hotel",                            // schema name
		tableName: "hotel_curated_data_cache_solr_v2", // table name
	}
}
