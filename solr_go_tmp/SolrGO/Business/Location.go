package business

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	constant "mongo-to-solr/Config"
	"mongo-to-solr/MongoService/models"

	"github.com/jackc/pgx/v5"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Main function: fetch from Mongo in batches and insert into Solr in batches of 40, only for records not already in Solr
func LocationUpdateSolr() {
	const batchSize = 500

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Hour)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(constant.MongoConnectionString))
	if err != nil {
		log.Fatalf("MongoDB connection error: %v", err)
	}
	defer client.Disconnect(ctx)

	// Step 1: Fetch existing Solr IDs once
	log.Println("🔍 Fetching existing Solr location IDs...")
	//Solr ID Fetch
	solrIDs, err := fetchAllSolrLocationIDs()
	if err != nil {
		log.Fatalf("Error fetching Solr IDs: %v", err)
	}
	log.Printf("✅ Loaded %d location IDs from Solr into memory", len(solrIDs))

	//Postgresql ID Fetch
	// postgresqlIDs, err := fetchTableCityDataFromPostgresql()
	// if err != nil {
	// 	log.Fatalf("Error fetching Postgresql IDs: %v", err)
	// }
	// log.Printf("✅ Loaded %d location IDs from Postgresql into memory", len(postgresqlIDs))
	// postgresqlIDsMap := make(map[string]struct{})
	// cityCodes := make([]string, 0, len(postgresqlIDs))
	// for _, city := range postgresqlIDs {
	// 	postgresqlIDsMap[city.CityCode] = struct{}{}
	// 	cityCodes = append(cityCodes, city.CityCode)
	// }

	// collection := client.Database("HotelStaticData").Collection("geo_location")
	// filter := bson.M{
	// 	"geo_location_id": bson.M{"$in": cityCodes}, // filter by list
	// }
	// Helper to split and trim by comma
	// Enhanced splitAndTrim to handle Arabic and other RTL text correctly.
	splitAndTrim := func(s string) []string {
		raw := make([]string, 0)
		// Use Unicode-aware trimming and splitting
		// Also handle Arabic/RTL comma (U+060C) in addition to standard comma
		separators := []rune{',', '،'} // standard comma and Arabic comma
		parts := []string{s}
		for _, sep := range separators {
			var temp []string
			for _, p := range parts {
				temp = append(temp, strings.Split(p, string(sep))...)
			}
			parts = temp
		}
		for _, part := range parts {
			trimmed := strings.TrimSpace(part)
			if trimmed != "" {
				raw = append(raw, trimmed)
			}
		}
		return raw
	}

	// Prepare Solr URL and authentication
	// solrURL := "https://solr.uat.sitc.com.sa/solr/geo_location_collection/update?commit=true"
	// solrUsername := "admin"
	// solrPassword := "DuSVIpjVO1"

	findOpts := options.Find().SetBatchSize(batchSize)
	cursor, err := client.Database(constant.MongoDatabase).Collection(constant.MongoGeoRegion).Find(ctx, bson.M{}, findOpts)
	if err != nil {
		log.Fatal("Mongo find error:", err)
	}
	defer cursor.Close(ctx)

	var solrBatch []models.LocationDataSolr
	// var postgresqlBatch []models.PorgresGeoLocation
	// helper := NewPostgresCopyHelper()
	count, skipped, inserted := 0, 0, 0

	for cursor.Next(ctx) {
		var hotel models.GeoRegion
		if err := cursor.Decode(&hotel); err != nil {
			log.Printf("Cursor decode error: %v", err)
			continue
		}

		// Skip if already in Solr
		if _, exists := solrIDs[hotel.GeoRegionID]; exists {
			skipped++
			continue
		}

		//Mongo
		// if _, exists := postgresqlIDsMap[hotel.GeoRegionID]; !exists {
		// 	skipped++
		// 	continue
		// }

		langNames := map[string]string{
			"en": hotel.NameFull,
			"ar": hotel.ArFullName,
			"fr": hotel.FrFullName,
			"pt": hotel.PtFullName,
			"cn": hotel.CnFullName,
			"es": hotel.EsFullName,
		}

		// Parse from the end (backward): country, state, city, location
		locationNames := make(map[string]string)
		commonNames := make(map[string]string)
		cityNames := make(map[string]string)
		stateNames := make(map[string]string)
		countryNames := make(map[string]string)

		for lang, fullName := range langNames {
			parts := splitAndTrim(fullName)
			location, city, state, country := "", "", "", ""
			n := len(parts)
			if n > 0 {
				country = parts[n-1]
			}
			if n > 1 {
				state = parts[n-2]
			}
			if n > 2 {
				city = parts[n-3]
			}
			if n > 3 {
				location = parts[n-4]
			}
			locationNames[lang] = location
			cityNames[lang] = city
			stateNames[lang] = state
			countryNames[lang] = country
			// Only set commonNames if exactly one of location, city, state is non-empty
			if location != "" {
				commonNames[lang] = location
				continue
			} else if city != "" {
				commonNames[lang] = city
				continue
			} else if state != "" {
				commonNames[lang] = state
				continue
			}
		}

		// Build SearchQuery by concatenating all location, city, state, country names in all languages
		// Solr Search Query
		searchQuery := locationNames["en"] + " " + cityNames["en"] + " " + stateNames["en"] + " " + countryNames["en"] +
			" " + locationNames["fr"] + " " + cityNames["fr"] + " " + stateNames["fr"] + " " + countryNames["fr"] +
			" " + locationNames["ar"] + " " + cityNames["ar"] + " " + stateNames["ar"] + " " + countryNames["ar"] +
			" " + locationNames["pt"] + " " + cityNames["pt"] + " " + stateNames["pt"] + " " + countryNames["pt"] +
			" " + locationNames["cn"] + " " + cityNames["cn"] + " " + stateNames["cn"] + " " + countryNames["cn"] +
			" " + locationNames["es"] + " " + cityNames["es"] + " " + stateNames["es"] + " " + countryNames["es"]

		solrBatch = append(solrBatch, models.LocationDataSolr{
			GeoRegionID: hotel.GeoRegionID,

			LocationNameEn: locationNames["en"],
			LocationNameFr: locationNames["fr"],
			LocationNameAr: locationNames["ar"],
			LocationNamePt: locationNames["pt"],
			LocationNameCn: locationNames["cn"],
			LocationNameEs: locationNames["es"],

			CityNameEn: cityNames["en"],
			CityNameFr: cityNames["fr"],
			CityNameAr: cityNames["ar"],
			CityNamePt: cityNames["pt"],
			CityNameCn: cityNames["cn"],
			CityNameEs: cityNames["es"],

			StateNameEn: stateNames["en"],
			StateNameFr: stateNames["fr"],
			StateNameAr: stateNames["ar"],
			StateNamePt: stateNames["pt"],
			StateNameCn: stateNames["cn"],
			StateNameEs: stateNames["es"],

			CountryNameEn: countryNames["en"],
			CountryNameFr: countryNames["fr"],
			CountryNameAr: countryNames["ar"],
			CountryNamePt: countryNames["pt"],
			CountryNameCn: countryNames["cn"],
			CountryNameEs: countryNames["es"],
			HotelCount:    hotel.HotelCount,
			CountryCode:   hotel.CountryCode,
			LocationType:  hotel.Type,
			SearchQuery:   searchQuery,
			Location:      fmt.Sprintf("%s,%s", hotel.Latitude, hotel.Longitude),
			TextNGram:     searchQuery,
		})

		//Posgresql Insert
		// postgresqlBatch = append(postgresqlBatch, models.PorgresGeoLocation{
		// 	CityID:        hotel.LocationID,
		// 	CityCode:      hotel.GeoRegionID,
		// 	CityNameEn:    commonNames["en"],
		// 	CityNameAr:    commonNames["ar"],
		// 	CountryNameEn: countryNames["en"],
		// 	CountryNameAr: countryNames["ar"],
		// 	CountryCode:   hotel.CountryCode,
		// 	CountryNameFr: countryNames["fr"],
		// 	CountryNameCh: countryNames["cn"],
		// 	CityNameFr:    commonNames["fr"],
		// 	CityNameCh:    commonNames["cn"],
		// })
		if len(solrBatch) == batchSize {
			insertBatchToSolr(solrBatch, constant.SolrURL+"/"+constant.SolrGeoRegion+"/update?commit=true", constant.SolrUser, constant.SolrPassword)
			// helper.WriteToDatabase(postgresqlBatch)
			inserted += len(solrBatch)
			fmt.Printf("Inserted batch: %d, Total inserted: %d\n", len(solrBatch), inserted)
			// solrBatch = solrBatch[:0]             // reset batch
			solrBatch = solrBatch[:0] // reset batch
		}

		count++
	}

	// Insert any remaining records
	if len(solrBatch) > 0 {
		insertBatchToSolr(solrBatch, constant.SolrURL, constant.SolrUser, constant.SolrPassword)
		// helper.WriteToDatabase(postgresqlBatch)
		inserted += len(solrBatch)
	}

	if err := cursor.Err(); err != nil {
		log.Fatal("Cursor error:", err)
	}

	log.Printf("✅ Location processing complete - Total processed: %d, inserted: %d, skipped (already in Solr): %d", count, inserted, skipped)
}

// Fetch all location IDs from Solr once
func fetchAllSolrLocationIDs() (map[string]struct{}, error) {
	found := make(map[string]struct{})
	cursorMark := "*"
	more := true
	page := 0

	solrBaseURL := "https://solr.uat.sitc.com.sa/solr/geo_location_collection"
	solrUsername := "admin"
	solrPassword := "DuSVIpjVO1"
	httpClient := &http.Client{Timeout: 2 * time.Minute}

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
			return nil, fmt.Errorf("Solr query error (%d): %s", resp.StatusCode, body)
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
			return nil, fmt.Errorf("Solr decode error: %v", err)
		}
		resp.Body.Close()

		for _, doc := range solrResp.Response.Docs {
			found[doc.ID] = struct{}{}
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

func insertBatchToSolr(batch []models.LocationDataSolr, solrURL, solrUsername, solrPassword string) {
	jsonData, err := json.Marshal(batch)
	if err != nil {
		log.Fatal("JSON marshal error:", err)
	}

	req, err := http.NewRequest("POST", solrURL, bytes.NewBuffer(jsonData))
	if err != nil {
		log.Fatal("Failed to create HTTP request:", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.SetBasicAuth(solrUsername, solrPassword)

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		fmt.Printf("Solr POST error: %v\n", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		fmt.Printf("Batch of %d records successfully inserted into Solr!\n", len(batch))
	} else {
		body, _ := io.ReadAll(resp.Body)
		fmt.Printf("Failed to insert batch into Solr. Status: %s\nError message: %s\n", resp.Status, string(body))
	}
}

//Postgresql Insert

type PostgresCopyHelper struct {
	connStr   string
	schema    string
	tableName string
}

// NewPostgresCopyHelper initializes a new helper with connection info
func NewPostgresCopyHelper() *PostgresCopyHelper {
	connStr := "host=35.200.163.147 port=6432 user=postgres password=4VUeFC6Yav dbname=TMC_DEV sslmode=disable"
	return &PostgresCopyHelper{
		connStr:   connStr,
		schema:    "hotel",              // schema name
		tableName: "city_table_solr_v2", // table name
	}
}

// WriteToDatabase performs a bulk insert using COPY FROM
func (h *PostgresCopyHelper) WriteToDatabase(entities []models.PorgresGeoLocation) {
	if len(entities) == 0 {
		fmt.Println("No records to insert.")
		return
	}

	conn, err := pgx.Connect(context.Background(), h.connStr)
	if err != nil {
		log.Fatalf("❌ PostgreSQL connection error: %v", err)
	}
	defer conn.Close(context.Background())

	rows := make([][]interface{}, 0, len(entities))
	for _, e := range entities {
		rows = append(rows, []interface{}{
			e.CityID,
			e.CityCode,
			e.CityNameEn,
			e.CityNameAr,
			e.CountryNameEn,
			e.CountryNameAr,
			e.CountryCode,
			e.CountryNameFr,
			e.CountryNameCh,
			e.CountryNameGe,
			e.CountryNameUr,
			e.CityNameFr,
			e.CityNameCh,
			e.CityNameGe,
			e.CityNameUr,
		})
	}

	copyCount, err := conn.CopyFrom(
		context.Background(),
		pgx.Identifier{h.schema, h.tableName}, // table name
		[]string{
			"city_id",
			"city_code",
			"city_name_en",
			"city_name_ar",
			"country_name_en",
			"country_name_ar",
			"country_code",
			"country_name_fr",
			"country_name_ch",
			"country_name_ge",
			"country_name_ur",
			"city_name_fr",
			"city_name_ch",
			"city_name_ge",
			"city_name_ur",
		},
		pgx.CopyFromRows(rows),
	)

	if err != nil {
		log.Fatalf("❌ Bulk insert failed: %v", err)
	}

	fmt.Printf("✅ Successfully inserted %d records using COPY!\n", copyCount)
}

func fetchTableCityDataFromPostgresql() ([]models.PorgresGeoLocation, error) {
	var cities []models.PorgresGeoLocation
	conn, err := pgx.Connect(context.Background(), "host=35.200.163.147 port=6432 user=postgres password=4VUeFC6Yav dbname=TMC_DEV sslmode=disable")
	if err != nil {
		return nil, fmt.Errorf("❌ PostgreSQL connection error: %v", err)
	}
	defer conn.Close(context.Background())

	rows, err := conn.Query(context.Background(), "SELECT * FROM hotel.city_table_solr_v2 where city_name_en='' OR city_name_en is null")
	if err != nil {
		return nil, fmt.Errorf("❌ Query error: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var city models.PorgresGeoLocation
		var priority *int
		var isActive bool
		var synonyms sql.NullString
		err := rows.Scan(
			&city.CityID,
			&city.CityCode,
			&city.CityNameEn,
			&city.CityNameAr,
			&city.CountryNameEn,
			&city.CountryNameAr,
			&priority,
			&isActive,
			&synonyms,
			&city.CountryCode,
			&city.CountryNameFr,
			&city.CountryNameCh,
			&city.CountryNameGe,
			&city.CountryNameUr,
			&city.CityNameFr,
			&city.CityNameCh,
			&city.CityNameGe,
			&city.CityNameUr,
		)
		if err != nil {
			return nil, fmt.Errorf("❌ Scan error: %v", err)
		}
		// (Optionally) You could store priority, isActive, synonyms if desired
		cities = append(cities, city)
	}
	return cities, nil
}
