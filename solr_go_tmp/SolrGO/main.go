package main

import (
	business "mongo-to-solr/Business"
)

func main() {
	// business.LocationUpdateSolr()
	business.HotelUpdateSolr()
	// business.SaveHotelMappingv2Bulk()
}

// package main

// import (
// 	"bytes"
// 	"fmt"
// 	"net/http"
// )

// func main() {
// 	solrURL := "https://solr.uat.sitc.com.sa/solr/collection_geo_location/update?commit=true"
// 	deleteQuery := `{"delete":{"query":"*:*"}}`

// 	// Replace with your Solr credentials
// 	username := "admin"
// 	password := "DuSVIpjVO1"

// 	// Create request
// 	req, err := http.NewRequest("POST", solrURL, bytes.NewBuffer([]byte(deleteQuery)))
// 	if err != nil {
// 		panic(err)
// 	}
// 	req.Header.Set("Content-Type", "application/json")
// 	req.SetBasicAuth(username, password) // add basic auth

// 	// Send request
// 	client := &http.Client{}
// 	resp, err := client.Do(req)
// 	if err != nil {
// 		panic(err)
// 	}
// 	defer resp.Body.Close()

// 	fmt.Println("Response status:", resp.Status)
// }
