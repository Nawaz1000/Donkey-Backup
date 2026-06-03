package config

const (
	// Solr configuration
	SolrURL               = "https://solr.uat.sitc.com.sa/solr"
	SolrHotelMappingV2    = "hotels_collection"
	SolrGeoRegion         = "geo_location_collection"
	SolrUser              = "admin"
	SolrPassword          = "DuSVIpjVO1"
	SolrTotalResultUpdate = 40

	// Mongo configuration
	MongoDatabase         = "HotelStaticData"
	MongoHotelMappingV2   = "hotel_data_en"
	MongoGeoRegion        = "geo_location"
	MongoConnectionString = "mongodb://localhost:27017/"

	// Bot ID
	BotID = 7
)
