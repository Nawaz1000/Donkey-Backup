package models

type GeoRegion struct {
	GeoRegionID string `bson:"geo_location_id" json:"id"`
	LocationID  string `bson:"location_id" json:"location_id"`
	Type        string `bson:"type" json:"location_type"`
	NameFull    string `bson:"name_full" json:"en"`
	ArFullName  string `bson:"ar_full_name" json:"ar"`
	FrFullName  string `bson:"fr_full_name" json:"fr"`
	PtFullName  string `bson:"pt_full_name" json:"pt"`
	CnFullName  string `bson:"cn_full_name" json:"cn"`
	EsFullName  string `bson:"es_full_name" json:"es"`
	CountryCode string `bson:"country_code" json:"country_code"`
	Latitude    string `bson:"latitude" json:"-"`
	Longitude   string `bson:"longitude" json:"-"`
	HotelCount  int    `bson:"hotel_count" json:"hotel_count"`
}

type LocationDataSolr struct {
	GeoRegionID string `json:"id"`

	LocationNameEn string `json:"lc_en"`
	LocationNameFr string `json:"lc_fr"`
	LocationNameAr string `json:"lc_ar"`
	LocationNamePt string `json:"lc_pt"`
	LocationNameCn string `json:"lc_cn"`
	LocationNameEs string `json:"lc_es"`

	CityNameEn string `json:"ct_en"`
	CityNameFr string `json:"ct_fr"`
	CityNameAr string `json:"ct_ar"`
	CityNamePt string `json:"ct_pt"`
	CityNameCn string `json:"ct_cn"`
	CityNameEs string `json:"ct_es"`

	StateNameEn string `json:"st_en"`
	StateNameFr string `json:"st_fr"`
	StateNameAr string `json:"st_ar"`
	StateNamePt string `json:"st_pt"`
	StateNameCn string `json:"st_cn"`
	StateNameEs string `json:"st_es"`

	CountryNameEn string `json:"cn_en"`
	CountryNameFr string `json:"cn_fr"`
	CountryNameAr string `json:"cn_ar"`
	CountryNamePt string `json:"cn_pt"`
	CountryNameCn string `json:"cn_cn"`
	CountryNameEs string `json:"cn_es"`
	HotelCount    int    `json:"hotel_count"`
	CountryCode   string `json:"country_code"`
	LocationType  string `json:"location_type"`
	SearchQuery   string `json:"search_query"`
	Location      string `json:"location"`
	TextNGram     string `json:"text_ngram"`
}

type HotelMappingV2 struct {
	HotelId     int     `bson:"hotel_id" json:"id"`
	Name        string  `bson:"name" json:"hn_en"`
	Latitude    float64 `bson:"lat" json:"-"`
	Longitude   float64 `bson:"long" json:"-"`
	CountryCode string  `bson:"country_code" json:"-"`
	CityName    string  `bson:"city_name" json:"ct_en"`
	StateName   string  `bson:"state_province_name" json:"st_en"`
	CountryName string  `bson:"country_name" json:"cn_en"`
	SearchQuery string  `json:"search_query"`
	Location    string  `json:"location"`
	TextNGram   string  `json:"text_ngram"`
}

type PorgresGeoLocation struct {
	CityID        string `db:"city_id" json:"city_id"`
	CityCode      string `db:"city_code" json:"city_code"`
	CityNameEn    string `db:"city_name_en" json:"city_name_en,omitempty"`
	CityNameAr    string `db:"city_name_ar" json:"city_name_ar,omitempty"`
	CountryNameEn string `db:"country_name_en" json:"country_name_en,omitempty"`
	CountryNameAr string `db:"country_name_ar" json:"country_name_ar,omitempty"`
	CountryCode   string `db:"country_code" json:"country_code,omitempty"`
	CountryNameFr string `db:"country_name_fr" json:"country_name_fr"`
	CountryNameCh string `db:"country_name_ch" json:"country_name_ch"`
	CountryNameGe string `db:"country_name_ge" json:"country_name_ge"`
	CountryNameUr string `db:"country_name_ur" json:"country_name_ur"`
	CityNameFr    string `db:"city_name_fr" json:"city_name_fr"`
	CityNameCh    string `db:"city_name_ch" json:"city_name_ch"`
	CityNameGe    string `db:"city_name_ge" json:"city_name_ge"`
	CityNameUr    string `db:"city_name_ur" json:"city_name_ur"`
}

type PostgresqlHotelCuratedDataCacheSolr struct {
	HotelID       string  `db:"hotel_id" json:"hotel_id"`
	HotelNameEn   string  `db:"hotel_name_en" json:"hotel_name_en"`
	CityID        string  `db:"city_id" json:"city_id"`
	CityNameEn    string  `db:"city_name_en" json:"city_name_en"`
	CountryNameEn string  `db:"country_name_en" json:"country_name_en"`
	Latitude      float64 `db:"latitude" json:"latitude"`
	Longitude     float64 `db:"longitude" json:"longitude"`
	CountryCode   string  `db:"country_code" json:"country_code"`
}
