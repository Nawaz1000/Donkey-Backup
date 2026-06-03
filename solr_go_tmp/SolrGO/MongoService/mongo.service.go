package services

import (
	"context"
	"time"

	config "mongo-to-solr/Config"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var (
	mongoClient *mongo.Client
)

func GetMongoClient() (*mongo.Client, error) {
	if mongoClient == nil {
		// configDetails, err := config.LoadConfig()
		// if err != nil {
		// 	return nil, err
		// }
		clientOptions := options.Client().ApplyURI(config.MongoConnectionString)
		clientOptions.SetConnectTimeout(30 * time.Minute)
		client, err := mongo.Connect(context.Background(), clientOptions)
		if err != nil {
			return nil, err
		}
		mongoClient = client
	}
	return mongoClient, nil
}

func GetCollection(dbName, collectionName string) (*mongo.Collection, error) {
	client, err := GetMongoClient()
	if err != nil {
		return nil, err
	}
	database := client.Database(dbName)
	return database.Collection(collectionName), nil
}
