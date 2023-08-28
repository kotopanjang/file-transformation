package mongodb

import (
	"context"
	"errors"
	"file-transformation/backend/helper"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

type IMongodbStorage interface {
	HealthCheck() error

	FindOne(collectionName string, filter map[string]any, result any, opts map[string]any) error
	FindMany(collectionName string, filter map[string]any, result any, opts map[string]any) error

	InsertOne(collectionName string, data any) (*mongo.InsertOneResult, error)
	InsertMany(collectionName string, datas []any) (*mongo.InsertManyResult, error)
	Upsert(collectionName string, filter any, data any) (*mongo.UpdateResult, error)

	Delete(collectionName string, filter map[string]any) error
}

type MongodbStorage struct {
	ctx      context.Context
	client   *mongo.Client
	database *mongo.Database
}

func NewMongo(uri, dbName string) (*MongodbStorage, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	if err != nil {
		return nil, err
	}

	database := client.Database(dbName)

	return &MongodbStorage{
		client:   client,
		database: database,
		ctx:      ctx,
	}, nil
}

// HealthCheck is to check health of mongodb instance.
// it will return nil if it success and return error if there is a problem with the mongodb.
func (m *MongodbStorage) HealthCheck() error {
	err := m.client.Ping(m.ctx, readpref.Primary())

	return err
}

// InsertOne is to insert data to database, with only 1 document.
// data is only 1 single document/data
func (m *MongodbStorage) InsertOne(collectionName string, data any) (*mongo.InsertOneResult, error) {
	col := m.database.Collection(collectionName)
	return col.InsertOne(m.ctx, data)
}

// InsertMany is to insert data to database as bulk.
// datas is array of document/data
func (m *MongodbStorage) InsertMany(collectionName string, datas []any) (*mongo.InsertManyResult, error) {
	col := m.database.Collection(collectionName)
	return col.InsertMany(m.ctx, datas)
}

// Upsert is to update / insert data
// datas is document that we want to update
func (m *MongodbStorage) Upsert(collectionName string, filter any, data any) (*mongo.UpdateResult, error) {
	col := m.database.Collection(collectionName)
	upsert := true
	return col.UpdateOne(m.ctx, filter, bson.M{"$set": data}, &options.UpdateOptions{Upsert: &upsert})
}

// FindOne is to find 1 single document
// opts can have some options like this {"sort": {"fieldname": 1}, "skip": 0, "limit": 1}
func (m *MongodbStorage) FindOne(collectionName string, filter map[string]any, result any, opts map[string]any) error {
	col := m.database.Collection(collectionName)
	_, findOneOpt := buildOptions(opts)
	err := col.FindOne(m.ctx, filter, &findOneOpt).Decode(result)
	if err != nil {
		return err
	}

	return nil
}

// FindMany is to find 1 single document
// opts can have some options like this {"sort": {"fieldname": 1}, "skip": 0, "limit": 1}
func (m *MongodbStorage) FindMany(collectionName string, filter map[string]any, result any, opts map[string]any) error {
	col := m.database.Collection(collectionName)
	findOpt, _ := buildOptions(opts)
	cursor, err := col.Find(m.ctx, filter, &findOpt)
	if err != nil {
		return err
	}

	// fetch cursor into result
	err = cursor.All(m.ctx, result)
	if err != nil {
		return err
	}

	// close the cursor
	if cursor != nil {
		cursor.Close(m.ctx)
	}

	return nil
}

// Delete is to delete data from database
// filter param cannot be empty
func (m *MongodbStorage) Delete(collectionName string, filter map[string]any) error {
	if len(filter) == 0 {
		return errors.New("filter cannot be empty")
	}

	col := m.database.Collection(collectionName)
	_, err := col.DeleteMany(m.ctx, filter)
	if err != nil {
		return err
	}

	return nil
}

func buildOptions(opts map[string]any) (findOpt options.FindOptions, findoneOpt options.FindOneOptions) {
	for k, v := range opts {
		switch strings.ToLower(k) {
		case "sort":
			findOpt.SetSort(v)
			findoneOpt.SetSort(v)
		case "limit":
			findOpt.SetLimit(int64(helper.ToInt(v)))
		case "skip":
			findOpt.SetSkip(int64(helper.ToInt(v)))
			findoneOpt.SetSkip(int64(helper.ToInt(v)))
		}
	}
	return
}
