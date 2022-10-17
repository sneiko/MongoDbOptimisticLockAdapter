package services

import (
	"MongoDbOptimisticAdapter/pkg/mongodb_optimistic_adapter/models"
	"context"
	"log"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type MongoTableStorage[T any] struct {
	Host       string
	Database   string
	Collection string
}

type IMongoTableStorage[T any] interface {
	Insert(partitionKey string, entity T)
	GetByPartitionKey(key string) *models.MongoTableAdapter[T]
	Replace(partitionKey string, entity models.VersionOptimisticallyLockedEntity[T])
	Remove(partitionKey string)
}

func NewMongoTableStorage[T any](host string, database string, collection string) *MongoTableStorage[T] {
	mongoStorage := new(MongoTableStorage[T])
	mongoStorage.Host = host
	mongoStorage.Database = database
	mongoStorage.Collection = collection
	return mongoStorage
}

func (s *MongoTableStorage[T]) Insert(partitionKey string, entity T) {
	client, err := mongo.NewClient(options.Client().ApplyURI(s.Host))
	if err != nil {
		log.Fatal(err)
	}

	coll := client.Database(s.Database).Collection(s.Collection)

	adapter := models.NewMongoTableAdapter(entity, partitionKey, 0)

	_, err = coll.InsertOne(context.TODO(), adapter)
	if err != nil {
		log.Fatal(err)
	}
}

func (s *MongoTableStorage[T]) GetByPartitionKey(key string) *models.VersionOptimisticallyLockedEntity[T] {
	client, err := mongo.NewClient(options.Client().ApplyURI(s.Host))
	if err != nil {
		log.Fatal(err)
	}

	coll := client.Database(s.Database).Collection(s.Collection)

	filter := bson.D{{Key: "partition_key", Value: key}}
	cursor, err := coll.Find(context.TODO(), filter)
	if err != nil {
		log.Fatal(err)
	}

	entityResult := new(models.MongoTableAdapter[T])
	err = cursor.Decode(entityResult)
	if err != nil {
		log.Fatal(err)
	}

	return models.NewVersionOptimisticallyLockedEntity(entityResult.Entity, entityResult.Version)
}

func (s *MongoTableStorage[T]) Replace(partitionKey string, entity models.VersionOptimisticallyLockedEntity[T]) {
	client, err := mongo.NewClient(options.Client().ApplyURI(s.Host))
	if err != nil {
		log.Fatal(err)
	}

	coll := client.Database(s.Database).Collection(s.Collection)
	filter := bson.D{{Key: "partition_key", Value: partitionKey}}
	findedEntity := s.GetByPartitionKey(partitionKey)

	if findedEntity.Version != entity.Version {
		log.Fatal("Version not equal for replace entity")
	}

	entity.Version++

	adapter := models.NewMongoTableAdapter(entity.Entity, partitionKey, entity.Version)
	_, err = coll.ReplaceOne(context.TODO(), filter, adapter)
	if err != nil {
		return
	}

}

func (s *MongoTableStorage[T]) Remove(partitionKey string) {
	client, err := mongo.NewClient(options.Client().ApplyURI(s.Host))
	if err != nil {
		log.Fatal(err)
	}

	coll := client.Database(s.Database).Collection(s.Collection)

	filter := bson.D{{Key: "partition_key", Value: partitionKey}}
	_, err = coll.DeleteOne(context.TODO(), filter)
	if err != nil {
		log.Fatal(err)
	}
}
