package models

type MongoTableAdapter[T any] struct {
	PartitionKey string `bson:"partition_key"`
	Version      int    `bson:"version"`
	Entity       T      `bson:"inline"`
}

func NewMongoTableAdapter[T any](entity T, partitionKey string, version int) *MongoTableAdapter[T] {
	adapter := new(MongoTableAdapter[T])
	adapter.Entity = entity
	adapter.Version = version
	adapter.PartitionKey = partitionKey
	return adapter
}
