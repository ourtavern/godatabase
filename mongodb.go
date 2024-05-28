package godatabase

import (
	"context"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var client *mongo.Client

func MGDB_FindOne[T any](ctx context.Context, databaseName string, collection string, filter interface{}) (*T, error) {
	var result T
	coll := client.Database(databaseName).Collection(collection)
	err := coll.FindOne(ctx, filter).Decode(&result)
	if err != nil {
		return nil, err
	}
	return &result, nil
}

func MGDB_Find[T any](ctx context.Context, databaseName string, collection string, filter interface{}) ([]T, error) {
	var results []T
	coll := client.Database(databaseName).Collection(collection)
	cursor, err := coll.Find(ctx, filter)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)
	for cursor.Next(ctx) {
		var result T
		err := cursor.Decode(&result)
		if err != nil {
			return nil, err
		}
		results = append(results, result)
	}
	return results, nil
}

func MGDB_InsertOne[T any](ctx context.Context, databaseName string, collection string, document T) error {
	coll := client.Database(databaseName).Collection(collection)
	_, err := coll.InsertOne(ctx, document)
	if err != nil {
		return err
	}
	return nil
}

func MGDB_InsertMany[T any](ctx context.Context, databaseName string, collection string, documents []T) error {
	coll := client.Database(databaseName).Collection(collection)
	documentsInterface := make([]interface{}, len(documents))
	for i, doc := range documents {
		documentsInterface[i] = doc
	}
	_, err := coll.InsertMany(ctx, documentsInterface)
	if err != nil {
		return err
	}
	return nil
}

func MGDB_UpdateOne[T any](ctx context.Context, databaseName string, collection string, filter interface{}, update interface{}) error {
	coll := client.Database(databaseName).Collection(collection)
	_, err := coll.UpdateOne(ctx, filter, update)
	if err != nil {
		return err
	}
	return nil
}

func MGDB_UpdateMany[T any](ctx context.Context, databaseName string, collection string, filter interface{}, update interface{}) error {
	coll := client.Database(databaseName).Collection(collection)
	_, err := coll.UpdateMany(ctx, filter, update)
	if err != nil {
		return err
	}
	return nil
}

func MGDB_DeleteOne(ctx context.Context, databaseName string, collection string, filter interface{}) error {
	coll := client.Database(databaseName).Collection(collection)
	_, err := coll.DeleteOne(ctx, filter)
	if err != nil {
		return err
	}
	return nil
}

func MGDB_DeleteMany(ctx context.Context, databaseName string, collection string, filter interface{}) error {
	coll := client.Database(databaseName).Collection(collection)
	_, err := coll.DeleteMany(ctx, filter)
	if err != nil {
		return err
	}
	return nil
}

func MGDB_Connect(ctx context.Context, databaseUri string) error {
	var err error
	client, err = mongo.Connect(ctx, options.Client().ApplyURI(databaseUri))
	return err
}

func MGDB_Disconnect(ctx context.Context) error {
	return client.Disconnect(ctx)
}
