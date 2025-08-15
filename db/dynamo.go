package db

import (
	"context"
	"log"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type TableBasis struct {
	DynamoDbClient *dynamodb.Client
	TableName      string
}

func NewClient() (*dynamodb.Client, error) {
	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithSharedConfigProfile("test"))
	if err != nil {
		log.Fatal(err)
	}

	client := dynamodb.NewFromConfig(cfg)

	return client, nil
}

func NewTableBasis(tableName string, client *dynamodb.Client) TableBasis {
	return TableBasis{
		DynamoDbClient: client,
		TableName:      tableName,
	}
}

func (tb TableBasis) AddMembersBatch(ctx context.Context, requests []types.WriteRequest) (int, error) {
	var written int64 = 0

	batch := make(map[string][]types.WriteRequest)

	batch["members"] = requests

	op, err := tb.DynamoDbClient.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{
		RequestItems: batch,
	})
	if err != nil {
		return 0, err
	}

	if len(op.UnprocessedItems) > 0 {
		log.Printf("Unprocessed items: %v", op.UnprocessedItems)
	} else {
		log.Printf("Successfully wrote batch")
	}

	written = int64(len(requests))

	if written == 0 {
		return 0, nil
	}

	log.Printf("Total members written: %d", written)
	return int(written), nil
}
