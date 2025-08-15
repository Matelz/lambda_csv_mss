package db

import (
	"context"
	"log"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type TableBasis struct {
	DynamoDbClient *dynamodb.Client
	TableName      string
}

func NewClient(cfg aws.Config, ctx context.Context) (*dynamodb.Client, error) {
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
