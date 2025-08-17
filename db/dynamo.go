package db

import (
	"context"
	"log"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type DynamoTableBasics struct {
	DynamoDbClient *dynamodb.Client
	TableName      string
}

func NewDynamoTableBasics(tableName string, client *dynamodb.Client) DynamoTableBasics {
	return DynamoTableBasics{
		DynamoDbClient: client,
		TableName:      tableName,
	}
}

func (tb DynamoTableBasics) AddMembersBatch(ctx context.Context, requests []types.WriteRequest) (int, error) {
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
	}

	written = int64(len(requests))

	if written == 0 {
		return 0, nil
	}

	return int(written), nil
}
