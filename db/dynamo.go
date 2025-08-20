package db

import (
	"context"
	"log"
	"time"

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
	var unprocessedItems map[string][]types.WriteRequest
	
	batch := make(map[string][]types.WriteRequest)
	batch[tb.TableName] = requests

	// Retry logic for unprocessed items
	maxRetries := 3
	for retry := 0; retry <= maxRetries; retry++ {
		op, err := tb.DynamoDbClient.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{
			RequestItems: batch,
		})
		if err != nil {
			return 0, err
		}

		written += int64(len(batch[tb.TableName]) - len(op.UnprocessedItems[tb.TableName]))
		
		if len(op.UnprocessedItems) == 0 {
			break
		}
		
		if retry < maxRetries {
			log.Printf("Retrying %d unprocessed items (attempt %d/%d)", len(op.UnprocessedItems[tb.TableName]), retry+1, maxRetries)
			
			// Exponential backoff with jitter
			backoffDelay := time.Duration(1<<retry) * 100 * time.Millisecond
			time.Sleep(backoffDelay)
			
			batch = op.UnprocessedItems
		} else {
			log.Printf("Failed to process %d items after %d retries", len(op.UnprocessedItems[tb.TableName]), maxRetries)
			unprocessedItems = op.UnprocessedItems
		}
	}

	if len(unprocessedItems) > 0 {
		log.Printf("Final unprocessed items: %d", len(unprocessedItems[tb.TableName]))
	}

	if written == 0 {
		return 0, nil
	}

	return int(written), nil
}
