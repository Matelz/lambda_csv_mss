package db

import (
	"context"
	t "golangcsvparser/types"
	"log"
	"sync"
	"sync/atomic"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
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

func (tb TableBasis) AddMembersBatch(ctx context.Context, members []t.EntradaMembro, maxMembers int) (int, error) {
	var wg sync.WaitGroup
	var written int64 = 0

	batchSize := 25
	start := 0
	end := start + batchSize

	for start < maxMembers && start < len(members) {
		wg.Add(1)

		go func(start, end int) {
			defer wg.Done()

			batch := make(map[string][]types.WriteRequest)
			var requests []types.WriteRequest

			for i := start; i != end; i++ {
				item, err := attributevalue.MarshalMap(members[i])
				if err != nil {
					log.Printf("Error marshalling item %d: %v", i, err)
					continue
				}

				requests = append(requests, types.WriteRequest{
					PutRequest: &types.PutRequest{
						Item: item,
					},
				})
			}

			batch["members"] = requests

			op, err := tb.DynamoDbClient.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{
				RequestItems: batch,
			})
			if err != nil {
				log.Printf("Error writing batch from %d to %d: %v", start, end, err)
				return
			}

			if len(op.UnprocessedItems) > 0 {
				log.Printf("Unprocessed items from %d to %d: %v", start, end, op.UnprocessedItems)
			} else {
				log.Printf("Successfully wrote batch from %d to %d", start, end)
			}

			atomic.AddInt64(&written, int64(len(requests)))
		}(start, end)

		start = end
		end += batchSize
	}

	wg.Wait()

	if written == 0 {
		return 0, nil
	}

	log.Printf("Total members written: %d", written)
	return int(written), nil
}
