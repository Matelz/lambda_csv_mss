package db

import (
	"context"
	t "golangcsvparser/types"
	"log"

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
	var err error
	var item map[string]types.AttributeValue

	written := 0
	batchSize := 25
	start := 0
	end := start + batchSize

	for start < maxMembers && start < len(members) {
		var writeReqs []types.WriteRequest
		if end > len(members) {
			end = len(members)
		}

		for _, member := range members[start:end] {
			item, err = attributevalue.MarshalMap(member)
			if err != nil {
				log.Printf("Error marshalling member: %v", err)
			} else {
				writeReqs = append(writeReqs, types.WriteRequest{
					PutRequest: &types.PutRequest{
						Item: item,
					},
				})
			}
		}

		_, err = tb.DynamoDbClient.BatchWriteItem(ctx, &dynamodb.BatchWriteItemInput{
			RequestItems: map[string][]types.WriteRequest{
				tb.TableName: writeReqs,
			},
		})

		if err != nil {
			log.Printf("Error writing batch to DynamoDB: %v", err)
		} else {
			written += len(writeReqs)
		}

		start = end
		end += batchSize
	}

	return written, err
}
