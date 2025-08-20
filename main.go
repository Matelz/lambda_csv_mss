package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"sync"

	"encoding/csv"
	"golangcsvparser/db"
	t "golangcsvparser/types"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
)

const (
	batchSize    uint8 = 25
	maxWorkers   int   = 10  // Number of concurrent workers for batch processing
	bufferSize   int   = 100 // Initial buffer capacity to reduce allocations
)

var ExpectedHeaders = [7]string{
	"nome",
	"ra",
	"curso",
	"serie",
	"role",
	"status",
	"entidade",
}

var (
	s3Client     *s3.Client
	dynamoClient *dynamodb.Client
	initOnce     sync.Once
)

func parseCSV(fileObj *s3.GetObjectOutput, tb db.DynamoTableBasics, ctx context.Context) error {
	defer fileObj.Body.Close()
	reader := csv.NewReader(fileObj.Body)
	reader.ReuseRecord = true

	// Pre-allocate buffer with initial capacity to reduce memory allocations
	buffer := make([]types.WriteRequest, 0, bufferSize)

	// Worker pool for batch processing
	batchChan := make(chan []types.WriteRequest, maxWorkers)
	var wg sync.WaitGroup

	// Start worker goroutines
	for i := 0; i < maxWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for batch := range batchChan {
				if _, err := tb.AddMembersBatch(ctx, batch); err != nil {
					log.Printf("Error processing batch: %v", err)
				}
			}
		}()
	}

	// Process CSV records
	for {
		record, err := reader.Read()
		if err != nil {
			if err.Error() == io.EOF.Error() {
				break
			}
			log.Printf("Error reading CSV record: %v", err)
			continue
		}

		if len(record) != len(ExpectedHeaders) {
			fmt.Printf("Unexpected number of columns. Expected %d, got %d\n", len(ExpectedHeaders), len(record))
			continue
		}

		member := t.EntradaMembro{
			Nome:     record[0],
			RA:       record[1],
			Curso:    record[2],
			Serie:    record[3],
			Role:     record[4],
			Status:   record[5],
			Entidade: record[6],
		}

		item, err := attributevalue.MarshalMap(member)
		if err != nil {
			fmt.Printf("Error marshalling item: %v\n", err)
			continue
		}

		buffer = append(buffer, types.WriteRequest{
			PutRequest: &types.PutRequest{
				Item: item,
			},
		})

		// Send batch when full
		if len(buffer) >= int(batchSize) {
			// Make a copy of the batch to send to worker
			batchCopy := make([]types.WriteRequest, len(buffer))
			copy(batchCopy, buffer)
			
			select {
			case batchChan <- batchCopy:
			case <-ctx.Done():
				close(batchChan)
				wg.Wait()
				return ctx.Err()
			}
			
			// Reset buffer, reusing underlying array capacity
			buffer = buffer[:0]
		}
	}

	// Send remaining items in buffer
	if len(buffer) > 0 {
		batchCopy := make([]types.WriteRequest, len(buffer))
		copy(batchCopy, buffer)
		
		select {
		case batchChan <- batchCopy:
		case <-ctx.Done():
			close(batchChan)
			wg.Wait()
			return ctx.Err()
		}
	}

	// Close channel and wait for all workers to finish
	close(batchChan)
	wg.Wait()

	return nil
}

func generateConfig(ctx context.Context) (aws.Config, error) {
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion("us-east-1"))
	if err != nil {
		log.Fatal(err)
		return cfg, err
	}

	return cfg, nil
}

func handler(ctx context.Context, event events.S3Event) (events.APIGatewayProxyResponse, error) {
	bucketBasics := db.NewS3BucketBasics(s3Client)

	tableBasis := db.NewDynamoTableBasics("members", dynamoClient)

	for _, record := range event.Records {
		bucket := record.S3.Bucket.Name
		key := record.S3.Object.URLDecodedKey

		fileStream, err := bucketBasics.StreamFile(ctx, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
		if err != nil {
			panic(err)
		}

		parseCSV(fileStream, tableBasis, ctx)
	}

	return events.APIGatewayProxyResponse{
		StatusCode: 200,
	}, nil
}

func init() {
	initOnce.Do(func() {
		cfg, _ := generateConfig(context.Background())
		s3Client = s3.NewFromConfig(cfg)
		dynamoClient = dynamodb.NewFromConfig(cfg)
	})
}

func main() {
	lambda.Start(handler)
}
