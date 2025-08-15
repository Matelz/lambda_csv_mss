package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"golangcsvparser/db"
	t "golangcsvparser/types"

	"bufio"
	"encoding/csv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
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

func parseRow(row string) t.EntradaMembro {
	values := csv.NewReader(strings.NewReader(row))
	record, err := values.Read()
	if err != nil {
		fmt.Printf("Error reading CSV row: %v\n", err)
		return t.EntradaMembro{}
	}

	if len(record) != len(ExpectedHeaders) {
		fmt.Printf("Unexpected number of columns. Expected %d, got %d\n", len(ExpectedHeaders), len(record))
		return t.EntradaMembro{}
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

	return member
}

// The files are temporarily accessed as a file path, but later they will be read from S3 or another storage service.
func parseCSV(filePath string, tb db.TableBasis) error {
	f, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("error opening CSV file: %w", err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	scanner.Scan()

	var counter int = 0
	var buffer []types.WriteRequest

	var wg sync.WaitGroup

	for scanner.Scan() {
		row := scanner.Text()
		member := parseRow(row)

		counter++

		item, err := attributevalue.MarshalMap(member)
		if err != nil {
			fmt.Printf("Error marshalling item %d: %v\n", counter, err)
			continue
		}

		buffer = append(buffer, types.WriteRequest{
			PutRequest: &types.PutRequest{
				Item: item,
			},
		})

		if len(buffer) >= 25 {
			wg.Add(1)
			go func(members []types.WriteRequest) {
				defer wg.Done()
				tb.AddMembersBatch(context.Background(), members)
			}(buffer)

			buffer = nil
		}
	}

	if len(buffer) > 0 {
		wg.Add(1)
		go func(members []types.WriteRequest) {
			defer wg.Done()
			tb.AddMembersBatch(context.Background(), members)
		}(buffer)
	}

	wg.Wait()

	return nil
}

func handler(ctx context.Context, event map[string]interface{}) (events.APIGatewayProxyResponse, error) {
	client, err := db.NewClient()
	if err != nil {
		panic(err)
	}

	tableBasis := db.NewTableBasis("members", client)

	// TODO: Set actual timeout duration
	_, cancel := context.WithTimeoutCause(context.Background(), 150*time.Second, errors.New("timeout exceeded while writing to DynamoDB"))
	defer cancel()

	parseCSV("membros_mock_10.csv", tableBasis)

	return events.APIGatewayProxyResponse{
		StatusCode: 200,
	}, nil
}

func main() {
	lambda.Start(handler)
}
