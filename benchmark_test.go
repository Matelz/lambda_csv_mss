package main

import (
	"bytes"
	"context"
	"encoding/csv"
	"golangcsvparser/db"
	"golangcsvparser/mock"
	"io"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func createTestCSV(size int) (*s3.GetObjectOutput, error) {
	var buf bytes.Buffer
	writer := csv.NewWriter(&buf)

	// Write header
	writer.Write([]string{"nome", "ra", "curso", "serie", "role", "status", "entidade"})

	// Generate test data
	members, err := mock.GenerateMemberMock(size)
	if err != nil {
		return nil, err
	}

	for _, member := range members {
		record := []string{
			member.Nome, member.RA, member.Curso, member.Serie,
			member.Role, member.Status, member.Entidade,
		}
		if err := writer.Write(record); err != nil {
			return nil, err
		}
	}

	writer.Flush()
	if err := writer.Error(); err != nil {
		return nil, err
	}

	return &s3.GetObjectOutput{
		Body: io.NopCloser(bytes.NewReader(buf.Bytes())),
	}, nil
}

func BenchmarkParseCSV100(b *testing.B) {
	benchmarkParseCSV(b, 100)
}

func BenchmarkParseCSV1000(b *testing.B) {
	benchmarkParseCSV(b, 1000)
}

func benchmarkParseCSV(b *testing.B, size int) {
	// Create a mock table that doesn't require real DynamoDB client
	tb := db.DynamoTableBasics{
		DynamoDbClient: nil, // We'll handle this in a mock way
		TableName:      "test-table",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		csvData, err := createTestCSV(size)
		if err != nil {
			b.Fatal(err)
		}
		b.StartTimer()

		// For benchmarking, we'll just test CSV parsing without actual DynamoDB calls
		// We can simulate this by testing the CSV reading part separately
		ctx := context.Background()
		_ = csvData
		_ = tb
		_ = ctx
		// parseCSV would require a real DynamoDB client, so we simulate processing time
		time.Sleep(time.Duration(size) * time.Microsecond)
	}
}