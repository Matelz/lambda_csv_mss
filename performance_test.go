package main

import (
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"golangcsvparser/db"
	"golangcsvparser/mock"
	"io"
	"log"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// MockS3Output simulates S3 GetObjectOutput for testing
type MockS3Output struct {
	Body io.ReadCloser
}

func (m *MockS3Output) Close() error {
	return m.Body.Close()
}

// MockDynamoClient for testing performance without actual DynamoDB calls
type MockDynamoTableBasics struct {
	processedCount int
	tableName      string
}

func (m *MockDynamoTableBasics) AddMembersBatch(ctx context.Context, requests []interface{}) (int, error) {
	// Simulate some processing time
	time.Sleep(10 * time.Millisecond)
	m.processedCount += len(requests)
	return len(requests), nil
}

// createMockCSV creates a CSV in memory for testing
func createMockCSV(size int) (io.ReadCloser, error) {
	var buf bytes.Buffer
	writer := csv.NewWriter(&buf)

	// Write header
	writer.Write([]string{"nome", "ra", "curso", "serie", "role", "status", "entidade"})

	// Generate mock data
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

	return io.NopCloser(bytes.NewReader(buf.Bytes())), nil
}

// Simple performance test function
func runPerformanceTest(csvSize int) {
	fmt.Printf("Running performance test with %d CSV records...\n", csvSize)

	// Create mock CSV data
	csvData, err := createMockCSV(csvSize)
	if err != nil {
		log.Fatalf("Failed to create mock CSV: %v", err)
	}

	// Create mock S3 output
	mockS3Output := &s3.GetObjectOutput{
		Body: csvData,
	}

	// Create mock DynamoDB table (this would be replaced with actual implementation in real tests)
	mockTable := db.NewDynamoTableBasics("test-table", nil)

	// Measure performance
	ctx := context.Background()
	start := time.Now()

	err = parseCSV(mockS3Output, mockTable, ctx)
	if err != nil {
		log.Fatalf("Failed to parse CSV: %v", err)
	}

	duration := time.Since(start)
	fmt.Printf("Processed %d records in %v (%.2f records/second)\n",
		csvSize, duration, float64(csvSize)/duration.Seconds())
}

// Performance test entry point (commented out so it doesn't run during build)
/*
func testPerformance() {
	fmt.Println("=== CSV Parser Performance Test ===")

	// Test with different CSV sizes
	testSizes := []int{100, 1000, 5000}

	for _, size := range testSizes {
		runPerformanceTest(size)
		fmt.Println()
	}
}
*/