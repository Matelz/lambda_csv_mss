package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"runtime"
	"time"

	"golangcsvparser/db"
	"golangcsvparser/mock"
	"golangcsvparser/types"

	"encoding/csv"
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

type CSVParserResult struct {
	Members []types.EntradaMembro
	Count   int
	Err     error
}

// The files are temporarily accessed as a file path, but later they will be read from the lambda event
func parseCSV(filePath string, csvReady chan CSVParserResult) {
	file, err := os.Open(filePath)
	if err != nil {
		csvReady <- CSVParserResult{Err: fmt.Errorf("error opening CSV file: %w", err)}
		return
	}

	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		csvReady <- CSVParserResult{Err: fmt.Errorf("error reading CSV file: %w", err)}
		return
	}

	if len(records) == 0 {
		csvReady <- CSVParserResult{Err: errors.New("no records found in the CSV file")}
		return
	}

	// Exclude the header row
	var members = make([]types.EntradaMembro, len(records)-1)

	for i, record := range records[1:] {
		if len(record) != len(ExpectedHeaders) {
			csvReady <- CSVParserResult{Err: fmt.Errorf("record %d does not match expected headers. Expected %d fields, got %d", i+1, len(ExpectedHeaders), len(record))}
			return
		}

		members[i] = types.EntradaMembro{
			Nome:     record[0],
			RA:       record[1],
			Curso:    record[2],
			Serie:    record[3],
			Role:     record[4],
			Status:   record[5],
			Entidade: record[6],
		}
	}

	csvReady <- CSVParserResult{Members: members, Count: len(members), Err: nil}
}

func main() {
	mock.GenerateMemberMock(5000)

	csvReady := make(chan CSVParserResult)
	go func() {
		parseCSV("membros_mock_5000.csv", csvReady)
	}()

	startTime := time.Now()

	client, err := db.NewClient()
	if err != nil {
		panic(err)
	}

	tableBasis := db.NewTableBasis("members", client)

	// TODO: Set actual timeout duration
	cntx, cancel := context.WithTimeoutCause(context.Background(), 200*time.Second, errors.New("timeout exceeded while writing to DynamoDB"))
	defer cancel()

	result := <-csvReady
	if result.Err != nil {
		fmt.Println("Error parsing CSV:", result.Err)
		return
	}

	tableBasis.AddMembersBatch(cntx, result.Members, result.Count)

	elapsedTime := time.Since(startTime)

	println("Elapsed time:", elapsedTime.String())

	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	fmt.Printf("Total Alloc: %v Bytes\n", m.TotalAlloc)
}
