package main

import (
	"context"
	"os"

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

// The files are temporarily accessed as a file path, but later they will be read from the lambda event
func parseCSV(filePath string) ([]types.EntradaMembro, int, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, 0, err
	}

	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		return nil, 0, err
	}

	if len(records) == 0 {
		return nil, 0, nil
	}

	// Exclude the header row
	var members = make([]types.EntradaMembro, len(records)-1)

	for i, record := range records[1:] {
		if len(record) != len(ExpectedHeaders) {
			return nil, 0, err
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

	return members, len(members), nil
}

func writeToDynamoDB(tb db.TableBasis, members []types.EntradaMembro, offset int) (int, error) {
	written, err := tb.AddMembersBatch(context.Background(), members[offset:], len(members)-offset)
	if err != nil {
		return 0, err
	}

	if written == 0 {
		return 0, nil
	}

	if written < len(members)-offset {
		return written, nil
	}
	return written, nil
}

func main() {
	mock.GenerateMemberMock(10)

	client, err := db.NewClient()
	if err != nil {
		panic(err)
	}

	tableBasis := db.NewTableBasis("members", client)

	members, _, err := parseCSV("membros_mock_10.csv")
	if err != nil {
		panic(err)
	}

	written, err := writeToDynamoDB(tableBasis, members, 0)
	if err != nil {
		panic(err)
	}

	if written == 0 {
		panic("No members were written to DynamoDB")
	}
}
