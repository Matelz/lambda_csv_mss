package main

import "golangcsvparser/db"

var ExpectedHeaders = [7]string{
	"nome",
	"ra",
	"curso",
	"serie",
	"role",
	"status",
	"entidade",
}

func main() {
	db.NewClient()
}
