package types

type EntradaMembro struct {
	Nome     string `dynamodbav:"nome"`
	RA       string `dynamodbav:"ra"`
	Curso    string `dynamodbav:"curso"`
	Serie    string `dynamodbav:"serie"`
	Role     string `dynamodbav:"role"`
	Status   string `dynamodbav:"status"`
	Entidade string `dynamodbav:"entidade"`
}
