package types

type EntradaMembro struct {
	Nome     string `json:"nome"`
	RA       string `json:"ra"`
	Curso    string `json:"curso"`
	Serie    string `json:"serie"`
	Role     string `json:"role"`
	Status   string `json:"status"`
	Entidade string `json:"entidade"`
}
