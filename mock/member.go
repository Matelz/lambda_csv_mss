package mock

import (
	"encoding/csv"
	"fmt"
	"golangcsvparser/types"
	"os"

	"github.com/brianvoe/gofakeit/v7"
)

func GenerateMemberMock(size int) ([]types.EntradaMembro, error) {
	var membros = make([]types.EntradaMembro, size)
	file, err := os.Create(fmt.Sprintf("./membros_mock_%d.csv", size))
	if err != nil {
		return nil, err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	for i := range size {
		membros[i] = types.EntradaMembro{
			Nome: gofakeit.Name(),
			RA:   gofakeit.Regex(`[0-9]{2}\.[0-9]{5}-[0-9]{1}`),
			Curso: gofakeit.RandomString([]string{
				"Análise e Desenvolvimento de Sistemas",
				"Ciência da Computação",
				"Sistemas de Informação",
				"Engenharia de Software",
				"Redes de Computadores",
				"Banco de Dados",
				"Segurança da Informação",
				"Inteligência Artificial",
			}),
			Serie: gofakeit.RandomString([]string{
				"1º Semestre",
				"2º Semestre",
				"3º Semestre",
				"4º Semestre",
				"5º Semestre",
				"6º Semestre",
				"7º Semestre",
				"8º Semestre",
				"9º Semestre",
				"10º Semestre",
			}),
			Role: gofakeit.RandomString([]string{
				"Presidente",
				"Membro",
			}),
			Status: gofakeit.RandomString([]string{
				"Aprovado",
				"Pendente",
				"Rejeitado",
			}),
			Entidade: gofakeit.RandomString([]string{
				"Centro Acadêmico",
				"Mauá JR",
				"Dev. Community Mauá",
				"Kimauanisso",
			}),
		}
	}

	writer.Write([]string{"nome", "ra", "curso", "serie", "role", "status", "entidade"})
	for _, membro := range membros {
		record := []string{membro.Nome, membro.RA, membro.Curso, membro.Serie, membro.Role, membro.Status, membro.Entidade}
		if err := writer.Write(record); err != nil {
			return nil, err
		}
	}
	return membros, nil
}
