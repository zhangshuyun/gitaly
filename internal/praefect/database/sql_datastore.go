package database

import (
	"fmt"
	"os"

	"database/sql"

	_ "github.com/lib/pq"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/models"
)

type SQLDatastore struct {
	db *sql.DB
}

func NewSQLDatastore(addr string) (*sql.DB, error) {
	connStr := fmt.Sprintf("postgres://%s:%s@%s/%s?sslmode=disabled",
		os.Getenv("PRAEFECT_PG_USER"),
		os.Getenv("PRAEFECT_PG_PASSWORD"),
		addr,
		os.Getenv("PRAEFECT_PG_DB"))

	return sql.Open("postgres", connStr)
}

func (sd *SQLDatastore) GetSecondaries(relativePath string) ([]models.StorageNode, error) {
	var secondaries []models.StorageNode

	rows, err := sd.db.Query(`
		SELECT storage_nodes.* FROM repositories
			INNER JOIN repository_secondaries ON repositories.relative_path = repository_secondaries.repository_relative_path
			INNER JOIN storage_nodes ON storage_nodes.id = repository_secondaries.storage_node_id WHERE repositories.relative_path = $1
	`, relativePath)

	if err != nil {
		return nil, err
	}

	for rows.Next() {
		var s models.StorageNode
		err = rows.Scan(&s.ID, &s.Name, &s.Address)
		if err != nil {
			return nil, err
		}
		secondaries = append(secondaries, s)
	}

	return secondaries, nil
}

func (sd *SQLDatastore) GetPrimary(relativePath string) (models.StorageNode, error) {

	row := sd.db.QueryRow(`
		SELECT storage_nodes.* FROM repositories
			INNER JOIN storage_nodes ON repositories.primary = storage_nodes.id
			WHERE repositories.relative_path = $1
	`, relativePath)

	var s models.StorageNode
	if err := row.Scan(&s.ID, &s.Name, &s.Address); err != nil {
		return nil, err
	}

	return s, nil
}
