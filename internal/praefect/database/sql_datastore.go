package database

import (
	"errors"
	"fmt"
	"os"

	"database/sql"

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
		SELECT storage_nodes.id, storage_nodes.address, storage_nodes.storage_name FROM repositories
			INNER JOIN repository_secondaries ON repositories.relative_path = repository_secondaries.repository_relative_path
			INNER JOIN storage_nodes ON storage_nodes.id = repository_secondaries.storage_node_id WHERE repositories.relative_path = $1
	`, relativePath)

	if err != nil {
		return nil, err
	}

	for rows.Next() {
		var s models.StorageNode
		err = rows.Scan(&s.ID, &s.StorageName, &s.Address)
		if err != nil {
			return nil, err
		}
		secondaries = append(secondaries, s)
	}

	return secondaries, nil
}

func (sd *SQLDatastore) GetPrimary(relativePath string) (*models.StorageNode, error) {

	row := sd.db.QueryRow(`
		SELECT storage_nodes.id, storage_nodes.adddress, storage_nodes.storage_name FROM repositories
			INNER JOIN storage_nodes ON repositories.primary = storage_nodes.id
			WHERE repositories.relative_path = $1
	`, relativePath)

	var s models.StorageNode
	if err := row.Scan(&s.ID, &s.Address, &s.StorageName); err != nil {
		return nil, err
	}

	return &s, nil
}

func (sd *SQLDatastore) SetPrimary(relativePath, storageNodeID int) error {
	res, err := sd.db.Exec(`UPDATE repositories SET "primary" = ? WHERE relative_path= ?`, storageNodeID, relativePath)
	if err != nil {
		return err
	}

	if n, err := res.RowsAffected(); err != nil {
		return err
	} else if n == 0 {
		return errors.New("repository does not exist")
	}

	return nil
}

func (sd *SQLDatastore) GetDefaultPrimary(storage string) (*models.StorageNode, error) {
	row := sd.db.QueryRow("SELECT storage_nodes.id, storage_nodes.address, storage_nodes.storage_name from stroage_nodes where storage_name = ?", storage)

	var s models.StorageNode
	if err := row.Scan(&s.ID, &s.Address, &s.StorageName); err != nil {
		return nil, err
	}

	return &s, nil
}

func (sd *SQLDatastore) AddSecondary(relativePath string, storageNodeID int) error {
	res, err := sd.db.Exec("INSERT INTO repository_secondaries (repository_relative_path, node_storage_id) VALUES(?, ?)", relativePath, storageNodeID)
	if err != nil {
		return err
	}

	if n, err := res.RowsAffected(); err != nil {
		return err
	} else if n == 0 {
		return errors.New("secondary already exists")
	}

	return nil
}

func (sd *SQLDatastore) RemoveSecondary(relativePath string, storageNodeID int) error {
	res, err := sd.db.Exec("DELETE FROM repository_secondaries (repository_relative_path, node_storage_id) VALUES(?, ?)", relativePath, storageNodeID)
	if err != nil {
		return err
	}

	if n, err := res.RowsAffected(); err != nil {
		return err
	} else if n == 0 {
		return errors.New("secondary did not exist")
	}

	return nil
}

func (sd *SQLDatastore) GetShard(relativePath string) (*models.Shard, error) {
	primary, err := sd.GetPrimary(relativePath)
	if err != nil {
		return nil, fmt.Errorf("getting primary: %v", err)
	}

	secondaries, err := sd.GetSecondaries(relativePath)
	if err != nil {
		return nil, fmt.Errorf("getting secondaries: %v", err)
	}

	return &models.Shard{RelativePath: relativePath, Primary: *primary, Secondaries: secondaries}, nil
}
