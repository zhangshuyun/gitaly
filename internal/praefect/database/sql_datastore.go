package database

import (
	"errors"
	"fmt"

	"database/sql"

	_ "github.com/lib/pq"

	"gitlab.com/gitlab-org/gitaly/internal/praefect/models"
)

type SQLDatastore struct {
	db *sql.DB
}

// NewSQLDatastore instantiates a new sql datastore with environment variables
func NewSQLDatastore(user, password, address, database string) (*SQLDatastore, error) {
	connStr := fmt.Sprintf("postgres://%s:%s@%s/%s?sslmode=disable", user, password, address, database)
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, err
	}

	return &SQLDatastore{db: db}, nil
}

func (sd *SQLDatastore) GetSecondaries(relativePath string) ([]models.StorageNode, error) {
	var secondaries []models.StorageNode

	rows, err := sd.db.Query(`
	SELECT node_storages.id, node_storages.address, node_storages.storage_name FROM shards
		INNER JOIN shard_secondaries ON shards.relative_path = shard_secondaries.shard_relative_path
		INNER JOIN node_storages ON node_storages.id = shard_secondaries.node_storage_id WHERE shards.relative_path = $1`, relativePath)

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

func (sd *SQLDatastore) GetNodesForStorage(storageName string) ([]models.StorageNode, error) {
	var nodeStorages []models.StorageNode

	rows, err := sd.db.Query(`SELECT node_storages.id, node_storages.address, node_storages.storage_name FROM node_storages WHERE storage_name = $1`, storageName)

	if err != nil {
		return nil, err
	}

	for rows.Next() {
		var s models.StorageNode
		err = rows.Scan(&s.ID, &s.Address, &s.StorageName)
		if err != nil {
			return nil, err
		}
		nodeStorages = append(nodeStorages, s)
	}

	return nodeStorages, nil
}

func (sd *SQLDatastore) GetNodeStorages() ([]models.StorageNode, error) {
	var nodeStorages []models.StorageNode

	rows, err := sd.db.Query("SELECT node_storages.id, node_storages.address,node_storages.storage_name   FROM node_storages")

	if err != nil {
		return nil, err
	}

	for rows.Next() {
		var nodeStorage models.StorageNode
		err = rows.Scan(&nodeStorage.ID, &nodeStorage.Address, &nodeStorage.StorageName)
		if err != nil {
			return nil, err
		}
		nodeStorages = append(nodeStorages, nodeStorage)
	}

	return nodeStorages, nil

}

func (sd *SQLDatastore) GetDefaultPrimary() (*models.StorageNode, error) {

	row := sd.db.QueryRow("SELECT node_storages.id, node_storages.address, node_storages.storage_name FROM node_storages limit 1")

	var s models.StorageNode
	if err := row.Scan(&s.ID, &s.Address, &s.StorageName); err != nil {
		return nil, err
	}

	return &s, nil
}

func (sd *SQLDatastore) GetPrimary(relativePath string) (*models.StorageNode, error) {

	row := sd.db.QueryRow(`
	SELECT node_storages.id, node_storages.address, node_storages.storage_name FROM shards
	  INNER JOIN node_storages ON shards.primary = node_storages.id
	  WHERE shards.relative_path = $1
	`, relativePath)

	var s models.StorageNode
	if err := row.Scan(&s.ID, &s.Address, &s.StorageName); err != nil {
		return nil, err
	}

	return &s, nil
}

func (sd *SQLDatastore) SetPrimary(relativePath string, storageNodeID int) error {
	res, err := sd.db.Exec(`UPDATE shards SET "primary" = $1 WHERE relative_path = $2`, storageNodeID, relativePath)
	if err != nil {
		return err
	}

	if n, err := res.RowsAffected(); err != nil {
		return err
	} else if n == 0 {
		res, err = sd.db.Exec(`INSERT INTO shards (relative_path, "primary") VALUES ($1, $2)`, relativePath, storageNodeID)
		if err != nil {
			return err
		}
		if n, err := res.RowsAffected(); err != nil {
			return err
		} else if n == 0 {
			return errors.New("failed to set primary")
		}
	}

	return nil
}

func (sd *SQLDatastore) AddSecondary(relativePath string, storageNodeID int) error {
	res, err := sd.db.Exec("INSERT INTO shard_secondaries (shard_relative_path, node_storage_id) VALUES($1, $2)", relativePath, storageNodeID)
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
	res, err := sd.db.Exec("DELETE FROM shard_secondaries (shard_relative_path, node_storage_id) VALUES($1, $2)", relativePath, storageNodeID)
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

func (sd *SQLDatastore) RotatePrimary(primaryNodeStorageID int) error {

	// Add the primary as a secondary
	res, err := sd.db.Exec(`
	INSERT INTO shard_secondaries (shard_relative_path, node_storage_id) VALUES (SELECT shards.relative_path, shards.primary FROM shards WHERE shards.primary = $1)
	`, primaryNodeStorageID)
	if err != nil {
		return err
	}

	affected, err := res.RowsAffected()
	if err != nil {
		return err
	}

	if affected == 0 {
		return fmt.Errorf("no shards with primary %d found", primaryNodeStorageID)
	}

	// Choose a new secondary
	res, err = sd.db.Exec(`UPDATE shards SET "primary" =
              	(SELECT shard_secondaries.node_storage_id FROM shard_secondaries
              		INNER JOIN shards ON shard_secondaries.shard_relative_path = shards.relative_path
              		WHERE shards.primary = $1 AND shards.primary != shard_secondaries.node_storage_id LIMIT 1)
              	`, primaryNodeStorageID)
	if err != nil {
		return err
	}

	affected, err = res.RowsAffected()
	if err != nil {
		return err
	}

	if affected == 0 {
		return errors.New("no secondaries available to rotate")
	}
	return nil
}
