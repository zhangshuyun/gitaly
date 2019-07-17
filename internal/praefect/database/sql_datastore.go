package database

import (
	"errors"
	"fmt"

	"database/sql"

	// the lib/pg package provides postgres bindings for the sql package
	_ "github.com/lib/pq"

	"gitlab.com/gitlab-org/gitaly/internal/praefect/models"
)

// SQLDatastore is a sql based datastore that conforms to the ReplicasDatastore interface
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

// GetSecondaries gets the secondaries for a shard based on the relative path
func (sd *SQLDatastore) GetSecondaries(storage, relativePath string) ([]models.StorageNode, error) {
	var secondaries []models.StorageNode

	rows, err := sd.db.Query(`
	SELECT storage_nodes.id, storage_nodes.address FROM shards
		INNER JOIN shard_secondaries ON shards.id = shard_secondaries.shard_id
		INNER JOIN storage_nodes ON storage_nodes.id = shard_secondaries.storage_node_id WHERE shards.storage = $1 AND shards.relative_path = $2`, storage, relativePath)

	if err != nil {
		return nil, err
	}

	for rows.Next() {
		var s models.StorageNode
		err = rows.Scan(&s.ID, &s.Address)
		if err != nil {
			return nil, err
		}
		secondaries = append(secondaries, s)
	}

	return secondaries, nil
}

// GetStorageNode gets all storage storage_nodes
func (sd *SQLDatastore) GetStorageNode(nodeID int) (models.StorageNode, error) {
	var node models.StorageNode

	row := sd.db.QueryRow("SELECT storage_nodes.id, storage_nodes.address, storage_nodes.storage FROM storage_nodes WHERE storage_nodes.id = $1", nodeID)

	err := row.Scan(&node.ID, &node.Address, &node.Storage)
	if err != nil {
		return node, err
	}

	return node, nil

}

// GetStorageNodes gets all storage storage_nodes
func (sd *SQLDatastore) GetStorageNodes() ([]models.StorageNode, error) {
	var nodeStorages []models.StorageNode

	rows, err := sd.db.Query("SELECT storage_nodes.id, storage_nodes.address, storage_nodes.storage FROM storage_nodes")

	if err != nil {
		return nil, err
	}

	for rows.Next() {
		var nodeStorage models.StorageNode
		err = rows.Scan(&nodeStorage.ID, &nodeStorage.Address, &nodeStorage.Storage)
		if err != nil {
			return nil, err
		}
		nodeStorages = append(nodeStorages, nodeStorage)
	}

	return nodeStorages, nil

}

// GetPrimary gets the primary storage node for a shard of a repository relative path
func (sd *SQLDatastore) GetPrimary(storage, relativePath string) (*models.StorageNode, error) {

	row := sd.db.QueryRow(`
	SELECT storage_nodes.id, storage_nodes.address, storage_nodes.storage FROM shards
	  INNER JOIN storage_nodes ON shards.primary = storage_nodes.id
	  WHERE shards.storage = $1 AND shards.relative_path = $2
	`, storage, relativePath)

	var s models.StorageNode
	if err := row.Scan(&s.ID, &s.Address, &s.Storage); err != nil {
		return nil, err
	}

	return &s, nil
}

// SetPrimary sets the primary storagee node for a shard of a repository relative path
func (sd *SQLDatastore) SetPrimary(storage, relativePath string, storageNodeID int) error {
	res, err := sd.db.Exec(`UPDATE shards SET "primary" = $1 WHERE storage = $2 AND relative_path = $3`, storageNodeID, storage, relativePath)
	if err != nil {
		return err
	}

	if n, err := res.RowsAffected(); err != nil {
		return err
	} else if n == 0 {
		res, err = sd.db.Exec(`INSERT INTO shards (storage, relative_path, "primary") VALUES ($1, $2, $3)`, storage, relativePath, storageNodeID)
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

// AddSecondary adds a secondary to a shard of a repository relative path
func (sd *SQLDatastore) AddSecondary(storage, relativePath string, storageNodeID int) error {
	res, err := sd.db.Exec(`
		INSERT INTO shard_secondaries (shard_id, storage_node_id)
		VALUES (SELECT id, $1 FROM shards WHERE storage = $2 AND relative_path = $3)`, storageNodeID, storage, relativePath)
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

// RemoveSecondary removes a secondary from a shard of a repository relative path
func (sd *SQLDatastore) RemoveSecondary(storage, relativePath string, storageNodeID int) error {
	res, err := sd.db.Exec(`
		DELETE FROM shard_secondaries (shard_relative_path, node_storage_id)
			WHERE shard_id = (SELECT id FROM shard where storage = $1 AND relative_path = $2) AND storage_node_id = $3`, storage, relativePath, storageNodeID)
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

// GetShard gets the shard for a repository relative path
func (sd *SQLDatastore) GetShard(storage, relativePath string) (*models.Shard, error) {
	primary, err := sd.GetPrimary(storage, relativePath)
	if err != nil {
		return nil, fmt.Errorf("getting primary: %v", err)
	}

	secondaries, err := sd.GetSecondaries(storage, relativePath)
	if err != nil {
		return nil, fmt.Errorf("getting secondaries: %v", err)
	}

	return &models.Shard{RelativePath: relativePath, Primary: *primary, Secondaries: secondaries}, nil
}

// RotatePrimary rotates a primary out of being primary, and picks a secondary of each shard at random to promote to the new primary
func (sd *SQLDatastore) RotatePrimary(primaryNodeStorageID int) error {

	// Add the primary as a secondary
	res, err := sd.db.Exec(`
	INSERT INTO shard_secondaries (shard_id, node_storage_id) VALUES (SELECT shards.id, shards.primary FROM shards WHERE shards.primary = $1)
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
              	(SELECT shard_secondaries.storage_node_id FROM shard_secondaries
									INNER JOIN shards ON shard_secondaries.shard_id = shards.id
              		WHERE shards.primary = $1 AND shards.primary != shard_secondaries.storage_node_id LIMIT 1)
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
