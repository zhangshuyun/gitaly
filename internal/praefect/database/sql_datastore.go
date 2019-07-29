package database

import (
	"errors"
	"fmt"
	"strings"

	"database/sql"

	// the lib/pg package provides postgres bindings for the sql package
	_ "github.com/lib/pq"

	"gitlab.com/gitlab-org/gitaly/internal/praefect/config"
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

// LoadFromConfig loads the config into the database
func (sd *SQLDatastore) LoadFromConfig(cfg config.Config) error {
	_, err := sd.db.Exec(insertStorageNodesQuery(cfg.StorageNodes))
	if err != nil {
		return fmt.Errorf("loading StorageNodes: %v", err)
	}

	return nil
}

func insertStorageNodesQuery(storageNodes []*models.StorageNode) string {
	q := `INSERT INTO storage_nodes (storage) VALUES %s ON CONFLICT (storage) DO NOTHING`

	var values []string
	for _, storageNode := range storageNodes {
		values = append(values, fmt.Sprintf(`('%s')`, storageNode.Storage))
	}

	return fmt.Sprintf(q, strings.Join(values, ","))
}

// GetReplicas gets the replicas for a repository based on the relative path
func (sd *SQLDatastore) GetReplicas(relativePath string) ([]models.StorageNode, error) {
	var replicas []models.StorageNode

	rows, err := sd.db.Query(`
	SELECT storage_nodes.id FROM repositories
		INNER JOIN repository_replicas ON repositories.id = repository_replicas.repository_id
		INNER JOIN storage_nodes ON storage_nodes.id = repository_replicas.storage_node_id WHERE repositories.relative_path = $1`, relativePath)

	if err != nil {
		return nil, err
	}

	for rows.Next() {
		var s models.StorageNode
		err = rows.Scan(&s.ID)
		if err != nil {
			return nil, err
		}
		replicas = append(replicas, s)
	}

	return replicas, nil
}

// GetStorageNode gets all storage storage_nodes
func (sd *SQLDatastore) GetStorageNode(nodeID int) (models.StorageNode, error) {
	var node models.StorageNode

	row := sd.db.QueryRow("SELECT storage_nodes.id, storage_nodes.storage FROM storage_nodes WHERE storage_nodes.id = $1", nodeID)

	err := row.Scan(&node.ID, &node.Storage)
	if err != nil {
		return node, err
	}

	return node, nil

}

// GetStorageNodes gets all storage storage_nodes
func (sd *SQLDatastore) GetStorageNodes() ([]models.StorageNode, error) {
	var nodeStorages []models.StorageNode

	rows, err := sd.db.Query("SELECT storage_nodes.id, storage_nodes.storage FROM storage_nodes")

	if err != nil {
		return nil, err
	}

	for rows.Next() {
		var nodeStorage models.StorageNode
		err = rows.Scan(&nodeStorage.ID, &nodeStorage.Storage)
		if err != nil {
			return nil, err
		}
		nodeStorages = append(nodeStorages, nodeStorage)
	}

	return nodeStorages, nil

}

// GetPrimary gets the primary storage node for a repository of a repository relative path
func (sd *SQLDatastore) GetPrimary(relativePath string) (*models.StorageNode, error) {

	row := sd.db.QueryRow(`
	SELECT storage_nodes.id, storage_nodes.storage FROM repositories
	  INNER JOIN storage_nodes ON repositories.primary = storage_nodes.id
	  WHERE repositories.relative_path = $1
	`, relativePath)

	var s models.StorageNode
	if err := row.Scan(&s.ID, &s.Storage); err != nil {
		return nil, err
	}

	return &s, nil
}

// SetPrimary sets the primary storagee node for a repository of a repository relative path
func (sd *SQLDatastore) SetPrimary(relativePath string, storageNodeID int) error {
	res, err := sd.db.Exec(`UPDATE repositories SET "primary" = $1 WHERE relative_path = $2`, storageNodeID, relativePath)
	if err != nil {
		return err
	}

	if n, err := res.RowsAffected(); err != nil {
		return err
	} else if n == 0 {
		res, err = sd.db.Exec(`INSERT INTO repositories (storage, relative_path, "primary") VALUES ($1, $2)`, relativePath, storageNodeID)
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

// AddReplica adds a replica to a repository of a repository relative path
func (sd *SQLDatastore) AddReplica(relativePath string, storageNodeID int) error {
	res, err := sd.db.Exec(`
		INSERT INTO repository_replicas (repository_id, storage_node_id)
		VALUES (SELECT id, $1 FROM repositories WHERE relative_path = $2)`, storageNodeID, relativePath)
	if err != nil {
		return err
	}

	if n, err := res.RowsAffected(); err != nil {
		return err
	} else if n == 0 {
		return errors.New("replica already exists")
	}

	return nil
}

// RemoveReplica removes a replica from a repository of a repository relative path
func (sd *SQLDatastore) RemoveReplica(relativePath string, storageNodeID int) error {
	res, err := sd.db.Exec(`
		DELETE FROM repository_replicas (repository_relative_path, node_storage_id)
			WHERE repository_id = (SELECT id FROM repository WHERE relative_path = $1) AND storage_node_id = $2`, relativePath, storageNodeID)
	if err != nil {
		return err
	}

	if n, err := res.RowsAffected(); err != nil {
		return err
	} else if n == 0 {
		return errors.New("replica did not exist")
	}

	return nil
}

// GetRepository gets the repository for a repository relative path
func (sd *SQLDatastore) GetRepository(relativePath string) (*models.Repository, error) {
	primary, err := sd.GetPrimary(relativePath)
	if err != nil {
		return nil, fmt.Errorf("getting primary: %v", err)
	}

	replicas, err := sd.GetReplicas(relativePath)
	if err != nil {
		return nil, fmt.Errorf("getting replicas: %v", err)
	}

	return &models.Repository{RelativePath: relativePath, Primary: *primary, Replicas: replicas}, nil
}

// RotatePrimary rotates a primary out of being primary, and picks a replica of each repository at random to promote to the new primary
func (sd *SQLDatastore) RotatePrimary(primaryNodeStorageID int) error {

	// Add the primary as a replica
	res, err := sd.db.Exec(`
	INSERT INTO repository_replicas (repository_id, node_storage_id) VALUES (SELECT repositories.id, repositories.primary FROM repositories WHERE repositories.primary = $1)
	`, primaryNodeStorageID)
	if err != nil {
		return err
	}

	affected, err := res.RowsAffected()
	if err != nil {
		return err
	}

	if affected == 0 {
		return fmt.Errorf("no repositories with primary %d found", primaryNodeStorageID)
	}

	// Choose a new replica
	res, err = sd.db.Exec(`UPDATE repositories SET "primary" =
              	(SELECT repository_replicas.storage_node_id FROM repository_replicas
									INNER JOIN repositories ON repository_replicas.repository_id = repositories.id
              		WHERE repositories.primary = $1 AND repositories.primary != repository_replicas.storage_node_id LIMIT 1)
              	`, primaryNodeStorageID)
	if err != nil {
		return err
	}

	affected, err = res.RowsAffected()
	if err != nil {
		return err
	}

	if affected == 0 {
		return errors.New("no replicas available to rotate")
	}
	return nil
}
