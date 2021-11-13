package datastore

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/lib/pq"
)

// RepositoryClusterPath identifies location of the repository in the cluster.
type RepositoryClusterPath struct {
	ClusterPath
	// RelativePath relative path to the repository on the disk.
	RelativePath string
}

// NewRepositoryClusterPath initializes and returns RepositoryClusterPath.
func NewRepositoryClusterPath(virtualStorage, storage, relativePath string) RepositoryClusterPath {
	return RepositoryClusterPath{
		ClusterPath: ClusterPath{
			VirtualStorage: virtualStorage,
			Storage:        storage,
		},
		RelativePath: relativePath,
	}
}

// ClusterPath represents path on the cluster to the storage.
type ClusterPath struct {
	// VirtualStorage is the name of the virtual storage.
	VirtualStorage string
	// Storage is the name of the gitaly storage.
	Storage string
}

// NewStorageCleanup initialises and returns a new instance of the StorageCleanup.
func NewStorageCleanup(db *sql.DB) *StorageCleanup {
	return &StorageCleanup{db: db}
}

// StorageCleanup provides methods on the database for the repository cleanup operation.
type StorageCleanup struct {
	db *sql.DB
}

// DoesntExist returns RepositoryClusterPath for each repository that doesn't exist in the database
// by querying repositories and storage_repositories tables.
func (ss *StorageCleanup) DoesntExist(ctx context.Context, virtualStorage, storage string, relativePath []string) ([]RepositoryClusterPath, error) {
	if len(relativePath) == 0 {
		return nil, nil
	}

	rows, err := ss.db.QueryContext(
		ctx,
		`SELECT $1 AS virtual_storage, $2 AS storage, UNNEST($3::TEXT[]) AS relative_path
		EXCEPT (
			SELECT virtual_storage, storage, relative_path
			FROM repositories
			JOIN storage_repositories USING (virtual_storage, relative_path)
			WHERE virtual_storage = $1 AND storage = $2 AND relative_path = ANY($3)
		)`,
		virtualStorage, storage, pq.StringArray(relativePath),
	)
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var res []RepositoryClusterPath
	for rows.Next() {
		var curr RepositoryClusterPath
		if err := rows.Scan(&curr.VirtualStorage, &curr.Storage, &curr.RelativePath); err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}
		res = append(res, curr)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("loop: %w", err)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("close: %w", err)
	}
	return res, nil
}
