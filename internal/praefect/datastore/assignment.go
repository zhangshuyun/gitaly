package datastore

import (
	"context"
	"fmt"

	"github.com/lib/pq"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/datastore/glsql"
)

// InvalidArgumentError tags the error as being caused by an invalid argument.
type InvalidArgumentError struct{ error }

func newVirtualStorageNotFoundError(virtualStorage string) error {
	return InvalidArgumentError{fmt.Errorf("virtual storage %q not found", virtualStorage)}
}

func newUnattainableReplicationFactorError(attempted, maximum int) error {
	return InvalidArgumentError{fmt.Errorf("attempted to set replication factor %d but virtual storage only contains %d storages", attempted, maximum)}
}

func newMinimumReplicationFactorError(replicationFactor int) error {
	return InvalidArgumentError{fmt.Errorf("attempted to set replication factor %d but minimum is 1", replicationFactor)}
}

func newRepositoryNotFoundError(virtualStorage, relativePath string) error {
	return InvalidArgumentError{fmt.Errorf("repository %q/%q not found", virtualStorage, relativePath)}
}

// AssignmentStore manages host assignments in Postgres.
type AssignmentStore struct {
	db                 glsql.Querier
	configuredStorages map[string][]string
}

// NewAssignmentStore returns a new AssignmentStore using the passed in database.
func NewAssignmentStore(db glsql.Querier, configuredStorages map[string][]string) AssignmentStore {
	return AssignmentStore{db: db, configuredStorages: configuredStorages}
}

func (s AssignmentStore) GetHostAssignments(ctx context.Context, virtualStorage, relativePath string) ([]string, error) {
	configuredStorages, ok := s.configuredStorages[virtualStorage]
	if !ok {
		return nil, newVirtualStorageNotFoundError(virtualStorage)
	}

	rows, err := s.db.QueryContext(ctx, `
SELECT storage
FROM repository_assignments
WHERE virtual_storage = $1
AND   relative_path = $2
AND   storage = ANY($3)
`, virtualStorage, relativePath, pq.StringArray(configuredStorages))
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}
	defer rows.Close()

	var assignedStorages []string
	for rows.Next() {
		var storage string
		if err := rows.Scan(&storage); err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}

		assignedStorages = append(assignedStorages, storage)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating rows: %w", err)
	}

	if len(assignedStorages) == 0 {
		return configuredStorages, nil
	}

	return assignedStorages, nil
}

// SetReplicationFactor assigns or unassigns a repository's host nodes until the desired replication factor is met.
// Please see the protobuf documentation of the method for details.
func (s AssignmentStore) SetReplicationFactor(ctx context.Context, virtualStorage, relativePath string, replicationFactor int) ([]string, error) {
	candidateStorages, ok := s.configuredStorages[virtualStorage]
	if !ok {
		return nil, newVirtualStorageNotFoundError(virtualStorage)
	}

	if replicationFactor < 1 {
		return nil, newMinimumReplicationFactorError(replicationFactor)
	}

	if max := len(candidateStorages); replicationFactor > max {
		return nil, newUnattainableReplicationFactorError(replicationFactor, max)
	}

	// The query works as follows:
	//
	// 1. `repository` CTE locks the repository's record for the duration of the update.
	//    This prevents concurrent updates to the `repository_assignments` table for the given
	//    repository. It is not sufficient to rely on row locks in `repository_assignments`
	//    as there might be rows being inserted or deleted in another transaction that
	//    our transaction does not lock. This could be the case if the replication factor
	//    is being increased concurrently from two different nodes and they assign different
	//    storages.
	//
	// 2. `existing_assignments` CTE gets the existing assignments for the repository. While
	//    there may be assignments in the database for storage nodes that were removed from the
	//    cluster, the query filters them out.
	//
	// 3. `created_assignments` CTE assigns new hosts to the repository if the replication
	//    factor has been increased. Random storages which are not yet assigned to the repository
	//    are picked until the replication factor is met. The primary of a repository is always
	//    assigned first.
	//
	// 4. `removed_assignments` CTE removes host assignments if the replication factor has been
	//    decreased. Primary is never removed as it needs a copy of the repository in order to
	//    accept writes. Random hosts are removed until the replication factor is met.
	//
	// 6. Finally we return the current set of assignments. CTE updates are not visible in the
	//    tables during the transaction. To account for that, we filter out removed assignments
	//    from the existing assignments. If the replication factor was increased, we'll include the
	//    created assignments. If the replication factor did not change, the query returns the
	//    current assignments.
	rows, err := s.db.QueryContext(ctx, `
WITH repository AS (
	SELECT virtual_storage, relative_path, "primary"
	FROM repositories
	WHERE virtual_storage = $1
	AND   relative_path   = $2
	FOR UPDATE
),

existing_assignments AS (
	SELECT storage
	FROM repository
	JOIN repository_assignments USING (virtual_storage, relative_path)
	WHERE storage = ANY($4::text[])
),

created_assignments AS (
	INSERT INTO repository_assignments
	SELECT virtual_storage, relative_path, storage
	FROM repository
	CROSS JOIN ( SELECT unnest($4::text[]) AS storage ) AS configured_storages
	WHERE storage NOT IN ( SELECT storage FROM existing_assignments )
	ORDER BY CASE WHEN storage = "primary" THEN 1 ELSE 0 END DESC, random()
	LIMIT ( SELECT GREATEST(COUNT(*), $3) - COUNT(*) FROM existing_assignments )
	RETURNING storage
),

removed_assignments AS (
	DELETE FROM repository_assignments
	USING (
		SELECT virtual_storage, relative_path, storage
		FROM repository, existing_assignments
		WHERE storage != "primary"
		ORDER BY random()
		LIMIT ( SELECT COUNT(*) - LEAST(COUNT(*), $3)  FROM existing_assignments )
	) AS removals
	WHERE repository_assignments.virtual_storage = removals.virtual_storage
	AND   repository_assignments.relative_path   = removals.relative_path
	AND   repository_assignments.storage         = removals.storage
	RETURNING removals.storage
)

SELECT storage
FROM existing_assignments
WHERE storage NOT IN ( SELECT storage FROM removed_assignments )
UNION
SELECT storage
FROM created_assignments
ORDER BY storage
	`, virtualStorage, relativePath, replicationFactor, pq.StringArray(candidateStorages))
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}

	defer rows.Close()

	var storages []string
	for rows.Next() {
		var storage string
		if err := rows.Scan(&storage); err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}

		storages = append(storages, storage)
	}

	if len(storages) == 0 {
		return nil, newRepositoryNotFoundError(virtualStorage, relativePath)
	}

	return storages, rows.Err()
}
