package datastore

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/lib/pq"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/commonerr"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/datastore/glsql"
)

type storages map[string][]string

// GenerationUnknown is used to indicate lack of generation number in
// a replication job. Older instances can produce replication jobs
// without a generation number.
const GenerationUnknown = -1

var errWriteToOutdatedNodes = errors.New("write to outdated nodes")

// DowngradeAttemptedError is returned when attempting to get the replicated generation for a source repository
// that does not upgrade the target repository.
type DowngradeAttemptedError struct {
	Storage             string
	CurrentGeneration   int
	AttemptedGeneration int
}

func (err DowngradeAttemptedError) Error() string {
	return fmt.Sprintf("attempted downgrading storage %q from generation %d to %d",
		err.Storage, err.CurrentGeneration, err.AttemptedGeneration,
	)
}

// RepositoryNotExistsError is returned when trying to perform an operation on a non-existent repository.
type RepositoryNotExistsError struct {
	virtualStorage string
	relativePath   string
	storage        string
}

// Is checks whether the other errors is of the same type.
func (err RepositoryNotExistsError) Is(other error) bool {
	_, ok := other.(RepositoryNotExistsError)
	return ok
}

// Error returns the errors message.
func (err RepositoryNotExistsError) Error() string {
	return fmt.Sprintf("repository %q -> %q -> %q does not exist",
		err.virtualStorage, err.relativePath, err.storage,
	)
}

// RepositoryExistsError is returned when trying to create a repository that already exists.
type RepositoryExistsError struct {
	virtualStorage string
	relativePath   string
	storage        string
}

// Is checks whether the other errors is of the same type.
func (err RepositoryExistsError) Is(other error) bool {
	//nolint:errorlint
	_, ok := other.(RepositoryExistsError)
	return ok
}

// Error returns the errors message.
func (err RepositoryExistsError) Error() string {
	return fmt.Sprintf("repository %q -> %q -> %q already exists",
		err.virtualStorage, err.relativePath, err.storage,
	)
}

// ErrNoRowsAffected is returned when a query did not perform any changes.
var ErrNoRowsAffected = errors.New("no rows were affected by the query")

// RepositoryStore provides access to repository state.
type RepositoryStore interface {
	// GetGeneration gets the repository's generation on a given storage.
	GetGeneration(ctx context.Context, repositoryID int64, storage string) (int, error)
	// IncrementGeneration increments the generations of up to date nodes.
	IncrementGeneration(ctx context.Context, repositoryID int64, primary string, secondaries []string) error
	// SetGeneration sets the repository's generation on the given storage. If the generation is higher
	// than the virtual storage's generation, it is set to match as well to guarantee monotonic increments.
	SetGeneration(ctx context.Context, repositoryID int64, storage, relativePath string, generation int) error
	// GetReplicaPath gets the replica path of a repository. Returns a commonerr.ErrRepositoryNotFound if a record
	// for the repository ID is not found.
	GetReplicaPath(ctx context.Context, repositoryID int64) (string, error)
	// GetReplicatedGeneration returns the generation propagated by applying the replication. If the generation would
	// downgrade, a DowngradeAttemptedError is returned.
	GetReplicatedGeneration(ctx context.Context, repositoryID int64, source, target string) (int, error)
	// CreateRepository creates a record for a repository in the specified virtual storage and relative path.
	// Primary is the storage the repository was created on. UpdatedSecondaries are secondaries that participated
	// and successfully completed the transaction. OutdatedSecondaries are secondaries that were outdated or failed
	// the transaction. Returns RepositoryExistsError when trying to create a repository which already exists in the store.
	//
	// storePrimary should be set when repository specific primaries are enabled. When set, the primary is stored as
	// the repository's primary.
	//
	// storeAssignments should be set when variable replication factor is enabled. When set, the primary and the
	// secondaries are stored as the assigned hosts of the repository.
	CreateRepository(ctx context.Context, repositoryID int64, virtualStorage, relativePath, replicaPath, primary string, updatedSecondaries, outdatedSecondaries []string, storePrimary, storeAssignments bool) error
	// SetAuthoritativeReplica sets the given replica of a repsitory as the authoritative one by setting its generation as the latest one.
	SetAuthoritativeReplica(ctx context.Context, virtualStorage, relativePath, storage string) error
	// DeleteRepository deletes the database records associated with the repository. It returns the replica path and the storages
	// which are known to have a replica at the time of deletion. commonerr.RepositoryNotFoundError is returned when
	// the repository is not tracked by the Praefect datastore.
	DeleteRepository(ctx context.Context, virtualStorage, relativePath string) (string, []string, error)
	// DeleteReplica deletes a replica of a repository from a storage without affecting other state in the virtual storage.
	DeleteReplica(ctx context.Context, repositoryID int64, storage string) error
	// RenameRepository updates a repository's relative path. It renames the virtual storage wide record as well
	// as the storage's which is calling it. Returns RepositoryNotExistsError when trying to rename a repository
	// which has no record in the virtual storage or the storage.
	RenameRepository(ctx context.Context, virtualStorage, relativePath, storage, newRelativePath string) error
	// GetConsistentStoragesByRepositoryID returns the replica path and the set of up to date storages for the given repository keyed by repository ID.
	GetConsistentStoragesByRepositoryID(ctx context.Context, repositoryID int64) (string, map[string]struct{}, error)
	ConsistentStoragesGetter
	// RepositoryExists returns whether the repository exists on a virtual storage.
	RepositoryExists(ctx context.Context, virtualStorage, relativePath string) (bool, error)
	// GetPartiallyAvailableRepositories returns information on repositories which have assigned replicas which
	// are not able to serve requests at the moment.
	GetPartiallyAvailableRepositories(ctx context.Context, virtualStorage string) ([]RepositoryMetadata, error)
	// DeleteInvalidRepository is a method for deleting records of invalid repositories. It deletes a storage's
	// record of the invalid repository. If the storage was the only storage with the repository, the repository's
	// record on the virtual storage is also deleted.
	DeleteInvalidRepository(ctx context.Context, repositoryID int64, storage string) error
	// ReserveRepositoryID reserves an ID for a repository that is about to be created and returns it. If a repository already
	// exists with the given virtual storage and relative path combination, an error is returned.
	ReserveRepositoryID(ctx context.Context, virtualStorage, relativePath string) (int64, error)
	// GetRepositoryID gets the ID of the repository identified via the given virtual storage and relative path. Returns a
	// RepositoryNotFoundError if the repository doesn't exist.
	GetRepositoryID(ctx context.Context, virtualStorage, relativePath string) (int64, error)
	// GetRepositoryMetadata retrieves a repository's metadata.
	GetRepositoryMetadata(ctx context.Context, repositoryID int64) (RepositoryMetadata, error)
	// GetRepositoryMetadataByPath retrieves a repository's metadata by its virtual path.
	GetRepositoryMetadataByPath(ctx context.Context, virtualStorage, relativePath string) (RepositoryMetadata, error)
}

// PostgresRepositoryStore is a Postgres implementation of RepositoryStore.
// Refer to the interface for method documentation.
type PostgresRepositoryStore struct {
	db glsql.Querier
	storages
}

// NewPostgresRepositoryStore returns a Postgres implementation of RepositoryStore.
func NewPostgresRepositoryStore(db glsql.Querier, configuredStorages map[string][]string) *PostgresRepositoryStore {
	return &PostgresRepositoryStore{db: db, storages: storages(configuredStorages)}
}

//nolint: revive,stylecheck // This is unintentionally missing documentation.
func (rs *PostgresRepositoryStore) GetGeneration(ctx context.Context, repositoryID int64, storage string) (int, error) {
	const q = `
SELECT generation
FROM storage_repositories
WHERE repository_id = $1
AND storage = $2
`

	var gen int
	if err := rs.db.QueryRowContext(ctx, q, repositoryID, storage).Scan(&gen); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return GenerationUnknown, nil
		}

		return 0, err
	}

	return gen, nil
}

//nolint: revive,stylecheck // This is unintentionally missing documentation.
func (rs *PostgresRepositoryStore) IncrementGeneration(ctx context.Context, repositoryID int64, primary string, secondaries []string) error {
	const q = `
WITH updated_replicas AS (
	UPDATE storage_repositories
	SET generation = generation + 1
	FROM (
		SELECT repository_id, storage
		FROM repositories
		JOIN storage_repositories USING (repository_id, generation)
		WHERE repository_id = $1
		AND   storage       = ANY($2)
		FOR UPDATE
	) AS to_update
	WHERE storage_repositories.repository_id = to_update.repository_id
	AND   storage_repositories.storage       = to_update.storage
	RETURNING storage_repositories.repository_id
),

updated_repository AS (
	UPDATE repositories
	SET generation = generation + 1
	FROM (
		SELECT DISTINCT repository_id
		FROM updated_replicas
	) AS updated_repositories
	WHERE repositories.repository_id = updated_repositories.repository_id
)

SELECT
	EXISTS (
		SELECT FROM repositories
		WHERE repository_id = $1
	) AS repository_exists,
	EXISTS ( SELECT FROM updated_replicas ) AS repository_updated
`
	var repositoryExists, repositoryUpdated bool
	if err := rs.db.QueryRowContext(
		ctx, q, repositoryID, pq.StringArray(append(secondaries, primary)),
	).Scan(&repositoryExists, &repositoryUpdated); err != nil {
		return fmt.Errorf("scan: %w", err)
	}

	if !repositoryExists {
		return commonerr.ErrRepositoryNotFound
	}

	if !repositoryUpdated {
		return errWriteToOutdatedNodes
	}

	return nil
}

//nolint: revive,stylecheck // This is unintentionally missing documentation.
func (rs *PostgresRepositoryStore) SetGeneration(ctx context.Context, repositoryID int64, storage, relativePath string, generation int) error {
	const q = `
WITH repository AS (
	UPDATE repositories SET generation = $3
	WHERE repository_id = $1
	AND   COALESCE(repositories.generation, -1) < $3
)

INSERT INTO storage_repositories (
	repository_id,
	virtual_storage,
	relative_path,
	storage,
	generation
)
SELECT
	repository_id,
	virtual_storage,
	$4,
	$2,
	$3
FROM repositories
WHERE repository_id = $1
ON CONFLICT (repository_id, storage) DO UPDATE SET
	relative_path = EXCLUDED.relative_path,
	generation = EXCLUDED.generation
`

	_, err := rs.db.ExecContext(ctx, q, repositoryID, storage, generation, relativePath)
	return err
}

// SetAuthoritativeReplica sets the given replica of a repsitory as the authoritative one by setting its generation as the latest one.
func (rs *PostgresRepositoryStore) SetAuthoritativeReplica(ctx context.Context, virtualStorage, relativePath, storage string) error {
	result, err := rs.db.ExecContext(ctx, `
WITH updated_repository AS (
	UPDATE repositories
	SET generation = generation + 1
	WHERE virtual_storage = $1
	AND   relative_path   = $2
	RETURNING repository_id, virtual_storage, relative_path, generation
)

INSERT INTO storage_repositories (repository_id, virtual_storage, relative_path, storage, generation)
SELECT repository_id, virtual_storage, relative_path, $3, generation
FROM updated_repository
ON CONFLICT (virtual_storage, relative_path, storage) DO UPDATE
	SET repository_id = EXCLUDED.repository_id,
	    generation = EXCLUDED.generation
	`, virtualStorage, relativePath, storage)
	if err != nil {
		return fmt.Errorf("exec: %w", err)
	}

	if rowsAffected, err := result.RowsAffected(); err != nil {
		return fmt.Errorf("rows affected: %w", err)
	} else if rowsAffected == 0 {
		return commonerr.NewRepositoryNotFoundError(virtualStorage, relativePath)
	}

	return nil
}

//nolint: revive,stylecheck // This is unintentionally missing documentation.
func (rs *PostgresRepositoryStore) GetReplicatedGeneration(ctx context.Context, repositoryID int64, source, target string) (int, error) {
	const q = `
SELECT storage, generation
FROM storage_repositories
WHERE repository_id = $1
AND storage = ANY($2)
`

	rows, err := rs.db.QueryContext(ctx, q, repositoryID, pq.StringArray([]string{source, target}))
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	sourceGeneration := GenerationUnknown
	targetGeneration := GenerationUnknown
	for rows.Next() {
		var storage string
		var generation int
		if err := rows.Scan(&storage, &generation); err != nil {
			return 0, err
		}

		switch storage {
		case source:
			sourceGeneration = generation
		case target:
			targetGeneration = generation
		default:
			return 0, fmt.Errorf("unexpected storage: %s", storage)
		}
	}

	if err := rows.Err(); err != nil {
		return 0, err
	}

	if targetGeneration != GenerationUnknown && targetGeneration >= sourceGeneration {
		return 0, DowngradeAttemptedError{
			Storage:             target,
			CurrentGeneration:   targetGeneration,
			AttemptedGeneration: sourceGeneration,
		}
	}

	return sourceGeneration, nil
}

// CreateRepository creates a record for a repository in the specified virtual storage and relative path.
// Primary is the storage the repository was created on. UpdatedSecondaries are secondaries that participated
// and successfully completed the transaction. OutdatedSecondaries are secondaries that were outdated or failed
// the transaction. Returns RepositoryExistsError when trying to create a repository which already exists in the store.
//
// storePrimary should be set when repository specific primaries are enabled. When set, the primary is stored as
// the repository's primary.
//
// storeAssignments should be set when variable replication factor is enabled. When set, the primary and the
// secondaries are stored as the assigned hosts of the repository.
func (rs *PostgresRepositoryStore) CreateRepository(ctx context.Context, repositoryID int64, virtualStorage, relativePath, replicaPath, primary string, updatedSecondaries, outdatedSecondaries []string, storePrimary, storeAssignments bool) error {
	const q = `
WITH repo AS (
	INSERT INTO repositories (
		repository_id,
		virtual_storage,
		relative_path,
		replica_path,
		generation,
		"primary"
	) VALUES ($8, $1, $2, $9, 0, CASE WHEN $4 THEN $3 END)
),

assignments AS (
	INSERT INTO repository_assignments (
		repository_id,
		virtual_storage,
		relative_path,
		storage
	)
	SELECT $8, $1, $2, storage
	FROM (
		SELECT $3 AS storage
		UNION
		SELECT unnest($5::text[])
		UNION
		SELECT unnest($6::text[])
	) AS storages
	WHERE $7
)

INSERT INTO storage_repositories (
	repository_id,
	virtual_storage,
	relative_path,
	storage,
	generation
)
SELECT $8, $1, $2, storage, 0
FROM (
	SELECT $3 AS storage
	UNION
	SELECT unnest($5::text[])
) AS updated_storages
`

	_, err := rs.db.ExecContext(ctx, q,
		virtualStorage,
		relativePath,
		primary,
		storePrimary,
		pq.StringArray(updatedSecondaries),
		pq.StringArray(outdatedSecondaries),
		storeAssignments,
		repositoryID,
		replicaPath,
	)

	var pqerr *pq.Error
	if errors.As(err, &pqerr) && pqerr.Code.Name() == "unique_violation" {
		if pqerr.Constraint == "repositories_pkey" {
			return fmt.Errorf("repository id %d already in use", repositoryID)
		}

		return RepositoryExistsError{
			virtualStorage: virtualStorage,
			relativePath:   relativePath,
			storage:        primary,
		}
	}

	return err
}

//nolint: revive,stylecheck // This is unintentionally missing documentation.
func (rs *PostgresRepositoryStore) DeleteRepository(ctx context.Context, virtualStorage, relativePath string) (string, []string, error) {
	var (
		replicaPath string
		storages    pq.StringArray
	)

	if err := rs.db.QueryRowContext(ctx, `
WITH repository AS (
	DELETE FROM repositories
	WHERE virtual_storage = $1
	AND relative_path = $2
	RETURNING repository_id, replica_path
)

SELECT replica_path, ARRAY_AGG(storage_repositories.storage)
FROM repository
LEFT JOIN storage_repositories USING (repository_id)
GROUP BY replica_path
		`, virtualStorage, relativePath,
	).Scan(&replicaPath, &storages); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", nil, commonerr.NewRepositoryNotFoundError(virtualStorage, relativePath)
		}

		return "", nil, fmt.Errorf("scan: %w", err)
	}

	return replicaPath, storages, nil
}

// DeleteReplica deletes a record from the `storage_repositories`. See the interface documentation for details.
func (rs *PostgresRepositoryStore) DeleteReplica(ctx context.Context, repositoryID int64, storage string) error {
	result, err := rs.db.ExecContext(ctx, `
DELETE FROM storage_repositories
WHERE repository_id = $1
AND storage = $2
	`, repositoryID, storage)
	if err != nil {
		return err
	}

	if n, err := result.RowsAffected(); err != nil {
		return err
	} else if n == 0 {
		return ErrNoRowsAffected
	}

	return nil
}

//nolint: revive,stylecheck // This is unintentionally missing documentation.
func (rs *PostgresRepositoryStore) RenameRepository(ctx context.Context, virtualStorage, relativePath, storage, newRelativePath string) error {
	const q = `
WITH repo AS (
	UPDATE repositories
	SET relative_path = $4,
	    replica_path  = $4
	WHERE virtual_storage = $1
	AND relative_path = $2
)

UPDATE storage_repositories
SET relative_path = $4
WHERE virtual_storage = $1
AND relative_path = $2
AND storage = $3
`

	result, err := rs.db.ExecContext(ctx, q, virtualStorage, relativePath, storage, newRelativePath)
	if err != nil {
		return err
	}

	if n, err := result.RowsAffected(); err != nil {
		return err
	} else if n == 0 {
		return RepositoryNotExistsError{
			virtualStorage: virtualStorage,
			relativePath:   relativePath,
			storage:        storage,
		}
	}

	return err
}

// GetConsistentStoragesByRepositoryID returns the replica path and the set of up to date storages for the given repository keyed by repository ID.
func (rs *PostgresRepositoryStore) GetConsistentStoragesByRepositoryID(ctx context.Context, repositoryID int64) (string, map[string]struct{}, error) {
	return rs.getConsistentStorages(ctx, `
SELECT replica_path, ARRAY_AGG(storage)
FROM repositories
JOIN storage_repositories USING (repository_id, relative_path, generation)
WHERE repository_id = $1
GROUP BY replica_path
	`, repositoryID)
}

// GetConsistentStorages returns the replica path and the set of up to date storages for the given repository keyed by virtual storage and relative path.
func (rs *PostgresRepositoryStore) GetConsistentStorages(ctx context.Context, virtualStorage, relativePath string) (string, map[string]struct{}, error) {
	replicaPath, storages, err := rs.getConsistentStorages(ctx, `
SELECT replica_path, ARRAY_AGG(storage)
FROM repositories
JOIN storage_repositories USING (repository_id, relative_path, generation)
WHERE repositories.virtual_storage = $1
AND repositories.relative_path = $2
GROUP BY replica_path
	`, virtualStorage, relativePath)
	if errors.Is(err, commonerr.ErrRepositoryNotFound) {
		return "", nil, commonerr.NewRepositoryNotFoundError(virtualStorage, relativePath)
	}

	return replicaPath, storages, err
}

// getConsistentStorages is a helper for querying the consistent storages by different keys.
func (rs *PostgresRepositoryStore) getConsistentStorages(ctx context.Context, query string, params ...interface{}) (string, map[string]struct{}, error) {
	var replicaPath string
	var storages pq.StringArray
	if err := rs.db.QueryRowContext(ctx, query, params...).Scan(&replicaPath, &storages); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", nil, commonerr.ErrRepositoryNotFound
		}

		return "", nil, fmt.Errorf("query: %w", err)
	}

	consistentStorages := make(map[string]struct{}, len(storages))
	for _, storage := range storages {
		consistentStorages[storage] = struct{}{}
	}

	return replicaPath, consistentStorages, nil
}

//nolint: revive,stylecheck // This is unintentionally missing documentation.
func (rs *PostgresRepositoryStore) RepositoryExists(ctx context.Context, virtualStorage, relativePath string) (bool, error) {
	const q = `
SELECT true
FROM repositories
WHERE virtual_storage = $1
AND relative_path = $2
`

	var exists bool
	if err := rs.db.QueryRowContext(ctx, q, virtualStorage, relativePath).Scan(&exists); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}

		return false, err
	}

	return exists, nil
}

//nolint: revive,stylecheck // This is unintentionally missing documentation.
func (rs *PostgresRepositoryStore) DeleteInvalidRepository(ctx context.Context, repositoryID int64, storage string) error {
	_, err := rs.db.ExecContext(ctx, `
WITH invalid_repository AS (
	DELETE FROM storage_repositories
	WHERE repository_id = $1
	AND   storage = $2
)

DELETE FROM repositories
WHERE repository_id = $1
AND NOT EXISTS (
	SELECT 1
	FROM storage_repositories
	WHERE repository_id = $1
	AND storage != $2
)
	`, repositoryID, storage)
	return err
}

// Replica represents a replica of a repository.
type Replica struct {
	// Storage is the name of the replica's storage.
	Storage string
	// Generation is the replica's confirmed generation. If the replica does not yet exists, generation
	// is -1.
	Generation int64
	// Assigned indicates whether the storage is an assigned host of the repository.
	Assigned bool
	// Healthy indicates whether the replica is considered healthy by the consensus of Praefect nodes.
	Healthy bool
	// ValidPrimary indicates whether the replica is ready to serve as the primary if necessary.
	ValidPrimary bool
}

// RepositoryMetadata contains the repository's metadata.
type RepositoryMetadata struct {
	// RepositoryID is the internal id of the repository.
	RepositoryID int64
	// VirtualStorage is the virtual storage where the repository is.
	VirtualStorage string
	// RelativePath is the relative path of the repository.
	RelativePath string
	// ReplicaPath is the actual disk location where the replicas are stored in the storages.
	ReplicaPath string
	// Primary is the current primary of this repository.
	Primary string
	// Generation is the current generation of the repository.
	Generation int64
	// Replicas contains information of the repository on each storage that contains the repository
	// or does not contain the repository but is assigned to host it.
	Replicas []Replica
}

// GetRepositoryMetadata retrieves a repository's metadata.
func (rs *PostgresRepositoryStore) GetRepositoryMetadata(ctx context.Context, repositoryID int64) (RepositoryMetadata, error) {
	metadata, err := rs.getRepositoryMetadata(ctx, "WHERE repository_id = $3", "", repositoryID)
	if err != nil {
		return RepositoryMetadata{}, err
	}

	if len(metadata) == 0 {
		return RepositoryMetadata{}, commonerr.ErrRepositoryNotFound
	}

	return metadata[0], nil
}

// GetRepositoryMetadataByPath retrieves a repository's metadata by its virtual path.
func (rs *PostgresRepositoryStore) GetRepositoryMetadataByPath(ctx context.Context, virtualStorage, relativePath string) (RepositoryMetadata, error) {
	metadata, err := rs.getRepositoryMetadata(ctx, "WHERE virtual_storage = $3 AND relative_path = $4", "", virtualStorage, relativePath)
	if err != nil {
		return RepositoryMetadata{}, err
	}

	if len(metadata) == 0 {
		return RepositoryMetadata{}, commonerr.ErrRepositoryNotFound
	}

	return metadata[0], nil
}

// GetPartiallyAvailableRepositories returns information on repositories which have assigned replicas which
// are not able to serve requests at the moment.
func (rs *PostgresRepositoryStore) GetPartiallyAvailableRepositories(ctx context.Context, virtualStorage string) ([]RepositoryMetadata, error) {
	_, ok := rs.storages[virtualStorage]
	if !ok {
		return nil, fmt.Errorf("unknown virtual storage: %q", virtualStorage)
	}

	return rs.getRepositoryMetadata(ctx,
		"WHERE virtual_storage = $3",
		"HAVING bool_or(NOT valid_primary) FILTER(WHERE assigned)",
		virtualStorage)
}

// GetPartiallyAvailableRepositories returns information on repositories which have assigned replicas which
// are not able to serve requests at the moment.
func (rs *PostgresRepositoryStore) getRepositoryMetadata(ctx context.Context, keyFilter, groupFilter string, filterArgs ...interface{}) ([]RepositoryMetadata, error) {
	// The query below gets the status of every repository which has one or more assigned replicas that
	// are not able to serve requests at the moment. The status includes how many changes a replica is behind,
	// whether the replica is assigned host or not, whether the replica is healthy and whether the replica is
	// considered a valid primary candidate. It works as follows:
	//
	// 1. First we get all the storages which contain the repository from `storage_repositories`. We
	//    list every copy of the repository as the latest generation could exist on an unassigned
	//    storage.
	//
	// 2. We join `repository_assignments` table with fallback behavior in case the repository has no
	//    assignments. A storage is considered assigned if:
	//
	//    1. If the repository has no assignments, every configured storage is considered assigned.
	//    2. If the repository has assignments, the storage needs to be assigned explicitly.
	//    3. Assignments of unconfigured storages are treated as if they don't exist.
	//
	//    If none of the assigned storages are outdated, the repository is not considered outdated as
	//    the desired replication factor has been reached.
	//
	// 3. We join `repositories` table to filter out any repositories that have been deleted but still
	//    exist on some storages. While the `repository_assignments` has a foreign key on `repositories`
	//    and there can't be any assignments for deleted repositories, this is still needed as long as the
	//    fallback behavior of no assignments is in place.
	//
	// 4. We join the `healthy_storages` view to return the storages current health.
	//
	// 5. We join the `valid_primaries` view to return whether the storage is ready to act as a primary in case
	//    of a failover.
	//
	// 6. Finally we aggregate each repository's information in to a single row with a JSON object containing
	//    the information. This allows us to group the output already in the query and makes scanning easier
	//    We filter out groups which do not have an assigned storage as the replication factor on those
	//    is reached. Status of unassigned storages does not matter as long as they don't contain a later generation
	//    than the assigned ones.
	//

	var (
		virtualStorages []string
		storages        []string
	)

	for virtualStorage, configuredStorages := range rs.storages {
		for _, storage := range configuredStorages {
			virtualStorages = append(virtualStorages, virtualStorage)
			storages = append(storages, storage)
		}
	}

	args := append([]interface{}{pq.StringArray(virtualStorages), pq.StringArray(storages)}, filterArgs...)

	rows, err := rs.db.QueryContext(ctx, fmt.Sprintf(`
WITH configured_storages AS (
	SELECT unnest($1::text[]) AS virtual_storage,
	       unnest($2::text[]) AS storage
)

SELECT
	json_build_object (
		'RepositoryID', repository_id,
		'VirtualStorage', virtual_storage,
		'RelativePath', relative_path,
		'ReplicaPath', replica_path,
		'Primary', "primary",
		'Generation', generation,
		'Replicas', json_agg(
			json_build_object(
				'Storage', storage,
				'Generation', replica_generation,
				'Assigned', assigned,
				'Healthy', healthy,
				'ValidPrimary', valid_primary
			)
		)
	)
FROM (
	SELECT
		repository_id,
		virtual_storage,
		relative_path,
		replica_path,
		repositories.primary,
		repositories.generation,
		storage,
		COALESCE(storage_repositories.generation, -1) AS replica_generation,
		repository_assignments.storage IS NOT NULL AS assigned,
		healthy_storages.storage IS NOT NULL AS healthy,
		valid_primaries.storage IS NOT NULL AS valid_primary
	FROM ( SELECT repository_id, storage, generation FROM storage_repositories ) AS storage_repositories
	FULL JOIN (
		SELECT repository_id, storage
		FROM repositories
		JOIN configured_storages USING (virtual_storage)
		WHERE (
			SELECT COUNT(*) = 0 OR COUNT(*) FILTER (WHERE storage = configured_storages.storage) = 1
			FROM repository_assignments
			WHERE repository_id = repositories.repository_id
			AND   (virtual_storage, storage) IN (SELECT * FROM configured_storages)
		)
	) AS repository_assignments USING (repository_id, storage)
	JOIN repositories USING (repository_id)
	LEFT JOIN healthy_storages USING (virtual_storage, storage)
	LEFT JOIN ( SELECT repository_id, storage FROM valid_primaries ) AS valid_primaries USING (repository_id, storage)
	%s
	ORDER BY repository_id, storage
) AS outdated_repositories
GROUP BY repository_id, virtual_storage, relative_path, replica_path, "primary", generation
%s
ORDER BY repository_id
	`, keyFilter, groupFilter), args...)
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}
	defer rows.Close()

	var repos []RepositoryMetadata
	for rows.Next() {
		var repositoryJSON string
		if err := rows.Scan(&repositoryJSON); err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}

		var repo RepositoryMetadata
		if err := json.NewDecoder(strings.NewReader(repositoryJSON)).Decode(&repo); err != nil {
			return nil, fmt.Errorf("decode json: %w", err)
		}

		repos = append(repos, repo)
	}

	return repos, rows.Err()
}

// ReserveRepositoryID reserves an ID for a repository that is about to be created and returns it. If a repository already
// exists with the given virtual storage and relative path combination, an error is returned.
func (rs *PostgresRepositoryStore) ReserveRepositoryID(ctx context.Context, virtualStorage, relativePath string) (int64, error) {
	var id int64
	if err := rs.db.QueryRowContext(ctx, `
SELECT nextval('repositories_repository_id_seq')
WHERE NOT EXISTS (
	SELECT FROM repositories
	WHERE virtual_storage = $1
	AND   relative_path   = $2
)
	`, virtualStorage, relativePath).Scan(&id); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, commonerr.ErrRepositoryAlreadyExists
		}

		return 0, fmt.Errorf("scan: %w", err)
	}

	return id, nil
}

// GetRepositoryID gets the ID of the repository identified via the given virtual storage and relative path. Returns a
// RepositoryNotFoundError if the repository doesn't exist.
func (rs *PostgresRepositoryStore) GetRepositoryID(ctx context.Context, virtualStorage, relativePath string) (int64, error) {
	var id int64
	if err := rs.db.QueryRowContext(ctx, `
SELECT repository_id
FROM repositories
WHERE virtual_storage = $1
AND   relative_path   = $2
	`, virtualStorage, relativePath).Scan(&id); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, commonerr.NewRepositoryNotFoundError(virtualStorage, relativePath)
		}

		return 0, fmt.Errorf("scan: %w", err)
	}

	return id, nil
}

// GetReplicaPath gets the replica path of a repository. Returns a commonerr.ErrRepositoryNotFound if a record
// for the repository ID is not found.
func (rs *PostgresRepositoryStore) GetReplicaPath(ctx context.Context, repositoryID int64) (string, error) {
	var replicaPath string
	if err := rs.db.QueryRowContext(
		ctx, "SELECT replica_path FROM repositories WHERE repository_id = $1", repositoryID,
	).Scan(&replicaPath); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", commonerr.ErrRepositoryNotFound
		}

		return "", fmt.Errorf("scan: %w", err)
	}

	return replicaPath, nil
}
