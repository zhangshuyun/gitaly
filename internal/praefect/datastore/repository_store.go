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

// DowngradeAttemptedError is returned when attempting to get the replicated generation for a source repository
// that does not upgrade the target repository.
type DowngradeAttemptedError struct {
	VirtualStorage      string
	RelativePath        string
	Storage             string
	CurrentGeneration   int
	AttemptedGeneration int
}

func (err DowngradeAttemptedError) Error() string {
	return fmt.Sprintf("attempted downgrading %q -> %q -> %q from generation %d to %d",
		err.VirtualStorage, err.RelativePath, err.Storage, err.CurrentGeneration, err.AttemptedGeneration,
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
	GetGeneration(ctx context.Context, virtualStorage, relativePath, storage string) (int, error)
	// IncrementGeneration increments the primary's and the up to date secondaries' generations.
	IncrementGeneration(ctx context.Context, virtualStorage, relativePath, primary string, secondaries []string) error
	// SetGeneration sets the repository's generation on the given storage. If the generation is higher
	// than the virtual storage's generation, it is set to match as well to guarantee monotonic increments.
	SetGeneration(ctx context.Context, virtualStorage, relativePath, storage string, generation int) error
	// GetReplicatedGeneration returns the generation propagated by applying the replication. If the generation would
	// downgrade, a DowngradeAttemptedError is returned.
	GetReplicatedGeneration(ctx context.Context, virtualStorage, relativePath, source, target string) (int, error)
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
	CreateRepository(ctx context.Context, virtualStorage, relativePath, primary string, updatedSecondaries, outdatedSecondaries []string, storePrimary, storeAssignments bool) error
	// DeleteRepository deletes the repository's record from the virtual storage and the storages. Returns
	// ErrNoRowsAffected when trying to delete a repository which has no record in the virtual storage
	// or the storages.
	DeleteRepository(ctx context.Context, virtualStorage, relativePath string, storages []string) error
	// DeleteReplica deletes a replica of a repository from a storage without affecting other state in the virtual storage.
	DeleteReplica(ctx context.Context, virtualStorage, relativePath, storage string) error
	// RenameRepository updates a repository's relative path. It renames the virtual storage wide record as well
	// as the storage's which is calling it. Returns RepositoryNotExistsError when trying to rename a repository
	// which has no record in the virtual storage or the storage.
	RenameRepository(ctx context.Context, virtualStorage, relativePath, storage, newRelativePath string) error
	ConsistentStoragesGetter
	// RepositoryExists returns whether the repository exists on a virtual storage.
	RepositoryExists(ctx context.Context, virtualStorage, relativePath string) (bool, error)
	// GetPartiallyAvailableRepositories returns information on repositories which have assigned replicas which
	// are not able to serve requests at the moment.
	GetPartiallyAvailableRepositories(ctx context.Context, virtualStorage string) ([]PartiallyAvailableRepository, error)
	// DeleteInvalidRepository is a method for deleting records of invalid repositories. It deletes a storage's
	// record of the invalid repository. If the storage was the only storage with the repository, the repository's
	// record on the virtual storage is also deleted.
	DeleteInvalidRepository(ctx context.Context, virtualStorage, relativePath, storage string) error
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

func (rs *PostgresRepositoryStore) GetGeneration(ctx context.Context, virtualStorage, relativePath, storage string) (int, error) {
	const q = `
SELECT generation
FROM storage_repositories
WHERE virtual_storage = $1
AND relative_path = $2
AND storage = $3
`

	var gen int
	if err := rs.db.QueryRowContext(ctx, q, virtualStorage, relativePath, storage).Scan(&gen); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return GenerationUnknown, nil
		}

		return 0, err
	}

	return gen, nil
}

func (rs *PostgresRepositoryStore) IncrementGeneration(ctx context.Context, virtualStorage, relativePath, primary string, secondaries []string) error {
	// The query works as follows:
	//   1. `next_generation` CTE increments the latest generation by 1. If no previous records exists,
	//      the generation starts from 0.
	//   2. `base_generation` CTE gets the primary's current generation. A secondary has to be on the primary's
	//      generation, otherwise its generation won't be incremented. This avoids any issues where a concurrent
	//      reference transaction has failed and the secondary is no longer up to date when we are incrementing
	//      the generations.
	//   3. `eligible_secondaries` filters out secondaries which participated in a transaction but failed a
	///     concurrent transaction.
	//   4. `eligible_storages` CTE combines the primary and the up to date secondaries in a list of storages to
	//      increment the generation for.
	//   5. Finally, we update the records in 'storage_repositories' table to match the new generation for the
	//      eligible storages.

	const q = `
WITH next_generation AS (
	INSERT INTO repositories (
		virtual_storage,
		relative_path,
		generation
	) VALUES ($1, $2, 0)
	ON CONFLICT (virtual_storage, relative_path) DO
		UPDATE SET generation = COALESCE(repositories.generation, -1) + 1
	RETURNING virtual_storage, relative_path, generation
), base_generation AS (
	SELECT virtual_storage, relative_path, generation
	FROM storage_repositories
	WHERE virtual_storage = $1
	AND relative_path = $2
	AND storage = $3
	FOR UPDATE
), eligible_secondaries AS (
	SELECT storage
	FROM storage_repositories
	NATURAL JOIN base_generation
	WHERE storage = ANY($4::text[])
	FOR UPDATE
), eligible_storages AS (
	SELECT storage
	FROM eligible_secondaries
		UNION
	SELECT $3
)

UPDATE storage_repositories AS sr
SET generation = ng.generation
FROM eligible_storages AS es, next_generation AS ng
WHERE es.storage = sr.storage AND ng.virtual_storage = sr.virtual_storage AND ng.relative_path = sr.relative_path
`
	_, err := rs.db.ExecContext(ctx, q, virtualStorage, relativePath, primary, pq.StringArray(secondaries))
	return err
}

func (rs *PostgresRepositoryStore) SetGeneration(ctx context.Context, virtualStorage, relativePath, storage string, generation int) error {
	const q = `
WITH repository AS (
	INSERT INTO repositories (
		virtual_storage,
		relative_path,
		generation
	) VALUES ($1, $2, $4)
	ON CONFLICT (virtual_storage, relative_path) DO
		UPDATE SET generation = EXCLUDED.generation
		WHERE COALESCE(repositories.generation, -1) < EXCLUDED.generation
)

INSERT INTO storage_repositories (
	virtual_storage,
	relative_path,
	storage,
	generation
)
VALUES ($1, $2, $3, $4)
ON CONFLICT (virtual_storage, relative_path, storage) DO UPDATE SET
	generation = EXCLUDED.generation
`

	_, err := rs.db.ExecContext(ctx, q, virtualStorage, relativePath, storage, generation)
	return err
}

func (rs *PostgresRepositoryStore) GetReplicatedGeneration(ctx context.Context, virtualStorage, relativePath, source, target string) (int, error) {
	const q = `
SELECT storage, generation
FROM storage_repositories
WHERE virtual_storage = $1
AND relative_path = $2
AND storage = ANY($3)
`

	rows, err := rs.db.QueryContext(ctx, q, virtualStorage, relativePath, pq.StringArray([]string{source, target}))
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
			VirtualStorage:      virtualStorage,
			RelativePath:        relativePath,
			Storage:             target,
			CurrentGeneration:   targetGeneration,
			AttemptedGeneration: sourceGeneration,
		}
	}

	return sourceGeneration, nil
}

//nolint:stylecheck
//nolint:golint
func (rs *PostgresRepositoryStore) CreateRepository(ctx context.Context, virtualStorage, relativePath, primary string, updatedSecondaries, outdatedSecondaries []string, storePrimary, storeAssignments bool) error {
	const q = `
WITH repo AS (
	INSERT INTO repositories (
		virtual_storage,
		relative_path,
		generation,
		"primary"
	) VALUES ($1, $2, 0, CASE WHEN $4 THEN $3 END)
),

assignments AS (
	INSERT INTO repository_assignments (
		virtual_storage,
		relative_path,
		storage
	)
	SELECT $1, $2, storage
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
	virtual_storage,
	relative_path,
	storage,
	generation
)
SELECT $1, $2, storage, 0
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
	)

	var pqerr *pq.Error
	if errors.As(err, &pqerr) && pqerr.Code.Name() == "unique_violation" {
		return RepositoryExistsError{
			virtualStorage: virtualStorage,
			relativePath:   relativePath,
			storage:        primary,
		}
	}

	return err
}

func (rs *PostgresRepositoryStore) DeleteRepository(ctx context.Context, virtualStorage, relativePath string, storages []string) error {
	return rs.delete(ctx, `
WITH repo AS (
	DELETE FROM repositories
	WHERE virtual_storage = $1
	AND relative_path = $2
)

DELETE FROM storage_repositories
WHERE virtual_storage = $1
AND relative_path = $2
AND storage = ANY($3::text[])
		`, virtualStorage, relativePath, storages,
	)
}

// DeleteReplica deletes a record from the `storage_repositories`. See the interface documentation for details.
func (rs *PostgresRepositoryStore) DeleteReplica(ctx context.Context, virtualStorage, relativePath string, storage string) error {
	return rs.delete(ctx, `
DELETE FROM storage_repositories
WHERE virtual_storage = $1
AND relative_path = $2
AND storage = ANY($3::text[])
		`, virtualStorage, relativePath, []string{storage},
	)
}

func (rs *PostgresRepositoryStore) delete(ctx context.Context, query, virtualStorage, relativePath string, storages []string) error {
	result, err := rs.db.ExecContext(ctx, query, virtualStorage, relativePath, pq.StringArray(storages))
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

func (rs *PostgresRepositoryStore) RenameRepository(ctx context.Context, virtualStorage, relativePath, storage, newRelativePath string) error {
	const q = `
WITH repo AS (
	UPDATE repositories
	SET relative_path = $4
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

// GetConsistentStorages checks which storages are on the latest generation and returns them.
func (rs *PostgresRepositoryStore) GetConsistentStorages(ctx context.Context, virtualStorage, relativePath string) (map[string]struct{}, error) {
	const q = `
SELECT storage
FROM repositories
JOIN storage_repositories USING (virtual_storage, relative_path)
WHERE virtual_storage = $1
AND relative_path = $2
AND storage_repositories.generation = (
	SELECT MAX(generation)
	FROM storage_repositories
	WHERE virtual_storage = $1
	AND relative_path = $2
)`

	rows, err := rs.db.QueryContext(ctx, q, virtualStorage, relativePath)
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}
	defer rows.Close()

	consistentStorages := map[string]struct{}{}
	for rows.Next() {
		var storage string
		if err := rows.Scan(&storage); err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}

		consistentStorages[storage] = struct{}{}
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows: %w", err)
	}

	if len(consistentStorages) == 0 {
		return nil, commonerr.NewRepositoryNotFoundError(virtualStorage, relativePath)
	}

	return consistentStorages, nil
}

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

func (rs *PostgresRepositoryStore) DeleteInvalidRepository(ctx context.Context, virtualStorage, relativePath, storage string) error {
	_, err := rs.db.ExecContext(ctx, `
WITH invalid_repository AS (
	DELETE FROM storage_repositories
	WHERE virtual_storage = $1
	AND   relative_path = $2
	AND   storage = $3
)

DELETE FROM repositories
WHERE virtual_storage = $1
AND relative_path = $2
AND NOT EXISTS (
	SELECT 1
	FROM storage_repositories
	WHERE virtual_storage = $1
	AND relative_path = $2
	AND storage != $3
)
	`, virtualStorage, relativePath, storage)
	return err
}

// StorageDetails represents a storage that contains or should contain a
// copy of the repository.
type StorageDetails struct {
	// Name of the storage as configured.
	Name string
	// BehindBy indicates how many generations the storage's copy of the repository is missing at maximum.
	BehindBy int
	// Assigned indicates whether the storage is an assigned host of the repository.
	Assigned bool
	// Healthy indicates whether the replica is considered healthy by the consensus of Praefect nodes.
	Healthy bool
	// ValidPrimary indicates whether the replica is ready to serve as the primary if necessary.
	ValidPrimary bool
}

// PartiallyAvailableRepository is a repository with one or more assigned replicas which are not
// able to serve requests at the moment.
type PartiallyAvailableRepository struct {
	// RelativePath is the relative path of the repository.
	RelativePath string
	// Primary is the current primary of this repository.
	Primary string
	// Storages contains information of the repository on each storage that contains the repository
	// or does not contain the repository but is assigned to host it.
	Storages []StorageDetails
}

// GetPartiallyAvailableRepositories returns information on repositories which have assigned replicas which
// are not able to serve requests at the moment.
func (rs *PostgresRepositoryStore) GetPartiallyAvailableRepositories(ctx context.Context, virtualStorage string) ([]PartiallyAvailableRepository, error) {
	configuredStorages, ok := rs.storages[virtualStorage]
	if !ok {
		return nil, fmt.Errorf("unknown virtual storage: %q", virtualStorage)
	}

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
	rows, err := rs.db.QueryContext(ctx, `
SELECT
	json_build_object (
		'RelativePath', relative_path,
		'Primary', "primary",
		'Storages', json_agg(
			json_build_object(
				'Name', storage,
				'BehindBy', behind_by,
				'Assigned', assigned,
				'Healthy', healthy,
				'ValidPrimary', valid_primary
			)
		)
	)
FROM (
	SELECT
		relative_path,
		repositories.primary,
		storage,
		repository_generations.generation - COALESCE(storage_repositories.generation, -1) AS behind_by,
		repository_assignments.storage IS NOT NULL AS assigned,
		healthy_storages.storage IS NOT NULL AS healthy,
		valid_primaries.storage IS NOT NULL AS valid_primary
	FROM storage_repositories
	FULL JOIN (
		SELECT virtual_storage, relative_path, storage
		FROM repositories
		CROSS JOIN (SELECT unnest($2::text[]) AS storage) AS configured_storages
		WHERE (
			SELECT COUNT(*) = 0 OR COUNT(*) FILTER (WHERE storage = configured_storages.storage) = 1
			FROM repository_assignments
			WHERE virtual_storage = repositories.virtual_storage
			AND   relative_path   = repositories.relative_path
			AND   storage         = ANY($2::text[])
		)
	) AS repository_assignments USING (virtual_storage, relative_path, storage)
	JOIN repositories USING (virtual_storage, relative_path)
	JOIN repository_generations USING (virtual_storage, relative_path)
	LEFT JOIN healthy_storages USING (virtual_storage, storage)
	LEFT JOIN valid_primaries USING (virtual_storage, relative_path, storage)
	WHERE virtual_storage = $1
	ORDER BY relative_path, "primary", storage
) AS outdated_repositories
GROUP BY relative_path, "primary"
HAVING bool_or(NOT valid_primary) FILTER(WHERE assigned)
ORDER BY relative_path, "primary"
	`, virtualStorage, pq.StringArray(configuredStorages))
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}
	defer rows.Close()

	var repos []PartiallyAvailableRepository
	for rows.Next() {
		var repositoryJSON string
		if err := rows.Scan(&repositoryJSON); err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}

		var repo PartiallyAvailableRepository
		if err := json.NewDecoder(strings.NewReader(repositoryJSON)).Decode(&repo); err != nil {
			return nil, fmt.Errorf("decode json: %w", err)
		}

		repos = append(repos, repo)
	}

	return repos, rows.Err()
}
