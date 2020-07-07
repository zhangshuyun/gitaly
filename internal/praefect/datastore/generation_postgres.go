package datastore

import (
	"context"
	"database/sql"
	"errors"

	"github.com/lib/pq"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/datastore/glsql"
)

// GenerationUnknown is used to indicate lack of generation number in
// a replication job. Older instances can produce replication jobs
// without a generation number.
const GenerationUnknown = -1

var (
	errDowngradeAttempted    = errors.New("downgrade attempted")
	errUnknownVirtualStorage = errors.New("unknown virtual storage")
)

// GenerationStore provides access to repositoy generation metadata.
type GenerationStore interface {
	// IncrementGeneration increments the repository's generation and sets the storage's generation
	// to match the value.
	IncrementGeneration(ctx context.Context, virtualStorage, storage, relativePath string) (int, error)
	// SetGeneration sets the repository's generation on the given storage.
	SetGeneration(ctx context.Context, virtualStorage, storage, relativePath string, generation int) error
	// EnsureUpgrade returns an error if the given generation would downgrade the repository on the storage.
	EnsureUpgrade(ctx context.Context, virtualStorage, storage, relativePath string, generation int) error
	// DeleteRecord deletes a storage's entry for the repository.
	DeleteRecord(ctx context.Context, virtualStorage, storage, relativePath string) error
	// GetOutdatedRepositories gets all storage's which have outdated repositories and lists how many
	// generations they are behind.
	GetOutdatedRepositories(ctx context.Context, virtualStorage string) (map[string]map[string]int, error)
}

// PostgresGenerationStore is an in-memory implementation of GenerationStore.
// Refer to the interface for method documentation.
type PostgresGenerationStore struct {
	db       glsql.Querier
	storages map[string][]string
}

// NewLocalGenerationStore returns a Postgres implementation of GenerationStore.
func NewPostgresGenerationStore(db glsql.Querier, storages map[string][]string) *PostgresGenerationStore {
	return &PostgresGenerationStore{db: db, storages: storages}
}

func (rs *PostgresGenerationStore) IncrementGeneration(ctx context.Context, virtualStorage, storage, relativePath string) (int, error) {
	const q = `
WITH next AS (
	INSERT INTO repository_generations (
		virtual_storage,
		relative_path,
		generation
	) VALUES ($1, $2, 0)
	ON CONFLICT (virtual_storage, relative_path) DO
		UPDATE SET generation = repository_generations.generation + 1
	RETURNING virtual_storage, relative_path, generation
)

INSERT INTO storage_generations (
	virtual_storage,
	relative_path,
	storage,
	generation
)
SELECT virtual_storage, relative_path, $3, generation
FROM next
ON CONFLICT (virtual_storage, relative_path, storage) DO
	UPDATE SET generation = EXCLUDED.generation
RETURNING generation
`

	var generation int
	if err := rs.db.QueryRowContext(ctx, q, virtualStorage, relativePath, storage).Scan(&generation); err != nil {
		return 0, err
	}

	return generation, nil
}

func (rs *PostgresGenerationStore) SetGeneration(ctx context.Context, virtualStorage, storage, relativePath string, generation int) error {
	const q = `
WITH repository AS (
	INSERT INTO repository_generations (
		virtual_storage,
		relative_path,
		generation
	) VALUES ($1, $2, $4)
	ON CONFLICT (virtual_storage, relative_path) DO
		UPDATE SET generation = EXCLUDED.generation
		WHERE repository_generations.generation < EXCLUDED.generation
)

INSERT INTO storage_generations (
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

func (rs *PostgresGenerationStore) EnsureUpgrade(ctx context.Context, virtualStorage, storage, relativePath string, generation int) error {
	const q = `
SELECT generation
FROM storage_generations
WHERE virtual_storage = $1
AND relative_path = $2
AND storage = $3
`
	var current int
	if err := rs.db.QueryRowContext(ctx, q, virtualStorage, relativePath, storage).Scan(&current); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}

		return err
	}

	if current >= generation {
		return errDowngradeAttempted
	}

	return nil
}

func (rs *PostgresGenerationStore) DeleteRecord(ctx context.Context, virtualStorage, storage, relativePath string) error {
	const q = `
DELETE FROM storage_generations
WHERE virtual_storage = $1 
AND storage = $2 
AND relative_path = $3
`

	_, err := rs.db.ExecContext(ctx, q, virtualStorage, storage, relativePath)
	return err
}

func (rs *PostgresGenerationStore) GetOutdatedRepositories(ctx context.Context, virtualStorage string) (map[string]map[string]int, error) {
	// As some storages might be missing records from the table, we do a cross join between the repositories
	// configured storages. If a storage is missing an entry, it is considered fully outdated.
	const q = `
WITH repositories AS (
	SELECT virtual_storage, relative_path, generation
	FROM repository_generations
	WHERE virtual_storage = $1
)

SELECT storages.name, repository.relative_path, repository.generation - COALESCE(storage.generation, -1) AS behind_by
FROM (SELECT unnest($2::text[]) AS name) AS storages
CROSS JOIN repositories AS repository
LEFT JOIN storage_generations AS storage
	ON storage.virtual_storage = repository.virtual_storage 
	AND storage.relative_path = repository.relative_path  
	AND storage.storage = storages.name
WHERE COALESCE(storage.generation, -1) < repository.generation
`
	storages, ok := rs.storages[virtualStorage]
	if !ok {
		return nil, errUnknownVirtualStorage
	}

	rows, err := rs.db.QueryContext(ctx, q, virtualStorage, pq.StringArray(storages))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	outdated := make(map[string]map[string]int)
	for rows.Next() {
		var storage, relativePath string
		var difference int
		if err := rows.Scan(&storage, &relativePath, &difference); err != nil {
			return nil, err
		}

		storages := outdated[relativePath]
		if storages == nil {
			storages = make(map[string]int)
		}

		storages[storage] = difference
		outdated[relativePath] = storages
	}

	return outdated, rows.Err()
}
