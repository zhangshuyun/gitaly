package datastore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/lib/pq"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper"
)

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

// Populate adds storage to the set, so it can be acquired afterwards.
func (ss *StorageCleanup) Populate(ctx context.Context, virtualStorage, storage string) error {
	if _, err := ss.db.ExecContext(
		ctx,
		`
		INSERT INTO storage_cleanups (virtual_storage, storage) VALUES ($1, $2)
		ON CONFLICT (virtual_storage, storage) DO NOTHING`,
		virtualStorage, storage,
	); err != nil {
		return fmt.Errorf("exec: %w", err)
	}
	return nil
}

// AcquireNextStorage picks up the next storage for processing.
// Once acquired no other call to the same method will return the same storage, so it
// works as exclusive lock on that entry.
// Once processing is done the returned function needs to be called to release
// acquired storage. It updates last_run column of the entry on execution.
func (ss *StorageCleanup) AcquireNextStorage(ctx context.Context, inactive, updatePeriod time.Duration) (*ClusterPath, func() error, error) {
	var entry ClusterPath
	if err := ss.db.QueryRowContext(
		ctx,
		`UPDATE storage_cleanups
			SET triggered_at = NOW()
			WHERE (virtual_storage, storage) IN (
				SELECT virtual_storage, storage
				FROM storage_cleanups
				WHERE
					COALESCE(last_run, TO_TIMESTAMP(0)) <= (NOW() - INTERVAL '1 MILLISECOND' * $1)
					AND COALESCE(triggered_at, TO_TIMESTAMP(0)) <= (NOW() - INTERVAL '1 MILLISECOND' * $2)
				ORDER BY last_run NULLS FIRST, virtual_storage, storage
				LIMIT 1
				FOR UPDATE SKIP LOCKED
			)
			RETURNING virtual_storage, storage`,
		inactive.Milliseconds(), updatePeriod.Milliseconds(),
	).Scan(&entry.VirtualStorage, &entry.Storage); err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			return nil, nil, fmt.Errorf("scan: %w", err)
		}
		return nil, func() error { return nil }, nil
	}

	stop := make(chan struct{}, 1)
	stopped := make(chan struct{})
	go func() {
		trigger := helper.NewTimerTicker(updatePeriod - 100*time.Millisecond)
		defer func() {
			trigger.Stop()
			close(stopped)
		}()

		for {
			trigger.Reset()
			select {
			case <-ctx.Done():
				return
			case <-stop:
				return
			case <-trigger.C():
				if _, err := ss.db.ExecContext(
					ctx,
					`UPDATE storage_cleanups
					SET triggered_at = NOW()
					WHERE virtual_storage = $1 AND storage = $2`,
					entry.VirtualStorage, entry.Storage,
				); err != nil {
					return
				}
			}
		}
	}()

	return &entry, func() error {
		// signals health update goroutine to terminate
		stop <- struct{}{}
		// waits for the health update goroutine to terminate to prevent update
		// of the triggered_at after setting it to NULL
		<-stopped

		if _, err := ss.db.ExecContext(
			ctx,
			`UPDATE storage_cleanups
			SET last_run = NOW(), triggered_at = NULL
			WHERE virtual_storage = $1 AND storage = $2`,
			entry.VirtualStorage, entry.Storage,
		); err != nil {
			return fmt.Errorf("update storage_cleanups: %w", err)
		}
		return nil
	}, nil
}

// DoesntExist returns replica path for each repository that doesn't exist in the database
// by querying repositories and storage_repositories tables.
func (ss *StorageCleanup) DoesntExist(ctx context.Context, virtualStorage, storage string, replicaPaths []string) ([]string, error) {
	if len(replicaPaths) == 0 {
		return nil, nil
	}

	rows, err := ss.db.QueryContext(
		ctx,
		`SELECT UNNEST($3::TEXT[]) AS replica_path
		EXCEPT (
			SELECT replica_path
			FROM repositories
			JOIN storage_repositories USING (virtual_storage, relative_path)
			WHERE virtual_storage = $1 AND storage = $2 AND replica_path = ANY($3)
		)`,
		virtualStorage, storage, pq.StringArray(replicaPaths),
	)
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var res []string
	for rows.Next() {
		var curr string
		if err := rows.Scan(&curr); err != nil {
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
