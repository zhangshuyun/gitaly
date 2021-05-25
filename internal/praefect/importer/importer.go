package importer

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/lib/pq"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/datastore/glsql"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/nodes"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

const batchSize = 25

// Importer creates database records for repositories which are missing from the database
// but are present on the virtual storage's primary's disk.
type Importer struct {
	nm              nodes.Manager
	virtualStorages []string
	db              glsql.Querier
}

// New creates a new Importer.
func New(nm nodes.Manager, virtualStorages []string, db glsql.Querier) *Importer {
	return &Importer{
		nm:              nm,
		virtualStorages: virtualStorages,
		db:              db,
	}
}

// Result is a partial result of the import. VirtualStorage is set in each Result,
// along with either Error or RelativePaths.
type Result struct {
	// Error is set if the import was aborted by an error.
	Error error
	// VirtualStorage indicates which virtual storage this result relates to.
	VirtualStorage string
	// RelativePaths includes the relative paths of repositories successfully imported
	// in a batch.
	RelativePaths []string
}

// Run walks the repositories on primary nodes of each virtual storage and creates database records for every
// repository on the primary's disk that is missing from the database. Run only performs the import for virtual
// storages that have not had the import successfully completed before. The returned channel must be consumed in
// order to release the goroutines created by Run.
func (imp *Importer) Run(ctx context.Context) <-chan Result {
	var wg sync.WaitGroup
	wg.Add(len(imp.virtualStorages))

	output := make(chan Result)
	for _, virtualStorage := range imp.virtualStorages {
		go func(virtualStorage string) {
			defer wg.Done()
			if err := imp.importVirtualStorage(ctx, virtualStorage, output); err != nil {
				output <- Result{
					VirtualStorage: virtualStorage,
					Error:          fmt.Errorf("importing virtual storage: %w", err),
				}
			}
		}(virtualStorage)
	}

	go func() {
		wg.Wait()
		close(output)
	}()

	return output
}

// importVirtualStorage walks the virtual storage's primary's disk and creates database records for any repositories
// which are missing from the primary.
func (imp *Importer) importVirtualStorage(ctx context.Context, virtualStorage string, output chan<- Result) error {
	if migrated, err := imp.isAlreadyCompleted(ctx, virtualStorage); err != nil {
		return fmt.Errorf("check if already completed: %w", err)
	} else if migrated {
		return nil
	}

	shard, err := imp.nm.GetShard(ctx, virtualStorage)
	if err != nil {
		return fmt.Errorf("get shard: %w", err)
	}

	client := gitalypb.NewInternalGitalyClient(shard.Primary.GetConnection())
	stream, err := client.WalkRepos(ctx, &gitalypb.WalkReposRequest{StorageName: shard.Primary.GetStorage()})
	if err != nil {
		return fmt.Errorf("open stream: %w", err)
	}

	relativePaths := make([]string, 0, batchSize)
	for {
		// The importer sleeps here for a short duration as a crude way to rate limit
		// to reduce the pressure on the available resources.
		// 100 milliseconds gives us maximum rate of 250 imported repositories per second in
		// 10 database calls.
		time.Sleep(100 * time.Millisecond)

		resp, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}

			return fmt.Errorf("receive: %w", err)
		}

		relativePaths = append(relativePaths, resp.RelativePath)
		if len(relativePaths) == batchSize {
			if err := imp.storeBatch(ctx, virtualStorage, shard.Primary.GetStorage(), relativePaths, output); err != nil {
				return fmt.Errorf("store batch: %w", err)
			}

			relativePaths = relativePaths[:0]
		}
	}

	// store the final batch after finishing walking repositories
	if len(relativePaths) > 0 {
		if err := imp.storeBatch(ctx, virtualStorage, shard.Primary.GetStorage(), relativePaths, output); err != nil {
			return fmt.Errorf("store final batch: %w", err)
		}
	}

	if err := imp.markCompleted(ctx, virtualStorage); err != nil {
		return fmt.Errorf("mark completed: %w", err)
	}

	return nil
}

// isAlreadyCompleted checks if the import has already been run successfully to finish. If so,
// the import is skipped.
func (imp *Importer) isAlreadyCompleted(ctx context.Context, virtualStorage string) (bool, error) {
	var alreadyMigrated bool
	if err := imp.db.QueryRowContext(ctx, `
SELECT repositories_imported
FROM virtual_storages
WHERE virtual_storage = $1
	`, virtualStorage).Scan(&alreadyMigrated); err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			return false, fmt.Errorf("scan: %w", err)
		}

		return false, nil
	}

	return alreadyMigrated, nil
}

// markCompleted marks the virtual storage's repository import as completed so it won't be attempted
// again after successful completion.
func (imp *Importer) markCompleted(ctx context.Context, virtualStorage string) error {
	_, err := imp.db.ExecContext(ctx, `
INSERT INTO virtual_storages (virtual_storage, repositories_imported)
VALUES ($1, true)
ON CONFLICT (virtual_storage)
	DO UPDATE SET repositories_imported = true
	`, virtualStorage)
	return err
}

// storeBatch stores a batch of relative paths found on the primary in to the database. Records are only added
// if there is no existing record of the database in the `repositories` table.
func (imp *Importer) storeBatch(ctx context.Context, virtualStorage, primary string, relativePaths []string, output chan<- Result) error {
	rows, err := imp.db.QueryContext(ctx, `
WITH imported_repositories AS (
	INSERT INTO repositories (virtual_storage, relative_path, generation)
	SELECT $1 AS virtual_storage, unnest($2::text[]) AS relative_path, 0 AS generation
	ON CONFLICT DO NOTHING
	RETURNING virtual_storage, relative_path, generation
), primary_records AS (
	INSERT INTO storage_repositories (virtual_storage, relative_path, storage, generation)
	SELECT virtual_storage, relative_path, $3 AS storage, generation
	FROM imported_repositories
	ON CONFLICT DO NOTHING
	RETURNING relative_path
)

SELECT relative_path
FROM primary_records`, virtualStorage, pq.StringArray(relativePaths), primary)
	if err != nil {
		return fmt.Errorf("query: %w", err)
	}
	defer rows.Close()

	imported := make([]string, 0, len(relativePaths))
	for rows.Next() {
		var relativePath string
		if err := rows.Scan(&relativePath); err != nil {
			return fmt.Errorf("scan: %w", err)
		}

		imported = append(imported, relativePath)
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterating rows: %w", err)
	}

	if len(imported) > 0 {
		output <- Result{
			VirtualStorage: virtualStorage,
			RelativePaths:  imported,
		}
	}

	return nil
}
