package nodes

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/commonerr"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/datastore/glsql"
)

// ErrNoPrimary is returned if the repository does not have a primary.
var ErrNoPrimary = errors.New("no primary")

// PerRepositoryElector implements an elector that selects a primary for each repository.
// It elects a healthy node with most recent generation as the primary. If all nodes are
// on the same generation, it picks one randomly to balance repositories in simple fashion.
type PerRepositoryElector struct {
	log         logrus.FieldLogger
	db          glsql.Querier
	handleError func(error) error
	retryWait   time.Duration
}

// NewPerRepositoryElector returns a new per repository primary elector.
func NewPerRepositoryElector(log logrus.FieldLogger, db glsql.Querier) *PerRepositoryElector {
	log = log.WithField("component", "PerRepositoryElector")
	return &PerRepositoryElector{
		log: log,
		db:  db,
		handleError: func(err error) error {
			log.WithError(err).Error("failed performing failovers")
			return nil
		},
		retryWait: time.Second,
	}
}

// primaryChanges is a type for collecting promotion and demotion counts. It's keyed by
// virtual storage -> storage -> (promoted | demoted).
type primaryChanges map[string]map[string]map[string]int

// Run listens on the trigger channel for updates. On each update, it tries to elect new primaries for
// repositories which have an unhealthy primary. Blocks until the context is canceled or the trigger
// channel is closed. Returns the error from the context.
func (pr *PerRepositoryElector) Run(ctx context.Context, trigger <-chan struct{}) error {
	pr.log.Info("per repository elector started")
	defer pr.log.Info("per repository elector stopped")

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case _, ok := <-trigger:
			if !ok {
				return nil
			}

			for {
				if err := pr.performFailovers(ctx); err != nil {
					if err := pr.handleError(err); err != nil {
						return err
					}

					// Reattempt the failovers after one second if it failed. The trigger channel only ticks
					// when a health change has occurred. If we fail to perform failovers, we would
					// only try again when the health of a node has changed. This would leave some
					// repositories without a healthy primary. Ideally we'd fix this by getting rid of
					// the virtual storage wide failovers and perform failovers lazily for repositories
					// when necessary: https://gitlab.com/gitlab-org/gitaly/-/issues/3207
					select {
					case <-ctx.Done():
						return ctx.Err()
					case <-time.After(pr.retryWait):
						continue
					}
				}

				break
			}
		}
	}
}

func (pr *PerRepositoryElector) performFailovers(ctx context.Context) error {
	rows, err := pr.db.QueryContext(ctx, `
WITH updated AS (
	UPDATE repositories
		SET "primary" = (
			SELECT storage
			FROM valid_primaries
			WHERE virtual_storage = repositories.virtual_storage
			AND   relative_path   = repositories.relative_path
			ORDER BY random()
			LIMIT 1
		)
	WHERE NOT EXISTS (
		SELECT FROM valid_primaries
		WHERE virtual_storage = repositories.virtual_storage
		AND   relative_path   = repositories.relative_path
		AND   storage         = repositories."primary"
	)
	RETURNING virtual_storage, relative_path, "primary"
),

demoted AS (
	SELECT virtual_storage, repositories."primary" AS storage, COUNT(*) AS demoted
	FROM repositories
	JOIN updated USING (virtual_storage, relative_path)
	WHERE repositories."primary" IS NOT NULL
	GROUP BY virtual_storage, repositories."primary"
),

promoted AS (
	SELECT virtual_storage, "primary" AS storage, COUNT(*) AS promoted
	FROM updated
	WHERE updated."primary" IS NOT NULL
	GROUP BY virtual_storage, "primary"
)

SELECT virtual_storage, storage, COALESCE(demoted, 0), COALESCE(promoted, 0)
FROM demoted
FULL JOIN promoted USING (virtual_storage, storage)
	`)
	if err != nil {
		return fmt.Errorf("query: %w", err)
	}
	defer rows.Close()

	changes := primaryChanges{}
	for rows.Next() {
		var virtualStorage, storage string
		var demoted, promoted int

		if err := rows.Scan(&virtualStorage, &storage, &demoted, &promoted); err != nil {
			return fmt.Errorf("scan: %w", err)
		}

		storageChanges, ok := changes[virtualStorage]
		if !ok {
			storageChanges = map[string]map[string]int{}
		}

		storageChanges[storage] = map[string]int{"demoted": demoted, "promoted": promoted}
		changes[virtualStorage] = storageChanges
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("rows: %w", err)
	}

	if len(changes) > 0 {
		pr.log.WithField("changes", changes).Info("performed failovers")
	} else {
		pr.log.Info("attempting failovers resulted no changes")
	}

	return nil
}

// GetPrimary returns the primary storage of a repository.
func (pr *PerRepositoryElector) GetPrimary(ctx context.Context, virtualStorage, relativePath string) (string, error) {
	var primary sql.NullString
	if err := pr.db.QueryRowContext(ctx, `
SELECT "primary"
FROM repositories
WHERE virtual_storage = $1
AND relative_path = $2
		`,
		virtualStorage,
		relativePath,
	).Scan(&primary); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", commonerr.NewRepositoryNotFoundError(virtualStorage, relativePath)
		}

		return "", fmt.Errorf("scan: %w", err)
	}

	if !primary.Valid {
		return "", ErrNoPrimary
	}

	return primary.String, nil
}
