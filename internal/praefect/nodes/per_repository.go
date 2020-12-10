package nodes

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/lib/pq"
	"github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/datastore/glsql"
)

// ErrNoPrimary is returned if the repository does not have a primary.
var ErrNoPrimary = errors.New("no primary")

// PerRepositoryElector implements an elector that selects a primary for each repository.
type PerRepositoryElector struct {
	log logrus.FieldLogger
	db  glsql.Querier
	hc  HealthChecker
}

// HealthChecker maintains node health statuses.
type HealthChecker interface {
	// HealthyNodes returns lists of healthy nodes by virtual storages.
	HealthyNodes() map[string][]string
}

// NewPerRepositoryElector returns a new per repository primary elector.
func NewPerRepositoryElector(log logrus.FieldLogger, db glsql.Querier, hc HealthChecker) *PerRepositoryElector {
	log = log.WithField("component", "PerRepositoryElector")
	return &PerRepositoryElector{
		log: log,
		db:  db,
		hc:  hc,
	}
}

// GetPrimary returns the primary storage of a repository. If the current primary is unhealthy, GetPrimary attempts
// to elect a new primary node. Candidates are all healthy storages which are assigned to host the repository. From
// the assigned nodes, the most up to date storages are preferred. If there are equally suitable candidates, a random
// one is picked.
func (pr *PerRepositoryElector) GetPrimary(ctx context.Context, virtualStorage, relativePath string) (string, error) {
	var current, previous sql.NullString
	if err := pr.db.QueryRowContext(ctx, `
With new AS (
	UPDATE repositories
		SET "primary" = (
			SELECT storage
			FROM ( SELECT unnest($3::text[]) AS storage ) AS healthy_storages
			LEFT JOIN storage_repositories USING (storage)
			WHERE virtual_storage = repositories.virtual_storage
			AND relative_path = repositories.relative_path
			AND (
				SELECT COUNT(*) = 0 OR COUNT(*) FILTER (WHERE storage = storage_repositories.storage) = 1
				FROM repository_assignments
				WHERE repository_assignments.virtual_storage = storage_repositories.virtual_storage
				AND repository_assignments.relative_path = storage_repositories.relative_path
			)
			ORDER BY generation DESC NULLS LAST, random()
			LIMIT 1
		)
	WHERE virtual_storage = $1
	AND   relative_path   = $2
	AND   ("primary" IS NULL OR "primary" != ANY($3::text[]))
	RETURNING true AS elected, "primary"
)

SELECT
	CASE WHEN new.elected
		THEN new.primary
		ELSE old.primary
	END,
	old.primary
FROM repositories AS old
CROSS JOIN new
WHERE virtual_storage = $1
AND relative_path = $2
		`,
		virtualStorage,
		relativePath,
		pq.StringArray(pr.hc.HealthyNodes()[virtualStorage]),
	).Scan(&current, &previous); err != nil {
		return "", fmt.Errorf("scan: %w", err)
	}

	if current != previous {
		pr.log.WithFields(logrus.Fields{
			"virtual_storage":  virtualStorage,
			"relative_path":    relativePath,
			"current_primary":  current.String,
			"previous_primary": previous.String,
		}).Info("primary node changed")
	}

	if !current.Valid {
		return "", ErrNoPrimary
	}

	return current.String, nil
}
