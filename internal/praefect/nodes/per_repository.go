package nodes

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/commonerr"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/datastore/glsql"
)

// ErrNoPrimary is returned if the repository does not have a primary.
var ErrNoPrimary = errors.New("no primary")

// PerRepositoryElector implements an elector that selects a primary for each repository.
// It elects a healthy node with most recent generation as the primary. If all nodes are
// on the same generation, it picks one randomly to balance repositories in simple fashion.
type PerRepositoryElector struct{ db glsql.Querier }

// NewPerRepositoryElector returns a new per repository primary elector.
func NewPerRepositoryElector(db glsql.Querier) *PerRepositoryElector {
	return &PerRepositoryElector{db: db}
}

// GetPrimary returns the primary storage of a repository. If the primary is not a valid primary anymore, an election
// is attempted. If there are no valid primaries, the current primary is simply demoted.
func (pr *PerRepositoryElector) GetPrimary(ctx context.Context, repositoryID int64) (string, error) {
	var current, previous sql.NullString
	if err := pr.db.QueryRowContext(ctx, `
WITH new AS (
	UPDATE repositories
		SET "primary" = (
			SELECT storage
			FROM valid_primaries
			WHERE repository_id = $1
			ORDER BY random()
			LIMIT 1
		)
	WHERE repository_id = $1
	AND NOT EXISTS (
		SELECT FROM valid_primaries
		WHERE repository_id = $1
		AND   storage       = repositories."primary"
	)
	RETURNING true AS elected, "primary"
)

SELECT
	CASE WHEN new.elected
		THEN new.primary
		ELSE old.primary
	END,
	old.primary
FROM repositories AS old
FULL JOIN new ON true
WHERE repository_id = $1

		`,
		repositoryID,
	).Scan(&current, &previous); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", commonerr.ErrRepositoryNotFound
		}

		return "", fmt.Errorf("scan: %w", err)
	}

	if current != previous {
		ctxlogrus.Extract(ctx).WithFields(logrus.Fields{
			"repository_id":    repositoryID,
			"current_primary":  current.String,
			"previous_primary": previous.String,
		}).Info("primary node changed")
	}

	if !current.Valid {
		return "", ErrNoPrimary
	}

	return current.String, nil
}
