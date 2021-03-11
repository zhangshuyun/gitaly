package git2go

import (
	"context"
	"time"

	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
)

// CherryPickCommand contains parameters to perform a cherry pick.
type CherryPickCommand struct {
	// Repository is the path where to execute the cherry pick.
	Repository string
	// AuthorName is the author name for the resulting commit.
	AuthorName string
	// AuthorMail is the author mail for the resulting commit.
	AuthorMail string
	// AuthorDate is the author date of revert commit.
	AuthorDate time.Time
	// Message is the message to be used for the resulting commit.
	Message string
	// Ours is the commit that the revert is applied to.
	Ours string
	// Commit is the commit that is to be picked.
	Commit string
	// Mainline is the parent to be considered the mainline
	Mainline uint
}

// Run performs a cherry pick via gitaly-git2go.
func (m CherryPickCommand) Run(ctx context.Context, cfg config.Cfg) (git.ObjectID, error) {
	return runWithGob(ctx, cfg, "cherry-pick", m)
}
