package git2go

import (
	"context"
	"time"

	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/repository"
)

// CherryPickCommand contains parameters to perform a cherry pick.
type CherryPickCommand struct {
	// Repository is the path where to execute the cherry pick.
	Repository string
	// CommitterName is the committer name for the resulting commit.
	CommitterName string
	// CommitterMail is the committer mail for the resulting commit.
	CommitterMail string
	// CommitterDate is the committer date of revert commit.
	CommitterDate time.Time
	// Message is the message to be used for the resulting commit.
	Message string
	// Ours is the commit that the revert is applied to.
	Ours string
	// Commit is the commit that is to be picked.
	Commit string
	// Mainline is the parent to be considered the mainline
	Mainline uint
}

// CherryPick performs a cherry pick via gitaly-git2go.
func (b Executor) CherryPick(ctx context.Context, repo repository.GitRepo, m CherryPickCommand) (git.ObjectID, error) {
	return b.runWithGob(ctx, repo, "cherry-pick", m)
}
