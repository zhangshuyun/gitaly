package git2go

import (
	"context"

	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
)

// RebaseCommand contains parameters to rebase a branch.
type RebaseCommand struct {
	// Repository is the path to execute rebase in.
	Repository string
	// Committer contains the the committer signature.
	Committer Signature
	// BranchName is the branch that is rebased.
	BranchName string
	// UpstreamRevision is the revision where the branch is rebased onto.
	UpstreamRevision string
}

// Run performs the rebase via gitaly-git2go
func (r RebaseCommand) Run(ctx context.Context, cfg config.Cfg) (git.ObjectID, error) {
	return runWithGob(ctx, cfg, "rebase", r)
}
