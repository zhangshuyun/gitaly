package git2go

import (
	"context"

	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
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

// Rebase performs the rebase via gitaly-git2go
func (b Executor) Rebase(ctx context.Context, r RebaseCommand) (git.ObjectID, error) {
	return b.runWithGob(ctx, "rebase", r)
}
