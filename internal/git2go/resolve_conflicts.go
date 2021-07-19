package git2go

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"

	"gitlab.com/gitlab-org/gitaly/v14/internal/git/conflict"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/repository"
)

// ResolveCommand contains arguments to perform a merge commit and resolve any
// conflicts produced from that merge commit
type ResolveCommand struct {
	MergeCommand
	Resolutions []conflict.Resolution
}

// ResolveResult returns information about the successful merge and resolution
type ResolveResult struct {
	MergeResult
}

// Resolve will attempt merging and resolving conflicts for the provided request
func (b Executor) Resolve(ctx context.Context, repo repository.GitRepo, r ResolveCommand) (ResolveResult, error) {
	if err := r.verify(); err != nil {
		return ResolveResult{}, fmt.Errorf("resolve: %w: %s", ErrInvalidArgument, err.Error())
	}

	input := &bytes.Buffer{}
	if err := gob.NewEncoder(input).Encode(r); err != nil {
		return ResolveResult{}, fmt.Errorf("resolve: %w", err)
	}

	stdout, err := b.run(ctx, repo, input, "resolve")
	if err != nil {
		return ResolveResult{}, err
	}

	var response ResolveResult
	if err := gob.NewDecoder(stdout).Decode(&response); err != nil {
		return ResolveResult{}, fmt.Errorf("resolve: %w", err)
	}

	return response, nil
}
