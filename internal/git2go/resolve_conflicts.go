package git2go

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"

	"gitlab.com/gitlab-org/gitaly/v14/internal/git/conflict"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
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

// Run will attempt merging and resolving conflicts for the provided request
func (r ResolveCommand) Run(ctx context.Context, cfg config.Cfg) (ResolveResult, error) {
	if err := r.verify(); err != nil {
		return ResolveResult{}, fmt.Errorf("resolve: %w: %s", ErrInvalidArgument, err.Error())
	}

	input := &bytes.Buffer{}
	if err := gob.NewEncoder(input).Encode(r); err != nil {
		return ResolveResult{}, fmt.Errorf("resolve: %w", err)
	}

	stdout, err := run(ctx, BinaryPath(cfg.BinDir), input, "resolve")
	if err != nil {
		return ResolveResult{}, err
	}

	var response ResolveResult
	if err := gob.NewDecoder(stdout).Decode(&response); err != nil {
		return ResolveResult{}, fmt.Errorf("resolve: %w", err)
	}

	return response, nil
}
