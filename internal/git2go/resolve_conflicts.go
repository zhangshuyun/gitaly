package git2go

import (
	"context"
	"fmt"

	"gitlab.com/gitlab-org/gitaly/internal/git/conflict"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
)

type ResolveCommand struct {
	MergeCommand `json:"merge_command"`
	Resolutions  []conflict.Resolution `json:"conflict_files"`
}

type ResolveResult struct {
	MergeResult `json:"merge_result"`
}

func ResolveCommandFromSerialized(serialized string) (ResolveCommand, error) {
	var request ResolveCommand
	if err := deserialize(serialized, &request); err != nil {
		return ResolveCommand{}, err
	}

	if err := request.verify(); err != nil {
		return ResolveCommand{}, fmt.Errorf("resolve: %w: %s", ErrInvalidArgument, err.Error())
	}

	return request, nil
}

func (r ResolveCommand) Run(ctx context.Context, cfg config.Cfg) (ResolveResult, error) {
	if err := r.verify(); err != nil {
		return ResolveResult{}, fmt.Errorf("resolve: %w: %s", ErrInvalidArgument, err.Error())
	}

	serialized, err := serialize(r)
	if err != nil {
		return ResolveResult{}, err
	}

	stdout, err := run(ctx, cfg, "resolve", serialized)
	if err != nil {
		return ResolveResult{}, err
	}

	var response ResolveResult
	if err := deserialize(stdout, &response); err != nil {
		return ResolveResult{}, err
	}

	return response, nil
}
