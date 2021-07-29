package localrepo

import (
	"bytes"
	"context"
	"fmt"

	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitalyssh"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

// FetchError records an error during a `git fetch`
type FetchError struct {
	source error
	stderr []byte
}

func (e FetchError) Error() string {
	if len(e.stderr) == 0 {
		return fmt.Sprintf("fetch: %s", e.source.Error())
	}
	return fmt.Sprintf("fetch: %s, stderr: %q", e.source.Error(), e.stderr)
}

// Unwrap satisfies `errors.Unwrap` in order to determine the source of the failure
func (e FetchError) Unwrap() error {
	return e.source
}

// FetchInternalObject fetches a remote object from gitaly internal SSH
func (repo *Repo) FetchInternalObject(ctx context.Context, remoteRepo *gitalypb.Repository, oid git.ObjectID, opts FetchOpts) error {
	env, err := gitalyssh.UploadPackEnv(ctx, repo.cfg, &gitalypb.SSHUploadPackRequest{
		Repository:       remoteRepo,
		GitConfigOptions: []string{"uploadpack.allowAnySHA1InWant=true"},
	})
	if err != nil {
		return fmt.Errorf("fetch internal object: upload pack env: %w", err)
	}

	env = append(env, opts.Env...)

	var stderr bytes.Buffer
	if opts.Stderr == nil {
		opts.Stderr = &stderr
	}

	commandOptions := []git.CmdOpt{
		git.WithEnv(env...),
		git.WithStderr(opts.Stderr),
		git.WithRefTxHook(ctx, repo, repo.cfg),
	}
	commandOptions = append(commandOptions, opts.CommandOptions...)

	cmd, err := repo.Exec(ctx, git.SubCmd{
		Name:  "fetch",
		Flags: opts.buildFlags(),
		Args:  []string{gitalyssh.GitalyInternalURL, oid.String()},
	},
		commandOptions...,
	)
	if err != nil {
		return fmt.Errorf("fetch internal object: %w", err)
	}

	if err := cmd.Wait(); err != nil {
		return FetchError{
			source: err,
			stderr: stderr.Bytes(),
		}
	}

	return nil
}
