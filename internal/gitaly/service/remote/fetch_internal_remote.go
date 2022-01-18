package remote

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"gitlab.com/gitlab-org/gitaly/v14/client"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/remoterepo"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

const (
	mirrorRefSpec = "+refs/*:refs/*"
)

// FetchInternalRemote fetches another Gitaly repository set as a remote
func FetchInternalRemote(
	ctx context.Context,
	conns *client.Pool,
	repo *localrepo.Repo,
	remoteRepoProto *gitalypb.Repository,
) error {
	var stderr bytes.Buffer
	if err := repo.FetchInternal(
		ctx,
		remoteRepoProto,
		[]string{mirrorRefSpec},
		localrepo.FetchOpts{Prune: true, Stderr: &stderr},
	); err != nil {
		if errors.As(err, &localrepo.ErrFetchFailed{}) {
			return fmt.Errorf("fetch: %w, stderr: %q", err, stderr.String())
		}

		return fmt.Errorf("fetch: %w", err)
	}

	remoteRepo, err := remoterepo.New(ctx, remoteRepoProto, conns)
	if err != nil {
		return helper.ErrInternal(err)
	}

	remoteDefaultBranch, err := remoteRepo.GetDefaultBranch(ctx)
	if err != nil {
		return helper.ErrInternalf("getting remote default branch: %w", err)
	}

	defaultBranch, err := repo.GetDefaultBranch(ctx)
	if err != nil {
		return helper.ErrInternalf("getting local default branch: %w", err)
	}

	if defaultBranch != remoteDefaultBranch {
		if err := repo.SetDefaultBranch(ctx, remoteDefaultBranch); err != nil {
			return helper.ErrInternalf("setting default branch: %w", err)
		}
	}

	return nil
}
