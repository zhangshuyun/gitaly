package remote

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"gitlab.com/gitlab-org/gitaly/v14/client"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/remoterepo"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/service/ref"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	mirrorRefSpec = "+refs/*:refs/*"
)

type fetchFailedError struct {
	stderr string
	err    error
}

func (e fetchFailedError) Error() string {
	if e.stderr != "" {
		return fmt.Sprintf("FetchInternalRemote: fetch: %v, stderr: %q", e.err, e.stderr)
	}

	return fmt.Sprintf("FetchInternalRemote: fetch: %v", e.err)
}

// FetchInternalRemote fetches another Gitaly repository set as a remote
func FetchInternalRemote(
	ctx context.Context,
	cfg config.Cfg,
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
			return fetchFailedError{stderr.String(), err}
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
		if err := ref.SetDefaultBranchRef(ctx, repo, remoteDefaultBranch.String(), cfg); err != nil {
			return helper.ErrInternalf("setting default branch: %w", err)
		}
	}

	return nil
}

// FetchInternalRemote fetches another Gitaly repository set as a remote
func (s *server) FetchInternalRemote(ctx context.Context, req *gitalypb.FetchInternalRemoteRequest) (*gitalypb.FetchInternalRemoteResponse, error) {
	if err := validateFetchInternalRemoteRequest(req); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "FetchInternalRemote: %v", err)
	}

	repo := s.localrepo(req.GetRepository())

	if err := FetchInternalRemote(ctx, s.cfg, s.conns, repo, req.RemoteRepository); err != nil {
		var fetchErr fetchFailedError

		if errors.As(err, &fetchErr) {
			// Design quirk: if the fetch fails, this RPC returns Result: false, but no error.
			ctxlogrus.Extract(ctx).WithError(fetchErr.err).WithField("stderr", fetchErr.stderr).Warn("git fetch failed")
			return &gitalypb.FetchInternalRemoteResponse{Result: false}, nil
		}

		return nil, err
	}

	return &gitalypb.FetchInternalRemoteResponse{Result: true}, nil
}

func validateFetchInternalRemoteRequest(req *gitalypb.FetchInternalRemoteRequest) error {
	if req.GetRepository() == nil {
		return fmt.Errorf("empty Repository")
	}

	if req.GetRemoteRepository() == nil {
		return fmt.Errorf("empty Remote Repository")
	}

	return nil
}
