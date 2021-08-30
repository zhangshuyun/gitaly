package remote

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"gitlab.com/gitlab-org/gitaly/v14/client"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/localrepo"
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
	remoteRepo *gitalypb.Repository,
) error {
	var stderr bytes.Buffer
	if err := repo.FetchInternal(
		ctx,
		remoteRepo,
		[]string{mirrorRefSpec},
		localrepo.FetchOpts{Prune: true, Stderr: &stderr},
	); err != nil {
		if errors.As(err, &localrepo.ErrFetchFailed{}) {
			return fetchFailedError{stderr.String(), err}
		}

		return fmt.Errorf("create git fetch: %w", err)
	}

	remoteDefaultBranch, err := getRemoteDefaultBranch(ctx, remoteRepo, conns)
	if err != nil {
		return status.Errorf(codes.Internal, "FetchInternalRemote: remote default branch: %v", err)
	}

	defaultBranch, err := repo.GetDefaultBranch(ctx, nil)
	if err != nil {
		return status.Errorf(codes.Internal, "FetchInternalRemote: default branch: %v", err)
	}

	if defaultBranch.Name.String() != string(remoteDefaultBranch) {
		if err := ref.SetDefaultBranchRef(ctx, repo, string(remoteDefaultBranch), cfg); err != nil {
			return status.Errorf(codes.Internal, "FetchInternalRemote: set default branch: %v", err)
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

// getRemoteDefaultBranch gets the default branch of a repository hosted on another Gitaly node.
func getRemoteDefaultBranch(ctx context.Context, repo *gitalypb.Repository, conns *client.Pool) ([]byte, error) {
	serverInfo, err := helper.ExtractGitalyServer(ctx, repo.StorageName)
	if err != nil {
		return nil, fmt.Errorf("getRemoteDefaultBranch: %w", err)
	}

	if serverInfo.Address == "" {
		return nil, errors.New("getRemoteDefaultBranch: empty Gitaly address")
	}

	conn, err := conns.Dial(ctx, serverInfo.Address, serverInfo.Token)
	if err != nil {
		return nil, fmt.Errorf("getRemoteDefaultBranch: %w", err)
	}

	cc := gitalypb.NewRefServiceClient(conn)
	response, err := cc.FindDefaultBranchName(ctx, &gitalypb.FindDefaultBranchNameRequest{
		Repository: repo,
	})
	if err != nil {
		return nil, fmt.Errorf("getRemoteDefaultBranch: %w", err)
	}

	return response.Name, nil
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
