package remote

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"gitlab.com/gitlab-org/gitaly/v13/internal/git"
	"gitlab.com/gitlab-org/gitaly/v13/internal/gitaly/service/ref"
	"gitlab.com/gitlab-org/gitaly/v13/internal/gitalyssh"
	"gitlab.com/gitlab-org/gitaly/v13/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v13/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	gitalyInternalURL = "ssh://gitaly/internal.git"
	mirrorRefSpec     = "+refs/*:refs/*"
)

// FetchInternalRemote fetches another Gitaly repository set as a remote
func (s *server) FetchInternalRemote(ctx context.Context, req *gitalypb.FetchInternalRemoteRequest) (*gitalypb.FetchInternalRemoteResponse, error) {
	if err := validateFetchInternalRemoteRequest(req); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "FetchInternalRemote: %v", err)
	}

	env, err := gitalyssh.UploadPackEnv(ctx, &gitalypb.SSHUploadPackRequest{Repository: req.RemoteRepository})
	if err != nil {
		return nil, err
	}

	repoPath, err := s.locator.GetRepoPath(req.Repository)
	if err != nil {
		return nil, err
	}

	cmd, err := git.SafeBareCmd(ctx, git.CmdStream{}, env,
		[]git.Option{git.ValueFlag{"--git-dir", repoPath}},
		git.SubCmd{
			Name:  "fetch",
			Flags: []git.Option{git.Flag{Name: "--prune"}},
			Args:  []string{gitalyInternalURL, mirrorRefSpec},
		},
	)
	if err != nil {
		return nil, err
	}
	if err := cmd.Wait(); err != nil {
		// Design quirk: if the fetch fails, this RPC returns Result: false, but no error.
		ctxlogrus.Extract(ctx).WithError(err).Warn("git fetch failed")
		return &gitalypb.FetchInternalRemoteResponse{Result: false}, nil
	}

	remoteDefaultBranch, err := s.getRemoteDefaultBranch(ctx, req.RemoteRepository)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "FetchInternalRemote: remote default branch: %v", err)
	}

	defaultBranch, err := ref.DefaultBranchName(ctx, req.Repository)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "FetchInternalRemote: default branch: %v", err)
	}

	if !bytes.Equal(defaultBranch, remoteDefaultBranch) {
		if err := ref.SetDefaultBranchRef(ctx, req.Repository, string(remoteDefaultBranch)); err != nil {
			return nil, status.Errorf(codes.Internal, "FetchInternalRemote: set default branch: %v", err)
		}
	}

	return &gitalypb.FetchInternalRemoteResponse{Result: true}, nil
}

// getRemoteDefaultBranch gets the default branch of a repository hosted on another Gitaly node
func (s *server) getRemoteDefaultBranch(ctx context.Context, repo *gitalypb.Repository) ([]byte, error) {
	server, err := helper.ExtractGitalyServer(ctx, repo.StorageName)
	if err != nil {
		return nil, fmt.Errorf("getRemoteDefaultBranch: %w", err)
	}

	address := server["address"]
	if address == "" {
		return nil, errors.New("getRemoteDefaultBranch: empty Gitaly address")
	}
	token := server["token"]

	conn, err := s.conns.Dial(ctx, address, token)
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
