package remote

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/service/ref"
	"gitlab.com/gitlab-org/gitaly/internal/gitalyssh"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	mirrorRefSpec = "+refs/*:refs/*"
)

// FetchInternalRemote fetches another Gitaly repository set as a remote
func (s *server) FetchInternalRemote(ctx context.Context, req *gitalypb.FetchInternalRemoteRequest) (*gitalypb.FetchInternalRemoteResponse, error) {
	if err := validateFetchInternalRemoteRequest(req); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "FetchInternalRemote: %v", err)
	}

	gitVersion, err := git.CurrentVersion(ctx, s.gitCmdFactory)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "cannot determine current git version: %v", err)
	}

	env, err := gitalyssh.UploadPackEnv(ctx, s.cfg, &gitalypb.SSHUploadPackRequest{Repository: req.RemoteRepository})
	if err != nil {
		return nil, fmt.Errorf("upload pack environment: %w", err)
	}

	stderr := &bytes.Buffer{}

	flags := []git.Option{git.Flag{Name: "--prune"}}
	options := []git.CmdOpt{
		git.WithEnv(env...),
		git.WithStderr(stderr),
	}

	if gitVersion.SupportsAtomicFetches() && featureflag.IsEnabled(ctx, featureflag.AtomicFetch) {
		flags = append(flags, git.Flag{
			Name: "--atomic",
		})
		options = append(options, git.WithRefTxHook(ctx, req.Repository, s.cfg))
	} else {
		options = append(options, git.WithDisabledHooks())
	}

	cmd, err := s.gitCmdFactory.New(ctx, req.Repository,
		git.SubCmd{
			Name:  "fetch",
			Flags: flags,
			Args:  []string{gitalyssh.GitalyInternalURL, mirrorRefSpec},
		},
		options...,
	)
	if err != nil {
		return nil, fmt.Errorf("create git fetch: %w", err)
	}
	if err := cmd.Wait(); err != nil {
		// Design quirk: if the fetch fails, this RPC returns Result: false, but no error.
		ctxlogrus.Extract(ctx).WithError(err).WithField("stderr", stderr.String()).Warn("git fetch failed")
		return &gitalypb.FetchInternalRemoteResponse{Result: false}, nil
	}

	remoteDefaultBranch, err := s.getRemoteDefaultBranch(ctx, req.RemoteRepository)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "FetchInternalRemote: remote default branch: %v", err)
	}

	defaultBranch, err := ref.DefaultBranchName(ctx, s.gitCmdFactory, req.Repository)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "FetchInternalRemote: default branch: %v", err)
	}

	if !bytes.Equal(defaultBranch, remoteDefaultBranch) {
		if err := ref.SetDefaultBranchRef(ctx, s.gitCmdFactory, req.Repository, string(remoteDefaultBranch), s.cfg); err != nil {
			return nil, status.Errorf(codes.Internal, "FetchInternalRemote: set default branch: %v", err)
		}
	}

	return &gitalypb.FetchInternalRemoteResponse{Result: true}, nil
}

// getRemoteDefaultBranch gets the default branch of a repository hosted on another Gitaly node
func (s *server) getRemoteDefaultBranch(ctx context.Context, repo *gitalypb.Repository) ([]byte, error) {
	serverInfo, err := helper.ExtractGitalyServer(ctx, repo.StorageName)
	if err != nil {
		return nil, fmt.Errorf("getRemoteDefaultBranch: %w", err)
	}

	if serverInfo.Address == "" {
		return nil, errors.New("getRemoteDefaultBranch: empty Gitaly address")
	}

	conn, err := s.conns.Dial(ctx, serverInfo.Address, serverInfo.Token)
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
