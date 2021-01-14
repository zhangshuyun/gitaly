package repository

import (
	"io"

	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/streamio"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *server) CreateBundle(req *gitalypb.CreateBundleRequest, stream gitalypb.RepositoryService_CreateBundleServer) error {
	repo := req.GetRepository()
	if repo == nil {
		return status.Errorf(codes.InvalidArgument, "CreateBundle: empty Repository")
	}

	ctx := stream.Context()

	if _, err := s.Cleanup(ctx, &gitalypb.CleanupRequest{Repository: req.GetRepository()}); err != nil {
		return helper.ErrInternalf("running Cleanup on repository: %w", err)
	}

	cmd, err := git.NewCommand(ctx, repo, nil, git.SubSubCmd{
		Name:   "bundle",
		Action: "create",
		Flags:  []git.Option{git.OutputToStdout, git.Flag{Name: "--all"}},
	})
	if err != nil {
		return status.Errorf(codes.Internal, "CreateBundle: cmd start failed: %v", err)
	}

	writer := streamio.NewWriter(func(p []byte) error {
		return stream.Send(&gitalypb.CreateBundleResponse{Data: p})
	})

	_, err = io.Copy(writer, cmd)
	if err != nil {
		return status.Errorf(codes.Internal, "CreateBundle: stream writer failed: %v", err)
	}

	if err := cmd.Wait(); err != nil {
		return status.Errorf(codes.Internal, "CreateBundle: cmd wait failed: %v", err)
	}

	return nil
}
