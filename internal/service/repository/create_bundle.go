package repository

import (
	"io"

	"gitlab.com/gitlab-org/gitaly-proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/internal/git"
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

	cmd, err := git.Command(ctx, repo, "bundle", "create", "-", "--all")
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
