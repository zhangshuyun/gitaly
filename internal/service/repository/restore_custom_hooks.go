package repository

import (
	"os/exec"

	"gitlab.com/gitlab-org/gitaly/internal/command"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/streamio"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *server) RestoreCustomHooks(stream gitalypb.RepositoryService_RestoreCustomHooksServer) error {
	firstRequest, err := stream.Recv()
	if err != nil {
		return status.Errorf(codes.Internal, "RestoreCustomHooks: first request failed %v", err)
	}

	repo := firstRequest.GetRepository()
	if repo == nil {
		return status.Errorf(codes.InvalidArgument, "RestoreCustomHooks: empty Repository")
	}

	reader := streamio.NewReader(func() ([]byte, error) {
		if firstRequest != nil {
			data := firstRequest.GetData()
			firstRequest = nil
			return data, nil
		}

		request, err := stream.Recv()
		return request.GetData(), err
	})

	repoPath, err := helper.GetRepositoryPath(repo, s.storages)
	if err != nil {
		return status.Errorf(codes.Internal, "RestoreCustomHooks: getting repo path failed %v", err)
	}

	cmdArgs := []string{
		"-xf",
		"-",
		"-C",
		repoPath,
		customHooksDir,
	}

	ctx := stream.Context()
	cmd, err := command.New(ctx, exec.Command("tar", cmdArgs...), reader, nil, nil)
	if err != nil {
		return status.Errorf(codes.Internal, "RestoreCustomHooks: Could not untar custom hooks tar %v", err)
	}

	if err := cmd.Wait(); err != nil {
		return status.Errorf(codes.Internal, "RestoreCustomHooks: cmd wait failed: %v", err)
	}

	return stream.SendAndClose(&gitalypb.RestoreCustomHooksResponse{})
}
