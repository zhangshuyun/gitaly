package repository

import (
	"os"
	"os/exec"
	"path"

	"gitlab.com/gitlab-org/gitaly/internal/command"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/streamio"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const customHooksDir = "custom_hooks"

func (s *server) BackupCustomHooks(in *gitalypb.BackupCustomHooksRequest, stream gitalypb.RepositoryService_BackupCustomHooksServer) error {
	repoPath, err := helper.GetRepositoryPath(in.Repository, s.storages)
	if err != nil {
		return status.Errorf(codes.Internal, "BackupCustomHooks: getting repo path failed %v", err)
	}

	writer := streamio.NewWriter(func(p []byte) error {
		return stream.Send(&gitalypb.BackupCustomHooksResponse{Data: p})
	})

	if _, err := os.Lstat(path.Join(repoPath, customHooksDir)); os.IsNotExist(err) {
		return nil
	}

	ctx := stream.Context()
	tar := exec.Command("tar", "-c", "-f", "-", "-C", repoPath, customHooksDir)
	cmd, err := command.New(ctx, tar, nil, writer, nil)
	if err != nil {
		return status.Errorf(codes.Internal, "%v", err)
	}

	if err := cmd.Wait(); err != nil {
		return status.Errorf(codes.Internal, "%v", err)
	}

	return nil
}
