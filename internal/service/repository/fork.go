package repository

import (
	"fmt"
	"os"
	"os/exec"
	"path"

	"github.com/golang/protobuf/jsonpb"
	"gitlab.com/gitlab-org/gitaly-proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/internal/command"
	"gitlab.com/gitlab-org/gitaly/internal/config"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const gitalyInternalURL = "ssh://gitaly/internal.git"

func (s *server) CreateFork(ctx context.Context, req *gitalypb.CreateForkRequest) (*gitalypb.CreateForkResponse, error) {
	targetRepository := req.Repository
	sourceRepository := req.SourceRepository

	if sourceRepository == nil {
		return nil, status.Errorf(codes.InvalidArgument, "CreateFork: empty SourceRepository")
	}
	if targetRepository == nil {
		return nil, status.Errorf(codes.InvalidArgument, "CreateFork: empty Repository")
	}

	targetRepositoryFullPath, err := helper.GetPath(targetRepository)
	if err != nil {
		return nil, err
	}

	if _, err := os.Stat(targetRepositoryFullPath); !os.IsNotExist(err) {
		return nil, status.Errorf(codes.InvalidArgument, "CreateFork: dest dir exists")
	}

	if err := os.MkdirAll(targetRepositoryFullPath, 0770); err != nil {
		return nil, status.Errorf(codes.Internal, "CreateFork: create dest dir: %v", err)
	}

	gitalyServersInfo, err := helper.ExtractGitalyServers(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "CreateFork: extracting Gitaly servers: %v", err)
	}

	sourceRepositoryStorageInfo, ok := gitalyServersInfo[sourceRepository.StorageName]
	if !ok {
		return nil, status.Errorf(codes.InvalidArgument, "CreateFork: no storage info for %s", sourceRepository.StorageName)
	}

	sourceRepositoryGitalyAddress := sourceRepositoryStorageInfo["address"]
	if sourceRepositoryGitalyAddress == "" {
		return nil, status.Errorf(codes.InvalidArgument, "CreateFork: empty gitaly address")
	}

	sourceRepositoryGitalyToken := sourceRepositoryStorageInfo["token"]

	cloneReq := &gitalypb.SSHUploadPackRequest{Repository: sourceRepository}
	pbMarshaler := &jsonpb.Marshaler{}
	payload, err := pbMarshaler.MarshalToString(cloneReq)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "CreateFork: marshalling payload failed: %v", err)
	}

	gitalySSHPath := path.Join(config.Config.BinDir, "gitaly-ssh")

	env := []string{
		fmt.Sprintf("GITALY_ADDRESS=%s", sourceRepositoryGitalyAddress),
		fmt.Sprintf("GITALY_PAYLOAD=%s", payload),
		fmt.Sprintf("GITALY_TOKEN=%s", sourceRepositoryGitalyToken),
		fmt.Sprintf("GIT_SSH_COMMAND=%s upload-pack", gitalySSHPath),
	}
	args := []string{
		"clone",
		"--bare",
		"--no-local",
		"--",
		fmt.Sprintf("%s:%s", gitalyInternalURL, sourceRepository.RelativePath),
		targetRepositoryFullPath,
	}
	cmd, err := command.New(ctx, exec.Command(command.GitPath(), args...), nil, nil, nil, env...)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "CreateFork: clone cmd start: %v", err)
	}
	if err := cmd.Wait(); err != nil {
		return nil, status.Errorf(codes.Internal, "CreateFork: clone cmd wait: %v", err)
	}

	if err := removeOriginInRepo(ctx, targetRepository); err != nil {
		return nil, status.Errorf(codes.Internal, "CreateFork: %v", err)
	}

	// CreateRepository is harmless on existing repositories with the side effect that it creates the hook symlink.
	if _, err := s.CreateRepository(ctx, &gitalypb.CreateRepositoryRequest{Repository: targetRepository}); err != nil {
		return nil, status.Errorf(codes.Internal, "CreateFork: create hooks failed: %v", err)
	}

	return &gitalypb.CreateForkResponse{}, nil
}
