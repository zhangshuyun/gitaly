package repository

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"path"
	"strings"

	"gitlab.com/gitlab-org/gitaly-proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/internal/command"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/tempdir"
	"gitlab.com/gitlab-org/gitaly/streamio"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *server) CreateRepositoryFromBundle(stream gitalypb.RepositoryService_CreateRepositoryFromBundleServer) error {
	firstRequest, err := stream.Recv()
	if err != nil {
		return status.Errorf(codes.Internal, "CreateRepositoryFromBundle: first request failed: %v", err)
	}

	repo := firstRequest.GetRepository()
	if repo == nil {
		return status.Errorf(codes.InvalidArgument, "CreateRepositoryFromBundle: empty Repository")
	}

	firstRead := false
	reader := streamio.NewReader(func() ([]byte, error) {
		if !firstRead {
			firstRead = true
			return firstRequest.GetData(), nil
		}

		request, err := stream.Recv()
		return request.GetData(), err
	})

	ctx := stream.Context()

	tmpDir, err := tempdir.New(ctx, repo)
	if err != nil {
		cleanError := sanitizedError(tmpDir, "CreateRepositoryFromBundle: tmp dir failed: %v", err)
		return status.Error(codes.Internal, cleanError)
	}

	bundlePath := path.Join(tmpDir, "repo.bundle")
	file, err := os.Create(bundlePath)
	if err != nil {
		cleanError := sanitizedError(tmpDir, "CreateRepositoryFromBundle: new bundle file failed: %v", err)
		return status.Error(codes.Internal, cleanError)
	}

	_, err = io.Copy(file, reader)
	if err != nil {
		cleanError := sanitizedError(tmpDir, "CreateRepositoryFromBundle: new bundle file failed: %v", err)
		return status.Error(codes.Internal, cleanError)
	}

	repoPath, err := helper.GetPath(repo)
	if err != nil {
		return err
	}

	args := []string{
		"clone",
		"--bare",
		"--",
		bundlePath,
		repoPath,
	}
	cmd, err := command.New(ctx, exec.Command(command.GitPath(), args...), nil, nil, nil)
	if err != nil {
		cleanError := sanitizedError(repoPath, "CreateRepositoryFromBundle: cmd start failed: %v", err)
		return status.Error(codes.Internal, cleanError)
	}
	if err := cmd.Wait(); err != nil {
		cleanError := sanitizedError(repoPath, "CreateRepositoryFromBundle: cmd wait failed: %v", err)
		return status.Error(codes.Internal, cleanError)
	}

	// We do a fetch to get all refs including keep-around refs
	args = []string{
		"-C",
		repoPath,
		"fetch",
		bundlePath,
		"refs/*:refs/*",
	}

	cmd, err = command.New(ctx, exec.Command(command.GitPath(), args...), nil, nil, nil)
	if err != nil {
		cleanError := sanitizedError(repoPath, "CreateRepositoryFromBundle: cmd start failed fetching refs: %v", err)
		return status.Error(codes.Internal, cleanError)
	}
	if err := cmd.Wait(); err != nil {
		cleanError := sanitizedError(repoPath, "CreateRepositoryFromBundle: cmd wait failed fetching refs: %v", err)
		return status.Error(codes.Internal, cleanError)
	}

	// CreateRepository is harmless on existing repositories with the side effect that it creates the hook symlink.
	if _, err := s.CreateRepository(ctx, &gitalypb.CreateRepositoryRequest{Repository: repo}); err != nil {
		cleanError := sanitizedError(repoPath, "CreateRepositoryFromBundle: create hooks failed: %v", err)
		return status.Error(codes.Internal, cleanError)
	}

	return stream.SendAndClose(&gitalypb.CreateRepositoryFromBundleResponse{})
}

func sanitizedError(path, format string, a ...interface{}) string {
	str := fmt.Sprintf(format, a...)
	return strings.Replace(str, path, "[REPO PATH]", -1)
}
