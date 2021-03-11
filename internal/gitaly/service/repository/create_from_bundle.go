package repository

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/tempdir"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
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

	repoPath, err := s.locator.GetPath(repo)
	if err != nil {
		return helper.ErrInternal(err)
	}

	if !isDirEmpty(repoPath) {
		return helper.ErrPreconditionFailed(errors.New("CreateRepositoryFromBundle: target directory is non-empty"))
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

	tmpDir, err := tempdir.New(ctx, repo, s.locator)
	if err != nil {
		cleanError := sanitizedError(tmpDir, "CreateRepositoryFromBundle: tmp dir failed: %v", err)
		return status.Error(codes.Internal, cleanError)
	}

	bundlePath := filepath.Join(tmpDir, "repo.bundle")
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

	stderr := bytes.Buffer{}
	cmd, err := s.gitCmdFactory.NewWithoutRepo(ctx,
		git.SubCmd{
			Name: "clone",
			Flags: []git.Option{
				git.Flag{Name: "--bare"},
				git.Flag{Name: "--quiet"},
			},
			PostSepArgs: []string{bundlePath, repoPath},
		},
		git.WithStderr(&stderr),
		git.WithRefTxHook(ctx, repo, s.cfg),
	)
	if err != nil {
		cleanError := sanitizedError(repoPath, "CreateRepositoryFromBundle: cmd start failed: %v", err)
		return status.Error(codes.Internal, cleanError)
	}
	if err := cmd.Wait(); err != nil {
		cleanError := sanitizedError(repoPath, "CreateRepositoryFromBundle: cmd wait failed: %s: %v", stderr.String(), err)
		return status.Error(codes.Internal, cleanError)
	}

	// We do a fetch to get all refs including keep-around refs
	stderr.Reset()
	cmd, err = s.gitCmdFactory.NewWithDir(ctx, repoPath,
		git.SubCmd{
			Name:  "fetch",
			Flags: []git.Option{git.Flag{Name: "--quiet"}},
			Args:  []string{bundlePath, "refs/*:refs/*"},
		},
		git.WithStderr(&stderr),
		git.WithRefTxHook(ctx, repo, s.cfg),
	)
	if err != nil {
		cleanError := sanitizedError(repoPath, "CreateRepositoryFromBundle: cmd start failed fetching refs: %v", err)
		return status.Error(codes.Internal, cleanError)
	}
	if err := cmd.Wait(); err != nil {
		cleanError := sanitizedError(repoPath, "CreateRepositoryFromBundle: cmd wait failed fetching refs: %s", stderr.String())
		return status.Error(codes.Internal, cleanError)
	}

	// CreateRepository is harmless on existing repositories with the side effect that it creates the hook symlink.
	if _, err := s.CreateRepository(ctx, &gitalypb.CreateRepositoryRequest{Repository: repo}); err != nil {
		cleanError := sanitizedError(repoPath, "CreateRepositoryFromBundle: create hooks failed: %v", err)
		return status.Error(codes.Internal, cleanError)
	}

	return stream.SendAndClose(&gitalypb.CreateRepositoryFromBundleResponse{})
}

func isDirEmpty(path string) bool {
	f, err := os.Open(path)
	if os.IsNotExist(err) {
		return true
	}

	_, err = f.Readdir(1)

	return err == io.EOF
}

func sanitizedError(path, format string, a ...interface{}) string {
	str := fmt.Sprintf(format, a...)
	return strings.Replace(str, path, "[REPO PATH]", -1)
}
