package repository

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/tempdir"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/v14/streamio"
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

	ctx := stream.Context()

	firstRead := false
	bundleReader := streamio.NewReader(func() ([]byte, error) {
		if !firstRead {
			firstRead = true
			return firstRequest.GetData(), nil
		}

		request, err := stream.Recv()
		return request.GetData(), err
	})

	bundleDir, err := tempdir.New(ctx, repo.GetStorageName(), s.locator)
	if err != nil {
		return helper.ErrInternalf("creating bundle directory: %w", err)
	}

	bundlePath := filepath.Join(bundleDir.Path(), "repo.bundle")
	bundleFile, err := os.Create(bundlePath)
	if err != nil {
		return helper.ErrInternalf("creating bundle file: %w", err)
	}

	if _, err := io.Copy(bundleFile, bundleReader); err != nil {
		return helper.ErrInternalf("writing bundle file: %w", err)
	}

	if err := s.createRepository(ctx, repo, func(repo *gitalypb.Repository) error {
		var stderr bytes.Buffer
		cmd, err := s.gitCmdFactory.New(ctx, repo, git.SubCmd{
			Name: "fetch",
			Flags: []git.Option{
				git.Flag{Name: "--quiet"},
				git.Flag{Name: "--atomic"},
			},
			Args: []string{bundlePath, "refs/*:refs/*"},
		}, git.WithStderr(&stderr), git.WithRefTxHook(ctx, repo, s.cfg))
		if err != nil {
			return helper.ErrInternalf("spawning fetch: %w", err)
		}

		if err := cmd.Wait(); err != nil {
			sanitizedStderr := sanitizedError("%s", stderr.String())
			return helper.ErrInternalf("fetch from bundle: %w, stderr: %q", err, sanitizedStderr)
		}

		return nil
	}); err != nil {
		return helper.ErrInternalf("creating repository: %w", err)
	}

	return stream.SendAndClose(&gitalypb.CreateRepositoryFromBundleResponse{})
}

func sanitizedError(path, format string, a ...interface{}) string {
	str := fmt.Sprintf(format, a...)
	return strings.Replace(str, path, "[REPO PATH]", -1)
}
