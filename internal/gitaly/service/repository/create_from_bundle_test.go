package repository

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/tempdir"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/streamio"
	"google.golang.org/grpc/codes"
)

func TestServer_CreateRepositoryFromBundle_successful(t *testing.T) {
	locator := config.NewLocator(config.Config)
	serverSocketPath, stop := runRepoServer(t, locator, testhelper.WithInternalSocket(config.Config))
	defer stop()

	client, conn := newRepositoryClient(t, serverSocketPath)
	defer conn.Close()

	ctx, cancel := testhelper.Context()
	defer cancel()

	testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	tmpdir, err := tempdir.New(ctx, testRepo, locator)
	require.NoError(t, err)
	bundlePath := filepath.Join(tmpdir, "original.bundle")

	testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "update-ref", "refs/custom-refs/ref1", "HEAD")

	testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "bundle", "create", bundlePath, "--all")
	defer os.RemoveAll(bundlePath)

	stream, err := client.CreateRepositoryFromBundle(ctx)
	require.NoError(t, err)

	importedRepoProto := &gitalypb.Repository{
		StorageName:  testhelper.DefaultStorageName,
		RelativePath: "a-repo-from-bundle",
	}
	importedRepo := localrepo.New(importedRepoProto, config.Config)
	importedRepoPath, err := locator.GetPath(importedRepoProto)
	require.NoError(t, err)
	defer os.RemoveAll(importedRepoPath)

	request := &gitalypb.CreateRepositoryFromBundleRequest{Repository: importedRepoProto}
	writer := streamio.NewWriter(func(p []byte) error {
		request.Data = p

		if err := stream.Send(request); err != nil {
			return err
		}

		request = &gitalypb.CreateRepositoryFromBundleRequest{}

		return nil
	})

	file, err := os.Open(bundlePath)
	require.NoError(t, err)
	defer file.Close()

	_, err = io.Copy(writer, file)
	require.NoError(t, err)

	_, err = stream.CloseAndRecv()
	require.NoError(t, err)

	testhelper.MustRunCommand(t, nil, "git", "-C", importedRepoPath, "fsck")

	info, err := os.Lstat(filepath.Join(importedRepoPath, "hooks"))
	require.NoError(t, err)
	require.NotEqual(t, 0, info.Mode()&os.ModeSymlink)

	commit, err := importedRepo.ReadCommit(ctx, "refs/custom-refs/ref1")
	require.NoError(t, err)
	require.NotNil(t, commit)
}

func TestServer_CreateRepositoryFromBundle_failed_invalid_bundle(t *testing.T) {
	locator := config.NewLocator(config.Config)
	serverSocketPath, stop := runRepoServer(t, locator)
	defer stop()

	client, conn := newRepositoryClient(t, serverSocketPath)
	defer conn.Close()

	ctx, cancel := testhelper.Context()
	defer cancel()

	stream, err := client.CreateRepositoryFromBundle(ctx)
	require.NoError(t, err)

	importedRepo := &gitalypb.Repository{
		StorageName:  testhelper.DefaultStorageName,
		RelativePath: "a-repo-from-bundle",
	}
	importedRepoPath, err := locator.GetPath(importedRepo)
	require.NoError(t, err)
	defer os.RemoveAll(importedRepoPath)

	request := &gitalypb.CreateRepositoryFromBundleRequest{Repository: importedRepo}
	writer := streamio.NewWriter(func(p []byte) error {
		request.Data = p

		if err := stream.Send(request); err != nil {
			return err
		}

		request = &gitalypb.CreateRepositoryFromBundleRequest{}

		return nil
	})

	_, err = io.Copy(writer, bytes.NewBufferString("not-a-bundle"))
	require.NoError(t, err)

	_, err = stream.CloseAndRecv()
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid gitfile format")
}

func TestServer_CreateRepositoryFromBundle_failed_validations(t *testing.T) {
	locator := config.NewLocator(config.Config)
	serverSocketPath, stop := runRepoServer(t, locator)
	defer stop()

	client, conn := newRepositoryClient(t, serverSocketPath)
	defer conn.Close()

	ctx, cancel := testhelper.Context()
	defer cancel()

	stream, err := client.CreateRepositoryFromBundle(ctx)
	require.NoError(t, err)

	require.NoError(t, stream.Send(&gitalypb.CreateRepositoryFromBundleRequest{}))

	_, err = stream.CloseAndRecv()
	testhelper.RequireGrpcError(t, err, codes.InvalidArgument)
}

func TestServer_CreateRepositoryFromBundle_failed_existing_directory(t *testing.T) {
	locator := config.NewLocator(config.Config)
	serverSocketPath, stop := runRepoServer(t, locator)
	defer stop()

	testRepo, _, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()

	client, conn := newRepositoryClient(t, serverSocketPath)
	defer conn.Close()

	ctx, cancel := testhelper.Context()
	defer cancel()

	stream, err := client.CreateRepositoryFromBundle(ctx)
	require.NoError(t, err)

	require.NoError(t, stream.Send(&gitalypb.CreateRepositoryFromBundleRequest{
		Repository: testRepo,
	}))

	_, err = stream.CloseAndRecv()
	testhelper.RequireGrpcError(t, err, codes.FailedPrecondition)
	testhelper.GrpcErrorHasMessage(t, err, "CreateRepositoryFromBundle: target directory is non-empty")
}

func TestSanitizedError(t *testing.T) {
	testCases := []struct {
		path     string
		format   string
		a        []interface{}
		expected string
	}{
		{
			path:     "/home/git/storage",
			format:   "failed to create from bundle in /home/git/storage/my-project",
			expected: "failed to create from bundle in [REPO PATH]/my-project",
		},
		{
			path:     "/home/git/storage",
			format:   "failed to %s in [REPO PATH]/my-project",
			a:        []interface{}{"create from bundle"},
			expected: "failed to create from bundle in [REPO PATH]/my-project",
		},
	}

	for _, tc := range testCases {
		str := sanitizedError(tc.path, tc.format, tc.a...)
		assert.Equal(t, tc.expected, str)
	}
}
