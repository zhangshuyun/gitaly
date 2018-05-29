package repository

import (
	"io"
	"os"
	"path"
	"testing"

	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/tempdir"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/streamio"

	pb "gitlab.com/gitlab-org/gitaly-proto/go"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
)

func TestSuccessfulCreateRepositoryFromBundleRequest(t *testing.T) {
	server, serverSocketPath := runRepoServer(t)
	defer server.Stop()

	client, conn := newRepositoryClient(t, serverSocketPath)
	defer conn.Close()

	ctx, cancel := testhelper.Context()
	defer cancel()

	testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	tmpdir, err := tempdir.New(ctx, testRepo)
	require.NoError(t, err)
	bundlePath := path.Join(tmpdir, "original.bundle")

	testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "bundle", "create", bundlePath, "--all")
	defer os.RemoveAll(bundlePath)

	stream, err := client.CreateRepositoryFromBundle(ctx)
	require.NoError(t, err)

	importedRepo := &pb.Repository{
		StorageName:  testhelper.DefaultStorageName,
		RelativePath: "a-repo-from-bundle",
	}
	importedRepoPath, err := helper.GetPath(importedRepo)
	require.NoError(t, err)
	defer os.RemoveAll(importedRepoPath)

	request := &pb.CreateRepositoryFromBundleRequest{Repository: importedRepo}
	writer := streamio.NewWriter(func(p []byte) error {
		request.Data = p

		if err := stream.Send(request); err != nil {
			return err
		}

		request = &pb.CreateRepositoryFromBundleRequest{}

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

	info, err := os.Lstat(path.Join(importedRepoPath, "hooks"))
	require.NoError(t, err)
	require.NotEqual(t, 0, info.Mode()&os.ModeSymlink)
}

func TestFailedCreateRepositoryFromBundleRequestDueToValidations(t *testing.T) {
	server, serverSocketPath := runRepoServer(t)
	defer server.Stop()

	client, conn := newRepositoryClient(t, serverSocketPath)
	defer conn.Close()

	ctx, cancel := testhelper.Context()
	defer cancel()

	stream, err := client.CreateRepositoryFromBundle(ctx)
	require.NoError(t, err)

	require.NoError(t, stream.Send(&pb.CreateRepositoryFromBundleRequest{}))

	_, err = stream.CloseAndRecv()
	testhelper.RequireGrpcError(t, err, codes.InvalidArgument)
}
