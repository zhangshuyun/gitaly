package repository

import (
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/v14/streamio"
	"google.golang.org/grpc/codes"
)

func TestSuccessfullRestoreCustomHooksRequest(t *testing.T) {
	_, repo, repoPath, client := setupRepositoryService(t)

	ctx, cancel := testhelper.Context()
	defer cancel()

	stream, err := client.RestoreCustomHooks(ctx)
	require.NoError(t, err)

	request := &gitalypb.RestoreCustomHooksRequest{Repository: repo}
	writer := streamio.NewWriter(func(p []byte) error {
		request.Data = p
		if err := stream.Send(request); err != nil {
			return err
		}

		request = &gitalypb.RestoreCustomHooksRequest{}
		return nil
	})

	file, err := os.Open("testdata/custom_hooks.tar")
	require.NoError(t, err)
	defer file.Close()

	_, err = io.Copy(writer, file)
	require.NoError(t, err)
	_, err = stream.CloseAndRecv()
	require.NoError(t, err)

	require.FileExists(t, filepath.Join(repoPath, "custom_hooks", "pre-push.sample"))
}

func TestFailedRestoreCustomHooksDueToValidations(t *testing.T) {
	_, client := setupRepositoryServiceWithoutRepo(t)

	ctx, cancel := testhelper.Context()
	defer cancel()

	stream, err := client.RestoreCustomHooks(ctx)
	require.NoError(t, err)

	require.NoError(t, stream.Send(&gitalypb.RestoreCustomHooksRequest{}))

	_, err = stream.CloseAndRecv()
	testhelper.RequireGrpcError(t, err, codes.InvalidArgument)
}

func TestFailedRestoreCustomHooksDueToBadTar(t *testing.T) {
	_, repo, _, client := setupRepositoryService(t)

	ctx, cancel := testhelper.Context()
	defer cancel()

	stream, err := client.RestoreCustomHooks(ctx)

	require.NoError(t, err)

	request := &gitalypb.RestoreCustomHooksRequest{Repository: repo}
	writer := streamio.NewWriter(func(p []byte) error {
		request.Data = p
		if err := stream.Send(request); err != nil {
			return err
		}

		request = &gitalypb.RestoreCustomHooksRequest{}
		return nil
	})

	file, err := os.Open("testdata/corrupted_hooks.tar")
	require.NoError(t, err)
	defer file.Close()

	_, err = io.Copy(writer, file)
	require.NoError(t, err)
	_, err = stream.CloseAndRecv()

	testhelper.RequireGrpcError(t, err, codes.Internal)
}
