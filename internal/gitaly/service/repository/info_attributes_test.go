package repository

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/v14/streamio"
)

func TestGetInfoAttributesExisting(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	_, repo, repoPath, client := setupRepositoryService(ctx, t)

	infoPath := filepath.Join(repoPath, "info")
	require.NoError(t, os.MkdirAll(infoPath, 0o755))

	buffSize := streamio.WriteBufferSize + 1
	data := bytes.Repeat([]byte("*.pbxproj binary\n"), buffSize)
	attrsPath := filepath.Join(infoPath, "attributes")
	err := os.WriteFile(attrsPath, data, 0o644)
	require.NoError(t, err)

	request := &gitalypb.GetInfoAttributesRequest{Repository: repo}

	stream, err := client.GetInfoAttributes(ctx, request)
	require.NoError(t, err)

	receivedData, err := io.ReadAll(streamio.NewReader(func() ([]byte, error) {
		response, err := stream.Recv()
		return response.GetAttributes(), err
	}))

	require.NoError(t, err)
	require.Equal(t, data, receivedData)
}

func TestGetInfoAttributesNonExisting(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	_, repo, _, client := setupRepositoryService(ctx, t)

	request := &gitalypb.GetInfoAttributesRequest{Repository: repo}

	response, err := client.GetInfoAttributes(ctx, request)
	require.NoError(t, err)

	message, err := response.Recv()
	require.NoError(t, err)

	require.Empty(t, message.GetAttributes())
}
