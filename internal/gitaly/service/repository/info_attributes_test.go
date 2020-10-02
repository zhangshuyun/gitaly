package repository

import (
	"bytes"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v13/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v13/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/v13/streamio"
)

func TestGetInfoAttributesExisting(t *testing.T) {
	serverSocketPath, stop := runRepoServer(t)
	defer stop()

	client, conn := newRepositoryClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, repoPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	infoPath := filepath.Join(repoPath, "info")
	os.MkdirAll(infoPath, 0755)

	buffSize := streamio.WriteBufferSize + 1
	data := bytes.Repeat([]byte("*.pbxproj binary\n"), buffSize)
	attrsPath := filepath.Join(infoPath, "attributes")
	err := ioutil.WriteFile(attrsPath, data, 0644)
	require.NoError(t, err)

	request := &gitalypb.GetInfoAttributesRequest{Repository: testRepo}
	testCtx, cancelCtx := testhelper.Context()
	defer cancelCtx()

	stream, err := client.GetInfoAttributes(testCtx, request)
	require.NoError(t, err)

	receivedData, err := ioutil.ReadAll(streamio.NewReader(func() ([]byte, error) {
		response, err := stream.Recv()
		return response.GetAttributes(), err
	}))

	require.NoError(t, err)
	require.Equal(t, data, receivedData)
}

func TestGetInfoAttributesNonExisting(t *testing.T) {
	serverSocketPath, stop := runRepoServer(t)
	defer stop()

	client, conn := newRepositoryClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	request := &gitalypb.GetInfoAttributesRequest{Repository: testRepo}
	testCtx, cancelCtx := testhelper.Context()
	defer cancelCtx()

	response, err := client.GetInfoAttributes(testCtx, request)
	require.NoError(t, err)

	message, err := response.Recv()
	require.NoError(t, err)

	require.Empty(t, message.GetAttributes())
}
