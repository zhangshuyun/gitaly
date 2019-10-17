package repository

import (
	"bytes"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/streamio"
)

func TestGetInfoAttributesExisting(t *testing.T) {
	server, serverSocketPath := runRepoServer(t)
	defer server.Stop()

	client, conn := newRepositoryClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, repoPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	infoPath := path.Join(repoPath, "info")
	os.MkdirAll(infoPath, 0755)

	buffSize := streamio.WriteBufferSize + 1
	data := bytes.Repeat([]byte("*.pbxproj binary\n"), buffSize)
	attrsPath := path.Join(infoPath, "attributes")
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
	server, serverSocketPath := runRepoServer(t)
	defer server.Stop()

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

func TestSetInfoAttributes(t *testing.T) {
	server, serverSocketPath := runRepoServer(t)
	defer server.Stop()

	client, conn := newRepositoryClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, repoPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	ctx, cancel := testhelper.Context()
	defer cancel()

	stream, err := client.SetInfoAttributes(ctx)
	require.NoError(t, err)

	req := &gitalypb.SetInfoAttributesRequest{Repository: testRepo}

	writer := streamio.NewWriter(func(p []byte) error {
		req.Attributes = p

		if err := stream.Send(req); err != nil {
			return err
		}

		req = &gitalypb.SetInfoAttributesRequest{}
		return nil
	})

	attributes := []byte("*.docx diff=word\n*.pbxproj binary\n")

	_, err = writer.Write(attributes)
	require.NoError(t, err)

	_, err = stream.CloseAndRecv()
	require.NoError(t, err)

	attributesFile, err := os.Open(filepath.Join(repoPath, "info", "attributes"))
	require.NoError(t, err)

	attributesData, err := ioutil.ReadAll(attributesFile)
	require.NoError(t, err)

	require.Equal(t, attributes, attributesData)
}
