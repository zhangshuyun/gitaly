package repository

import (
	"bytes"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/streamio"
)

func (suite *RepositoryServiceTestSuite) TestGetInfoAttributesExisting() {
	locator := config.NewLocator(config.Config)
	serverSocketPath, stop := runRepoServer(suite.T(), locator)
	defer stop()

	client, conn := newRepositoryClient(suite.T(), serverSocketPath)
	defer conn.Close()

	infoPath := filepath.Join(suite.repositoryPath, "info")
	os.MkdirAll(infoPath, 0755)

	buffSize := streamio.WriteBufferSize + 1
	data := bytes.Repeat([]byte("*.pbxproj binary\n"), buffSize)
	attrsPath := filepath.Join(infoPath, "attributes")
	err := ioutil.WriteFile(attrsPath, data, 0644)
	require.NoError(suite.T(), err)

	request := &gitalypb.GetInfoAttributesRequest{Repository: suite.repository}
	testCtx, cancelCtx := testhelper.Context()
	defer cancelCtx()

	stream, err := client.GetInfoAttributes(testCtx, request)
	require.NoError(suite.T(), err)

	receivedData, err := ioutil.ReadAll(streamio.NewReader(func() ([]byte, error) {
		response, err := stream.Recv()
		return response.GetAttributes(), err
	}))

	require.NoError(suite.T(), err)
	require.Equal(suite.T(), data, receivedData)
}

func (suite *RepositoryServiceTestSuite) TestGetInfoAttributesNonExisting() {
	locator := config.NewLocator(config.Config)
	serverSocketPath, stop := runRepoServer(suite.T(), locator)
	defer stop()

	client, conn := newRepositoryClient(suite.T(), serverSocketPath)
	defer conn.Close()

	request := &gitalypb.GetInfoAttributesRequest{Repository: suite.repository}
	testCtx, cancelCtx := testhelper.Context()
	defer cancelCtx()

	response, err := client.GetInfoAttributes(testCtx, request)
	require.NoError(suite.T(), err)

	message, err := response.Recv()
	require.NoError(suite.T(), err)

	require.Empty(suite.T(), message.GetAttributes())
}
