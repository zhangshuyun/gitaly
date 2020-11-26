package repository

import (
	"bytes"
	"io/ioutil"
	"os"
	"path/filepath"

	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/streamio"
)

func (suite *RepositoryServiceTestSuite) TestGetInfoAttributesExisting() {
	require := suite.Require()

	infoPath := filepath.Join(suite.repositoryPath, "info")
	os.MkdirAll(infoPath, 0755)

	buffSize := streamio.WriteBufferSize + 1
	data := bytes.Repeat([]byte("*.pbxproj binary\n"), buffSize)
	attrsPath := filepath.Join(infoPath, "attributes")
	err := ioutil.WriteFile(attrsPath, data, 0644)
	require.NoError(err)

	request := &gitalypb.GetInfoAttributesRequest{Repository: suite.repository}
	testCtx, cancelCtx := testhelper.Context()
	defer cancelCtx()

	stream, err := suite.client.GetInfoAttributes(testCtx, request)
	require.NoError(err)

	receivedData, err := ioutil.ReadAll(streamio.NewReader(func() ([]byte, error) {
		response, err := stream.Recv()
		return response.GetAttributes(), err
	}))

	require.NoError(err)
	require.Equal(data, receivedData)
}

func (suite *RepositoryServiceTestSuite) TestGetInfoAttributesNonExisting() {
	require := suite.Require()

	request := &gitalypb.GetInfoAttributesRequest{Repository: suite.repository}
	testCtx, cancelCtx := testhelper.Context()
	defer cancelCtx()

	response, err := suite.client.GetInfoAttributes(testCtx, request)
	require.NoError(err)

	message, err := response.Recv()
	require.NoError(err)

	require.Empty(message.GetAttributes())
}
