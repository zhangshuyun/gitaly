package smarthttp

import (
	"fmt"
	"io"
	"io/ioutil"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly-proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/internal/config"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/streamio"
	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
)

func TestSuccessfulInfoRefsUploadPack(t *testing.T) {
	server, serverSocketPath := runSmartHTTPServer(t)
	defer server.Stop()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	rpcRequest := &gitalypb.InfoRefsRequest{Repository: testRepo}

	response, err := makeInfoRefsUploadPackRequest(t, serverSocketPath, rpcRequest)
	require.NoError(t, err)
	assertGitRefAdvertisement(t, "InfoRefsUploadPack", string(response), "001e# service=git-upload-pack", "0000", []string{
		"003ef4e6814c3e4e7a0de82a9e7cd20c626cc963a2f8 refs/tags/v1.0.0",
		"00416f6d7e7ed97bb5f0054f2b1df789b39ca89b6ff9 refs/tags/v1.0.0^{}",
	})
}

func TestSuccessfulInfoRefsUploadPackWithGitConfigOptions(t *testing.T) {
	server, serverSocketPath := runSmartHTTPServer(t)
	defer server.Stop()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	// transfer.hideRefs=refs will hide every ref that info-refs would normally
	// output, allowing us to test that the custom configuration is respected
	rpcRequest := &gitalypb.InfoRefsRequest{
		Repository:       testRepo,
		GitConfigOptions: []string{"transfer.hideRefs=refs"},
	}

	response, err := makeInfoRefsUploadPackRequest(t, serverSocketPath, rpcRequest)
	require.NoError(t, err)
	assertGitRefAdvertisement(t, "InfoRefsUploadPack", string(response), "001e# service=git-upload-pack", "0000", []string{})
}

func TestSuccessfulInfoRefsUploadPackWithGitProtocol(t *testing.T) {
	defer func(old string) {
		config.Config.Git.BinPath = old
	}(config.Config.Git.BinPath)
	config.Config.Git.BinPath = "../../testhelper/env_git"

	server, serverSocketPath := runSmartHTTPServer(t)
	defer server.Stop()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	rpcRequest := &gitalypb.InfoRefsRequest{
		Repository:  testRepo,
		GitProtocol: git.ProtocolV2,
	}

	client, _ := newSmartHTTPClient(t, serverSocketPath)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c, err := client.InfoRefsUploadPack(ctx, rpcRequest)

	for {
		_, err := c.Recv()
		if err != nil {
			require.Equal(t, io.EOF, err)
			break
		}
	}

	require.NoError(t, err)

	envData, err := testhelper.GetGitEnvData()

	require.NoError(t, err)
	require.Equal(t, fmt.Sprintf("GIT_PROTOCOL=%s\n", git.ProtocolV2), envData)
}

func makeInfoRefsUploadPackRequest(t *testing.T, serverSocketPath string, rpcRequest *gitalypb.InfoRefsRequest) ([]byte, error) {
	client, conn := newSmartHTTPClient(t, serverSocketPath)
	defer conn.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c, err := client.InfoRefsUploadPack(ctx, rpcRequest)
	require.NoError(t, err)

	response, err := ioutil.ReadAll(streamio.NewReader(func() ([]byte, error) {
		resp, err := c.Recv()
		return resp.GetData(), err
	}))

	return response, err
}

func TestSuccessfulInfoRefsReceivePack(t *testing.T) {
	server, serverSocketPath := runSmartHTTPServer(t)
	defer server.Stop()

	client, conn := newSmartHTTPClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	rpcRequest := &gitalypb.InfoRefsRequest{Repository: testRepo}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c, err := client.InfoRefsReceivePack(ctx, rpcRequest)
	if err != nil {
		t.Fatal(err)
	}

	response, err := ioutil.ReadAll(streamio.NewReader(func() ([]byte, error) {
		resp, err := c.Recv()
		return resp.GetData(), err
	}))
	if err != nil {
		t.Fatal(err)
	}

	assertGitRefAdvertisement(t, "InfoRefsReceivePack", string(response), "001f# service=git-receive-pack", "0000", []string{
		"003ef4e6814c3e4e7a0de82a9e7cd20c626cc963a2f8 refs/tags/v1.0.0",
		"003e8a2a6eb295bb170b34c24c76c49ed0e9b2eaf34b refs/tags/v1.1.0",
	})
}

func TestFailureRepoNotFoundInfoRefsReceivePack(t *testing.T) {
	server, serverSocketPath := runSmartHTTPServer(t)
	defer server.Stop()

	client, conn := newSmartHTTPClient(t, serverSocketPath)
	defer conn.Close()
	repo := &gitalypb.Repository{StorageName: "default", RelativePath: "testdata/data/another_repo"}
	rpcRequest := &gitalypb.InfoRefsRequest{Repository: repo}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c, err := client.InfoRefsReceivePack(ctx, rpcRequest)
	if err != nil {
		t.Fatal(err)
	}

	for err == nil {
		_, err = c.Recv()
	}
	testhelper.RequireGrpcError(t, err, codes.NotFound)
}

func TestFailureRepoNotSetInfoRefsReceivePack(t *testing.T) {
	server, serverSocketPath := runSmartHTTPServer(t)
	defer server.Stop()

	client, conn := newSmartHTTPClient(t, serverSocketPath)
	defer conn.Close()
	rpcRequest := &gitalypb.InfoRefsRequest{}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c, err := client.InfoRefsReceivePack(ctx, rpcRequest)
	if err != nil {
		t.Fatal(err)
	}

	for err == nil {
		_, err = c.Recv()
	}
	testhelper.RequireGrpcError(t, err, codes.InvalidArgument)
}

func assertGitRefAdvertisement(t *testing.T, rpc, responseBody string, firstLine, lastLine string, middleLines []string) {
	responseLines := strings.Split(responseBody, "\n")

	if responseLines[0] != firstLine {
		t.Errorf("%q: expected response first line to be %q, found %q", rpc, firstLine, responseLines[0])
	}

	lastIndex := len(responseLines) - 1
	if responseLines[lastIndex] != lastLine {
		t.Errorf("%q: expected response last line to be %q, found %q", rpc, lastLine, responseLines[lastIndex])
	}

	for _, ref := range middleLines {
		if !strings.Contains(responseBody, ref) {
			t.Errorf("%q: expected response to contain %q, found none", rpc, ref)
		}
	}
}
