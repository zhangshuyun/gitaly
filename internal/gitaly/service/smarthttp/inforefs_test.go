package smarthttp

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/cache"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/objectpool"
	"gitlab.com/gitlab-org/gitaly/internal/git/stats"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/tempdir"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/streamio"
	"google.golang.org/grpc/codes"
)

func TestSuccessfulInfoRefsUploadPack(t *testing.T) {
	serverSocketPath, stop := runSmartHTTPServer(t, config.Config)
	defer stop()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	rpcRequest := &gitalypb.InfoRefsRequest{Repository: testRepo}

	ctx, cancel := testhelper.Context()
	defer cancel()

	response, err := makeInfoRefsUploadPackRequest(ctx, t, serverSocketPath, config.Config.Auth.Token, rpcRequest)
	require.NoError(t, err)
	assertGitRefAdvertisement(t, "InfoRefsUploadPack", string(response), "001e# service=git-upload-pack", "0000", []string{
		"003ef4e6814c3e4e7a0de82a9e7cd20c626cc963a2f8 refs/tags/v1.0.0",
		"00416f6d7e7ed97bb5f0054f2b1df789b39ca89b6ff9 refs/tags/v1.0.0^{}",
	})
}

func TestSuccessfulInfoRefsUploadWithPartialClone(t *testing.T) {
	serverSocketPath, stop := runSmartHTTPServer(t, config.Config)
	defer stop()

	ctx, cancel := testhelper.Context()
	defer cancel()

	testRepo, _, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()

	request := &gitalypb.InfoRefsRequest{
		Repository: testRepo,
	}

	partialResponse, err := makeInfoRefsUploadPackRequest(ctx, t, serverSocketPath, config.Config.Auth.Token, request)
	require.NoError(t, err)
	partialRefs := stats.Get{}
	err = partialRefs.Parse(bytes.NewReader(partialResponse))
	require.NoError(t, err)

	for _, c := range []string{"allow-tip-sha1-in-want", "allow-reachable-sha1-in-want", "filter"} {
		require.Contains(t, partialRefs.Caps, c)
	}
}

func TestSuccessfulInfoRefsUploadPackWithGitConfigOptions(t *testing.T) {
	serverSocketPath, stop := runSmartHTTPServer(t, config.Config)
	defer stop()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	// transfer.hideRefs=refs will hide every ref that info-refs would normally
	// output, allowing us to test that the custom configuration is respected
	rpcRequest := &gitalypb.InfoRefsRequest{
		Repository:       testRepo,
		GitConfigOptions: []string{"transfer.hideRefs=refs"},
	}

	ctx, cancel := testhelper.Context()
	defer cancel()

	response, err := makeInfoRefsUploadPackRequest(ctx, t, serverSocketPath, config.Config.Auth.Token, rpcRequest)
	require.NoError(t, err)
	assertGitRefAdvertisement(t, "InfoRefsUploadPack", string(response), "001e# service=git-upload-pack", "0000", []string{})
}

func TestSuccessfulInfoRefsUploadPackWithGitProtocol(t *testing.T) {
	defer func(old config.Cfg) { config.Config = old }(config.Config)

	cfg, restore := testhelper.EnableGitProtocolV2Support(t, config.Config)
	defer restore()
	config.Config = cfg

	serverSocketPath, stop := runSmartHTTPServer(t, config.Config)
	defer stop()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	rpcRequest := &gitalypb.InfoRefsRequest{
		Repository:  testRepo,
		GitProtocol: git.ProtocolV2,
	}

	client, _ := newSmartHTTPClient(t, serverSocketPath, config.Config.Auth.Token)
	ctx, cancel := testhelper.Context()
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

	envData := testhelper.GetGitEnvData(t)
	require.Contains(t, envData, fmt.Sprintf("GIT_PROTOCOL=%s\n", git.ProtocolV2))
}

func makeInfoRefsUploadPackRequest(ctx context.Context, t *testing.T, serverSocketPath, token string, rpcRequest *gitalypb.InfoRefsRequest) ([]byte, error) {
	client, conn := newSmartHTTPClient(t, serverSocketPath, token)
	defer conn.Close()

	ctx, cancel := context.WithCancel(ctx)
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
	serverSocketPath, stop := runSmartHTTPServer(t, config.Config)
	defer stop()

	client, conn := newSmartHTTPClient(t, serverSocketPath, config.Config.Auth.Token)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	rpcRequest := &gitalypb.InfoRefsRequest{Repository: testRepo}

	ctx, cancel := testhelper.Context()
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

func TestObjectPoolRefAdvertisementHiding(t *testing.T) {
	serverSocketPath, stop := runSmartHTTPServer(t, config.Config)
	defer stop()

	client, conn := newSmartHTTPClient(t, serverSocketPath, config.Config.Auth.Token)
	defer conn.Close()

	repo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	ctx, cancel := testhelper.Context()
	defer cancel()

	pool, err := objectpool.NewObjectPool(config.Config, config.NewLocator(config.Config), git.NewExecCommandFactory(config.Config), repo.GetStorageName(), testhelper.NewTestObjectPoolName(t))
	require.NoError(t, err)

	require.NoError(t, pool.Create(ctx, repo))
	defer pool.Remove(ctx)

	commitID := testhelper.CreateCommit(t, pool.FullPath(), t.Name(), nil)

	require.NoError(t, pool.Link(ctx, repo))

	rpcRequest := &gitalypb.InfoRefsRequest{Repository: repo}

	c, err := client.InfoRefsReceivePack(ctx, rpcRequest)
	require.NoError(t, err)

	response, err := ioutil.ReadAll(streamio.NewReader(func() ([]byte, error) {
		resp, err := c.Recv()
		return resp.GetData(), err
	}))

	require.NoError(t, err)
	require.NotContains(t, string(response), commitID+" .have")
}

func TestFailureRepoNotFoundInfoRefsReceivePack(t *testing.T) {
	serverSocketPath, stop := runSmartHTTPServer(t, config.Config)
	defer stop()

	client, conn := newSmartHTTPClient(t, serverSocketPath, config.Config.Auth.Token)
	defer conn.Close()
	repo := &gitalypb.Repository{StorageName: "default", RelativePath: "testdata/scratch/another_repo"}
	rpcRequest := &gitalypb.InfoRefsRequest{Repository: repo}

	ctx, cancel := testhelper.Context()
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
	serverSocketPath, stop := runSmartHTTPServer(t, config.Config)
	defer stop()

	client, conn := newSmartHTTPClient(t, serverSocketPath, config.Config.Auth.Token)
	defer conn.Close()
	rpcRequest := &gitalypb.InfoRefsRequest{}

	ctx, cancel := testhelper.Context()
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

type mockStreamer struct {
	streamer
	putStream func(context.Context, *gitalypb.Repository, proto.Message, io.Reader) error
}

func (ms mockStreamer) PutStream(ctx context.Context, repo *gitalypb.Repository, req proto.Message, src io.Reader) error {
	if ms.putStream != nil {
		return ms.putStream(ctx, repo, req, src)
	}
	return ms.streamer.PutStream(ctx, repo, req, src)
}

func TestCacheInfoRefsUploadPack(t *testing.T) {
	clearCache(t)

	serverSocketPath, stop := runSmartHTTPServer(t, config.Config)
	defer stop()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	rpcRequest := &gitalypb.InfoRefsRequest{Repository: testRepo}

	ctx, cancel := testhelper.Context()
	defer cancel()

	assertNormalResponse := func() {
		ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()

		response, err := makeInfoRefsUploadPackRequest(ctx, t, serverSocketPath, config.Config.Auth.Token, rpcRequest)
		require.NoError(t, err)

		assertGitRefAdvertisement(t, "InfoRefsUploadPack", string(response),
			"001e# service=git-upload-pack", "0000",
			[]string{
				"003ef4e6814c3e4e7a0de82a9e7cd20c626cc963a2f8 refs/tags/v1.0.0",
				"00416f6d7e7ed97bb5f0054f2b1df789b39ca89b6ff9 refs/tags/v1.0.0^{}",
			},
		)
	}

	assertNormalResponse()
	require.FileExists(t, pathToCachedResponse(t, ctx, rpcRequest))

	replacedContents := []string{
		"first line",
		"meow meow meow meow",
		"woof woof woof woof",
		"last line",
	}

	// replace cached response file to prove the info-ref uses the cache
	replaceCachedResponse(t, ctx, rpcRequest, strings.Join(replacedContents, "\n"))
	response, err := makeInfoRefsUploadPackRequest(ctx, t, serverSocketPath, config.Config.Auth.Token, rpcRequest)
	require.NoError(t, err)
	assertGitRefAdvertisement(t, "InfoRefsUploadPack", string(response),
		replacedContents[0], replacedContents[3], replacedContents[1:3],
	)

	invalidateCacheForRepo := func() {
		ender, err := cache.NewLeaseKeyer(config.NewLocator(config.Config)).StartLease(rpcRequest.Repository)
		require.NoError(t, err)
		require.NoError(t, ender.EndLease(setInfoRefsUploadPackMethod(ctx)))
	}

	invalidateCacheForRepo()

	// replaced cache response is no longer valid
	assertNormalResponse()

	// failed requests should not cache response
	invalidReq := &gitalypb.InfoRefsRequest{
		Repository: &gitalypb.Repository{
			RelativePath: "fake_repo",
			StorageName:  testRepo.StorageName,
		},
	} // invalid request because repo is empty
	invalidRepoCleanup := createInvalidRepo(t, filepath.Join(testhelper.GitlabTestStoragePath(), invalidReq.Repository.RelativePath))
	defer invalidRepoCleanup()

	_, err = makeInfoRefsUploadPackRequest(ctx, t, serverSocketPath, config.Config.Auth.Token, invalidReq)
	testhelper.RequireGrpcError(t, err, codes.Internal)
	testhelper.AssertPathNotExists(t, pathToCachedResponse(t, ctx, invalidReq))

	// if an error occurs while putting stream, it should not interrupt
	// request from being served
	happened := false

	mockInfoRefCache := newInfoRefCache(mockStreamer{
		streamer: cache.NewStreamDB(cache.NewLeaseKeyer(config.NewLocator(config.Config))),
		putStream: func(context.Context, *gitalypb.Repository, proto.Message, io.Reader) error {
			happened = true
			return errors.New("oopsie")
		},
	})

	stop()
	serverSocketPath, stop = runSmartHTTPServer(t, config.Config, withInfoRefCache(mockInfoRefCache))
	defer stop()

	invalidateCacheForRepo()
	assertNormalResponse()
	require.True(t, happened)
}

func withInfoRefCache(cache infoRefCache) ServerOpt {
	return func(s *server) {
		s.infoRefCache = cache
	}
}

func createInvalidRepo(t testing.TB, repoDir string) func() {
	for _, subDir := range []string{"objects", "refs", "HEAD"} {
		require.NoError(t, os.MkdirAll(filepath.Join(repoDir, subDir), 0755))
	}
	return func() { require.NoError(t, os.RemoveAll(repoDir)) }
}

func replaceCachedResponse(t testing.TB, ctx context.Context, req *gitalypb.InfoRefsRequest, newContents string) {
	path := pathToCachedResponse(t, ctx, req)
	require.NoError(t, ioutil.WriteFile(path, []byte(newContents), 0644))
}

func clearCache(t testing.TB) {
	for _, storage := range config.Config.Storages {
		require.NoError(t, os.RemoveAll(tempdir.CacheDir(storage)))
	}
}

func setInfoRefsUploadPackMethod(ctx context.Context) context.Context {
	return testhelper.SetCtxGrpcMethod(ctx, "/gitaly.SmartHTTPService/InfoRefsUploadPack")
}

func pathToCachedResponse(t testing.TB, ctx context.Context, req *gitalypb.InfoRefsRequest) string {
	ctx = setInfoRefsUploadPackMethod(ctx)
	path, err := cache.NewLeaseKeyer(config.NewLocator(config.Config)).KeyPath(ctx, req.GetRepository(), req)
	require.NoError(t, err)
	return path
}
