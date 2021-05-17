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
	"gitlab.com/gitlab-org/gitaly/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/internal/git/objectpool"
	"gitlab.com/gitlab-org/gitaly/internal/git/stats"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/streamio"
	"google.golang.org/grpc/codes"
)

func TestSuccessfulInfoRefsUploadPack(t *testing.T) {
	cfg, repo, _ := testcfg.BuildWithRepo(t)

	serverSocketPath := runSmartHTTPServer(t, cfg)

	rpcRequest := &gitalypb.InfoRefsRequest{Repository: repo}

	ctx, cancel := testhelper.Context()
	defer cancel()

	response, err := makeInfoRefsUploadPackRequest(ctx, t, serverSocketPath, cfg.Auth.Token, rpcRequest)
	require.NoError(t, err)
	assertGitRefAdvertisement(t, "InfoRefsUploadPack", string(response), "001e# service=git-upload-pack", "0000", []string{
		"003ef4e6814c3e4e7a0de82a9e7cd20c626cc963a2f8 refs/tags/v1.0.0",
		"00416f6d7e7ed97bb5f0054f2b1df789b39ca89b6ff9 refs/tags/v1.0.0^{}",
	})
}

func TestSuccessfulInfoRefsUploadWithPartialClone(t *testing.T) {
	cfg, repo, _ := testcfg.BuildWithRepo(t)

	serverSocketPath := runSmartHTTPServer(t, cfg)

	ctx, cancel := testhelper.Context()
	defer cancel()

	request := &gitalypb.InfoRefsRequest{
		Repository: repo,
	}

	partialResponse, err := makeInfoRefsUploadPackRequest(ctx, t, serverSocketPath, cfg.Auth.Token, request)
	require.NoError(t, err)
	partialRefs := stats.Get{}
	err = partialRefs.Parse(bytes.NewReader(partialResponse))
	require.NoError(t, err)

	for _, c := range []string{"allow-tip-sha1-in-want", "allow-reachable-sha1-in-want", "filter"} {
		require.Contains(t, partialRefs.Caps, c)
	}
}

func TestSuccessfulInfoRefsUploadPackWithGitConfigOptions(t *testing.T) {
	cfg, repo, _ := testcfg.BuildWithRepo(t)

	serverSocketPath := runSmartHTTPServer(t, cfg)

	// transfer.hideRefs=refs will hide every ref that info-refs would normally
	// output, allowing us to test that the custom configuration is respected
	rpcRequest := &gitalypb.InfoRefsRequest{
		Repository:       repo,
		GitConfigOptions: []string{"transfer.hideRefs=refs"},
	}

	ctx, cancel := testhelper.Context()
	defer cancel()

	response, err := makeInfoRefsUploadPackRequest(ctx, t, serverSocketPath, cfg.Auth.Token, rpcRequest)
	require.NoError(t, err)
	assertGitRefAdvertisement(t, "InfoRefsUploadPack", string(response), "001e# service=git-upload-pack", "0000", []string{})
}

func TestSuccessfulInfoRefsUploadPackWithGitProtocol(t *testing.T) {
	cfg, repo, _ := testcfg.BuildWithRepo(t)

	readProtocol, cfg := gittest.EnableGitProtocolV2Support(t, cfg)

	serverSocketPath := runSmartHTTPServer(t, cfg)

	rpcRequest := &gitalypb.InfoRefsRequest{
		Repository:  repo,
		GitProtocol: git.ProtocolV2,
	}

	client, _ := newSmartHTTPClient(t, serverSocketPath, cfg.Auth.Token)
	ctx, cancel := testhelper.Context()
	defer cancel()

	c, err := client.InfoRefsUploadPack(ctx, rpcRequest)
	require.NoError(t, err)

	for {
		if _, err := c.Recv(); err != nil {
			require.Equal(t, io.EOF, err)
			break
		}
	}

	envData := readProtocol()
	require.Contains(t, envData, fmt.Sprintf("GIT_PROTOCOL=%s\n", git.ProtocolV2))
}

func makeInfoRefsUploadPackRequest(ctx context.Context, t *testing.T, serverSocketPath, token string, rpcRequest *gitalypb.InfoRefsRequest) ([]byte, error) {
	t.Helper()

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
	cfg, repo, _ := testcfg.BuildWithRepo(t)

	serverSocketPath := runSmartHTTPServer(t, cfg)

	client, conn := newSmartHTTPClient(t, serverSocketPath, cfg.Auth.Token)
	defer conn.Close()

	rpcRequest := &gitalypb.InfoRefsRequest{Repository: repo}

	ctx, cancel := testhelper.Context()
	defer cancel()
	c, err := client.InfoRefsReceivePack(ctx, rpcRequest)
	require.NoError(t, err)

	response, err := ioutil.ReadAll(streamio.NewReader(func() ([]byte, error) {
		resp, err := c.Recv()
		return resp.GetData(), err
	}))
	require.NoError(t, err)

	assertGitRefAdvertisement(t, "InfoRefsReceivePack", string(response), "001f# service=git-receive-pack", "0000", []string{
		"003ef4e6814c3e4e7a0de82a9e7cd20c626cc963a2f8 refs/tags/v1.0.0",
		"003e8a2a6eb295bb170b34c24c76c49ed0e9b2eaf34b refs/tags/v1.1.0",
	})
}

func TestObjectPoolRefAdvertisementHiding(t *testing.T) {
	cfg, repo, _ := testcfg.BuildWithRepo(t)

	testhelper.ConfigureGitalyHooksBin(t, cfg)

	serverSocketPath := runSmartHTTPServer(t, cfg)

	client, conn := newSmartHTTPClient(t, serverSocketPath, cfg.Auth.Token)
	defer conn.Close()

	ctx, cancel := testhelper.Context()
	defer cancel()

	pool, err := objectpool.NewObjectPool(
		cfg,
		config.NewLocator(cfg),
		git.NewExecCommandFactory(cfg),
		nil,
		repo.GetStorageName(),
		gittest.NewObjectPoolName(t),
	)
	require.NoError(t, err)

	require.NoError(t, pool.Create(ctx, repo))
	defer func() {
		require.NoError(t, pool.Remove(ctx))
	}()

	commitID := gittest.WriteCommit(t, cfg, pool.FullPath(), gittest.WithBranch(t.Name()))

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
	cfg := testcfg.Build(t)

	serverSocketPath := runSmartHTTPServer(t, cfg)

	client, conn := newSmartHTTPClient(t, serverSocketPath, cfg.Auth.Token)
	defer conn.Close()
	repo := &gitalypb.Repository{StorageName: cfg.Storages[0].Name, RelativePath: "testdata/scratch/another_repo"}
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
	cfg := testcfg.Build(t)

	serverSocketPath := runSmartHTTPServer(t, cfg)

	client, conn := newSmartHTTPClient(t, serverSocketPath, cfg.Auth.Token)
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
	cfg, repo, _ := testcfg.BuildWithRepo(t)

	locator := config.NewLocator(cfg)
	cache := cache.New(cfg, locator)

	gitalyServer := startSmartHTTPServer(t, cfg, withInfoRefCache(newInfoRefCache(cache)))

	rpcRequest := &gitalypb.InfoRefsRequest{Repository: repo}

	ctx, cancel := testhelper.Context()
	defer cancel()

	assertNormalResponse := func(addr string) {
		ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()

		response, err := makeInfoRefsUploadPackRequest(ctx, t, addr, cfg.Auth.Token, rpcRequest)
		require.NoError(t, err)

		assertGitRefAdvertisement(t, "InfoRefsUploadPack", string(response),
			"001e# service=git-upload-pack", "0000",
			[]string{
				"003ef4e6814c3e4e7a0de82a9e7cd20c626cc963a2f8 refs/tags/v1.0.0",
				"00416f6d7e7ed97bb5f0054f2b1df789b39ca89b6ff9 refs/tags/v1.0.0^{}",
			},
		)
	}

	assertNormalResponse(gitalyServer.Address())
	require.FileExists(t, pathToCachedResponse(t, ctx, cache, rpcRequest))

	replacedContents := []string{
		"first line",
		"meow meow meow meow",
		"woof woof woof woof",
		"last line",
	}

	// replace cached response file to prove the info-ref uses the cache
	replaceCachedResponse(t, ctx, cache, rpcRequest, strings.Join(replacedContents, "\n"))
	response, err := makeInfoRefsUploadPackRequest(ctx, t, gitalyServer.Address(), cfg.Auth.Token, rpcRequest)
	require.NoError(t, err)
	assertGitRefAdvertisement(t, "InfoRefsUploadPack", string(response),
		replacedContents[0], replacedContents[3], replacedContents[1:3],
	)

	invalidateCacheForRepo := func() {
		ender, err := cache.StartLease(rpcRequest.Repository)
		require.NoError(t, err)
		require.NoError(t, ender.EndLease(setInfoRefsUploadPackMethod(ctx)))
	}

	invalidateCacheForRepo()

	// replaced cache response is no longer valid
	assertNormalResponse(gitalyServer.Address())

	// failed requests should not cache response
	invalidReq := &gitalypb.InfoRefsRequest{
		Repository: &gitalypb.Repository{
			RelativePath: "fake_repo",
			StorageName:  repo.StorageName,
		},
	} // invalid request because repo is empty
	invalidRepoCleanup := createInvalidRepo(t, filepath.Join(testhelper.GitlabTestStoragePath(), invalidReq.Repository.RelativePath))
	defer invalidRepoCleanup()

	_, err = makeInfoRefsUploadPackRequest(ctx, t, gitalyServer.Address(), cfg.Auth.Token, invalidReq)
	testhelper.RequireGrpcError(t, err, codes.NotFound)
	require.NoFileExists(t, pathToCachedResponse(t, ctx, cache, invalidReq))

	// if an error occurs while putting stream, it should not interrupt
	// request from being served
	happened := false

	mockInfoRefCache := newInfoRefCache(mockStreamer{
		streamer: cache,
		putStream: func(context.Context, *gitalypb.Repository, proto.Message, io.Reader) error {
			happened = true
			return errors.New("oopsie")
		},
	})

	gitalyServer.Shutdown()
	addr := runSmartHTTPServer(t, cfg, withInfoRefCache(mockInfoRefCache))

	invalidateCacheForRepo()
	assertNormalResponse(addr)
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

func replaceCachedResponse(t testing.TB, ctx context.Context, cache *cache.Cache, req *gitalypb.InfoRefsRequest, newContents string) {
	path := pathToCachedResponse(t, ctx, cache, req)
	require.NoError(t, ioutil.WriteFile(path, []byte(newContents), 0644))
}

func setInfoRefsUploadPackMethod(ctx context.Context) context.Context {
	return testhelper.SetCtxGrpcMethod(ctx, "/gitaly.SmartHTTPService/InfoRefsUploadPack")
}

func pathToCachedResponse(t testing.TB, ctx context.Context, cache *cache.Cache, req *gitalypb.InfoRefsRequest) string {
	ctx = setInfoRefsUploadPackMethod(ctx)
	path, err := cache.KeyPath(ctx, req.GetRepository(), req)
	require.NoError(t, err)
	return path
}
