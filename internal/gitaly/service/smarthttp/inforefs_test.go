package smarthttp

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/backchannel"
	"gitlab.com/gitlab-org/gitaly/v14/internal/cache"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/housekeeping"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/objectpool"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/stats"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/v14/streamio"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
)

func TestSuccessfulInfoRefsUploadPack(t *testing.T) {
	t.Parallel()

	cfg := testcfg.Build(t)

	cfg.SocketPath = runSmartHTTPServer(t, cfg)

	ctx := testhelper.Context(t)
	repo, _ := gittest.CreateRepository(ctx, t, cfg, gittest.CreateRepositoryConfig{
		Seed: gittest.SeedGitLabTest,
	})

	rpcRequest := &gitalypb.InfoRefsRequest{Repository: repo}
	response, err := makeInfoRefsUploadPackRequest(ctx, t, cfg.SocketPath, cfg.Auth.Token, rpcRequest)
	require.NoError(t, err)
	assertGitRefAdvertisement(t, "InfoRefsUploadPack", string(response), "001e# service=git-upload-pack", "0000", []string{
		"003ef4e6814c3e4e7a0de82a9e7cd20c626cc963a2f8 refs/tags/v1.0.0",
		"00416f6d7e7ed97bb5f0054f2b1df789b39ca89b6ff9 refs/tags/v1.0.0^{}",
	})
}

func TestInfoRefsUploadPack_repositoryDoesntExist(t *testing.T) {
	t.Parallel()

	cfg := testcfg.Build(t)

	serverSocketPath := runSmartHTTPServer(t, cfg)

	rpcRequest := &gitalypb.InfoRefsRequest{Repository: &gitalypb.Repository{
		StorageName:  cfg.Storages[0].Name,
		RelativePath: "doesnt/exist",
	}}
	ctx := testhelper.Context(t)

	_, err := makeInfoRefsUploadPackRequest(ctx, t, serverSocketPath, cfg.Auth.Token, rpcRequest)

	expectedErr := helper.ErrNotFoundf(`GetRepoPath: not a git repository: "` + cfg.Storages[0].Path + `/doesnt/exist"`)
	if testhelper.IsPraefectEnabled() {
		expectedErr = helper.ErrNotFoundf(`accessor call: route repository accessor: consistent storages: repository "default"/"doesnt/exist" not found`)
	}

	testhelper.RequireGrpcError(t, expectedErr, err)
}

func TestSuccessfulInfoRefsUploadWithPartialClone(t *testing.T) {
	t.Parallel()

	cfg := testcfg.Build(t)

	cfg.SocketPath = runSmartHTTPServer(t, cfg)
	ctx := testhelper.Context(t)

	repo, _ := gittest.CreateRepository(ctx, t, cfg, gittest.CreateRepositoryConfig{
		Seed: gittest.SeedGitLabTest,
	})

	request := &gitalypb.InfoRefsRequest{
		Repository: repo,
	}

	partialResponse, err := makeInfoRefsUploadPackRequest(ctx, t, cfg.SocketPath, cfg.Auth.Token, request)
	require.NoError(t, err)
	partialRefs := stats.ReferenceDiscovery{}
	err = partialRefs.Parse(bytes.NewReader(partialResponse))
	require.NoError(t, err)

	for _, c := range []string{"allow-tip-sha1-in-want", "allow-reachable-sha1-in-want", "filter"} {
		require.Contains(t, partialRefs.Caps, c)
	}
}

func TestSuccessfulInfoRefsUploadPackWithGitConfigOptions(t *testing.T) {
	t.Parallel()

	cfg := testcfg.Build(t)
	cfg.SocketPath = runSmartHTTPServer(t, cfg)

	ctx := testhelper.Context(t)
	repo, _ := gittest.CreateRepository(ctx, t, cfg, gittest.CreateRepositoryConfig{
		Seed: gittest.SeedGitLabTest,
	})

	// transfer.hideRefs=refs will hide every ref that info-refs would normally
	// output, allowing us to test that the custom configuration is respected
	rpcRequest := &gitalypb.InfoRefsRequest{
		Repository:       repo,
		GitConfigOptions: []string{"transfer.hideRefs=refs"},
	}
	response, err := makeInfoRefsUploadPackRequest(ctx, t, cfg.SocketPath, cfg.Auth.Token, rpcRequest)
	require.NoError(t, err)
	assertGitRefAdvertisement(t, "InfoRefsUploadPack", string(response), "001e# service=git-upload-pack", "0000", []string{})
}

func TestSuccessfulInfoRefsUploadPackWithGitProtocol(t *testing.T) {
	t.Parallel()

	cfg := testcfg.Build(t)
	ctx := testhelper.Context(t)

	gitCmdFactory, readProtocol := gittest.NewProtocolDetectingCommandFactory(ctx, t, cfg)

	server := startSmartHTTPServerWithOptions(t, cfg, nil, []testserver.GitalyServerOpt{
		testserver.WithGitCommandFactory(gitCmdFactory),
	})
	cfg.SocketPath = server.Address()

	repo, _ := gittest.CreateRepository(ctx, t, cfg, gittest.CreateRepositoryConfig{
		Seed: gittest.SeedGitLabTest,
	})

	rpcRequest := &gitalypb.InfoRefsRequest{
		Repository:  repo,
		GitProtocol: git.ProtocolV2,
	}

	client, conn := newSmartHTTPClient(t, server.Address(), cfg.Auth.Token)
	defer testhelper.MustClose(t, conn)

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

	response, err := io.ReadAll(streamio.NewReader(func() ([]byte, error) {
		resp, err := c.Recv()
		return resp.GetData(), err
	}))

	return response, err
}

func TestSuccessfulInfoRefsReceivePack(t *testing.T) {
	t.Parallel()

	cfg := testcfg.Build(t)

	cfg.SocketPath = runSmartHTTPServer(t, cfg)

	ctx := testhelper.Context(t)
	repo, _ := gittest.CreateRepository(ctx, t, cfg, gittest.CreateRepositoryConfig{
		Seed: gittest.SeedGitLabTest,
	})

	rpcRequest := &gitalypb.InfoRefsRequest{Repository: repo}

	response, err := makeInfoRefsReceivePackRequest(ctx, t, cfg.SocketPath, cfg.Auth.Token, rpcRequest)
	require.NoError(t, err)

	assertGitRefAdvertisement(t, "InfoRefsReceivePack", string(response), "001f# service=git-receive-pack", "0000", []string{
		"003ef4e6814c3e4e7a0de82a9e7cd20c626cc963a2f8 refs/tags/v1.0.0",
		"003e8a2a6eb295bb170b34c24c76c49ed0e9b2eaf34b refs/tags/v1.1.0",
	})
}

func TestObjectPoolRefAdvertisementHiding(t *testing.T) {
	t.Parallel()

	cfg := testcfg.Build(t)

	testcfg.BuildGitalyHooks(t, cfg)

	cfg.SocketPath = runSmartHTTPServer(t, cfg)
	ctx := testhelper.Context(t)

	repoProto, _ := gittest.CreateRepository(ctx, t, cfg, gittest.CreateRepositoryConfig{
		Seed: gittest.SeedGitLabTest,
	})
	repo := localrepo.NewTestRepo(t, cfg, repoProto)
	txManager := transaction.NewManager(cfg, backchannel.NewRegistry())

	pool, err := objectpool.NewObjectPool(
		config.NewLocator(cfg),
		gittest.NewCommandFactory(t, cfg),
		nil,
		txManager,
		housekeeping.NewManager(txManager),
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

	rpcRequest := &gitalypb.InfoRefsRequest{Repository: repoProto}

	response, err := makeInfoRefsReceivePackRequest(ctx, t, cfg.SocketPath, cfg.Auth.Token, rpcRequest)
	require.NoError(t, err)
	require.NotContains(t, string(response), commitID+" .have")
}

func TestFailureRepoNotFoundInfoRefsReceivePack(t *testing.T) {
	t.Parallel()

	cfg := testcfg.Build(t)

	serverSocketPath := runSmartHTTPServer(t, cfg)

	repo := &gitalypb.Repository{StorageName: cfg.Storages[0].Name, RelativePath: "testdata/scratch/another_repo"}
	rpcRequest := &gitalypb.InfoRefsRequest{Repository: repo}
	ctx := testhelper.Context(t)
	_, err := makeInfoRefsReceivePackRequest(ctx, t, serverSocketPath, cfg.Auth.Token, rpcRequest)

	expectedErr := helper.ErrNotFoundf(`GetRepoPath: not a git repository: "` + cfg.Storages[0].Path + "/" + repo.RelativePath + `"`)
	if testhelper.IsPraefectEnabled() {
		expectedErr = helper.ErrNotFoundf(`accessor call: route repository accessor: consistent storages: repository "default"/"testdata/scratch/another_repo" not found`)
	}

	testhelper.RequireGrpcError(t, expectedErr, err)
}

func TestFailureRepoNotSetInfoRefsReceivePack(t *testing.T) {
	t.Parallel()

	cfg := testcfg.Build(t)

	serverSocketPath := runSmartHTTPServer(t, cfg)

	rpcRequest := &gitalypb.InfoRefsRequest{}
	ctx := testhelper.Context(t)
	_, err := makeInfoRefsReceivePackRequest(ctx, t, serverSocketPath, cfg.Auth.Token, rpcRequest)
	testhelper.RequireGrpcCode(t, err, codes.InvalidArgument)
}

func makeInfoRefsReceivePackRequest(ctx context.Context, t *testing.T, serverSocketPath, token string, rpcRequest *gitalypb.InfoRefsRequest) ([]byte, error) {
	t.Helper()

	client, conn := newSmartHTTPClient(t, serverSocketPath, token)
	defer conn.Close()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	c, err := client.InfoRefsReceivePack(ctx, rpcRequest)
	require.NoError(t, err)

	response, err := io.ReadAll(streamio.NewReader(func() ([]byte, error) {
		resp, err := c.Recv()
		return resp.GetData(), err
	}))

	return response, err
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
	cache.Streamer
	putStream func(context.Context, *gitalypb.Repository, proto.Message, io.Reader) error
}

func (ms *mockStreamer) PutStream(ctx context.Context, repo *gitalypb.Repository, req proto.Message, src io.Reader) error {
	if ms.putStream != nil {
		return ms.putStream(ctx, repo, req, src)
	}
	return ms.Streamer.PutStream(ctx, repo, req, src)
}

func TestCacheInfoRefsUploadPack(t *testing.T) {
	t.Parallel()

	cfg := testcfg.Build(t)

	locator := config.NewLocator(cfg)
	cache := cache.New(cfg, locator)

	streamer := mockStreamer{
		Streamer: cache,
	}
	mockInfoRefCache := newInfoRefCache(&streamer)

	gitalyServer := startSmartHTTPServer(t, cfg, withInfoRefCache(mockInfoRefCache))
	cfg.SocketPath = gitalyServer.Address()

	ctx := testhelper.Context(t)
	repo, _ := gittest.CreateRepository(ctx, t, cfg, gittest.CreateRepositoryConfig{
		Seed: gittest.SeedGitLabTest,
	})

	rpcRequest := &gitalypb.InfoRefsRequest{Repository: repo}

	// The key computed for the cache entry takes into account all feature flags. Because
	// Praefect explicitly injects all unset feature flags, the key is thus differend depending
	// on whether Praefect is in use or not. We thus manually inject all feature flags here such
	// that they're forced to the same state.
	for _, ff := range featureflag.All {
		ctx = featureflag.OutgoingCtxWithFeatureFlag(ctx, ff, true)
		ctx = featureflag.IncomingCtxWithFeatureFlag(ctx, ff, true)
	}

	assertNormalResponse := func(addr string) {
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
	rewrittenRequest := &gitalypb.InfoRefsRequest{Repository: gittest.RewrittenRepository(ctx, t, cfg, repo)}
	require.FileExists(t, pathToCachedResponse(t, ctx, cache, rewrittenRequest))

	replacedContents := []string{
		"first line",
		"meow meow meow meow",
		"woof woof woof woof",
		"last line",
	}

	// replace cached response file to prove the info-ref uses the cache
	replaceCachedResponse(t, ctx, cache, rewrittenRequest, strings.Join(replacedContents, "\n"))
	response, err := makeInfoRefsUploadPackRequest(ctx, t, gitalyServer.Address(), cfg.Auth.Token, rpcRequest)
	require.NoError(t, err)
	assertGitRefAdvertisement(t, "InfoRefsUploadPack", string(response),
		replacedContents[0], replacedContents[3], replacedContents[1:3],
	)

	invalidateCacheForRepo := func() {
		ender, err := cache.StartLease(rewrittenRequest.Repository)
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
	testhelper.RequireGrpcCode(t, err, codes.NotFound)
	require.NoFileExists(t, pathToCachedResponse(t, ctx, cache, invalidReq))

	// if an error occurs while putting stream, it should not interrupt
	// request from being served
	happened := false
	streamer.putStream = func(context.Context, *gitalypb.Repository, proto.Message, io.Reader) error {
		happened = true
		return errors.New("oopsie")
	}

	invalidateCacheForRepo()
	assertNormalResponse(gitalyServer.Address())
	require.True(t, happened)
}

func withInfoRefCache(cache infoRefCache) ServerOpt {
	return func(s *server) {
		s.infoRefCache = cache
	}
}

func createInvalidRepo(t testing.TB, repoDir string) func() {
	for _, subDir := range []string{"objects", "refs", "HEAD"} {
		require.NoError(t, os.MkdirAll(filepath.Join(repoDir, subDir), 0o755))
	}
	return func() { require.NoError(t, os.RemoveAll(repoDir)) }
}

func replaceCachedResponse(t testing.TB, ctx context.Context, cache *cache.DiskCache, req *gitalypb.InfoRefsRequest, newContents string) {
	path := pathToCachedResponse(t, ctx, cache, req)
	require.NoError(t, os.WriteFile(path, []byte(newContents), 0o644))
}

func setInfoRefsUploadPackMethod(ctx context.Context) context.Context {
	return testhelper.SetCtxGrpcMethod(ctx, "/gitaly.SmartHTTPService/InfoRefsUploadPack")
}

func pathToCachedResponse(t testing.TB, ctx context.Context, cache *cache.DiskCache, req *gitalypb.InfoRefsRequest) string {
	ctx = setInfoRefsUploadPackMethod(ctx)
	path, err := cache.KeyPath(ctx, req.GetRepository(), req)
	require.NoError(t, err)
	return path
}
