package smarthttp

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/cache"
	"gitlab.com/gitlab-org/gitaly/internal/config"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/objectpool"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/internal/tempdir"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/streamio"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
)

func TestSuccessfulInfoRefsUploadPack(t *testing.T) {
	stop, serverSocketPath := runSmartHTTPServer(t)
	defer stop()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	rpcRequest := &gitalypb.InfoRefsRequest{Repository: testRepo}

	response, err := makeInfoRefsUploadPackRequest(context.Background(), t, serverSocketPath, rpcRequest)
	require.NoError(t, err)
	assertGitRefAdvertisement(t, "InfoRefsUploadPack", string(response), "001e# service=git-upload-pack", "0000", []string{
		"003ef4e6814c3e4e7a0de82a9e7cd20c626cc963a2f8 refs/tags/v1.0.0",
		"00416f6d7e7ed97bb5f0054f2b1df789b39ca89b6ff9 refs/tags/v1.0.0^{}",
	})
}

func TestSuccessfulInfoRefsUploadPackWithGitConfigOptions(t *testing.T) {
	stop, serverSocketPath := runSmartHTTPServer(t)
	defer stop()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	// transfer.hideRefs=refs will hide every ref that info-refs would normally
	// output, allowing us to test that the custom configuration is respected
	rpcRequest := &gitalypb.InfoRefsRequest{
		Repository:       testRepo,
		GitConfigOptions: []string{"transfer.hideRefs=refs"},
	}

	response, err := makeInfoRefsUploadPackRequest(context.Background(), t, serverSocketPath, rpcRequest)
	require.NoError(t, err)
	assertGitRefAdvertisement(t, "InfoRefsUploadPack", string(response), "001e# service=git-upload-pack", "0000", []string{})
}

func TestSuccessfulInfoRefsUploadPackWithGitProtocol(t *testing.T) {
	restore := testhelper.EnableGitProtocolV2Support()
	defer restore()

	stop, serverSocketPath := runSmartHTTPServer(t)
	defer stop()

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

func makeInfoRefsUploadPackRequest(ctx context.Context, t *testing.T, serverSocketPath string, rpcRequest *gitalypb.InfoRefsRequest) ([]byte, error) {
	client, conn := newSmartHTTPClient(t, serverSocketPath)
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
	stop, serverSocketPath := runSmartHTTPServer(t)
	defer stop()

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

func TestObjectPoolRefAdvertisementHiding(t *testing.T) {
	stop, serverSocketPath := runSmartHTTPServer(t)
	defer stop()

	client, conn := newSmartHTTPClient(t, serverSocketPath)
	defer conn.Close()

	repo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	ctx, cancel := testhelper.Context()
	defer cancel()

	pool, err := objectpool.NewObjectPool(repo.GetStorageName(), testhelper.NewTestObjectPoolName(t))
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
	stop, serverSocketPath := runSmartHTTPServer(t)
	defer stop()

	client, conn := newSmartHTTPClient(t, serverSocketPath)
	defer conn.Close()
	repo := &gitalypb.Repository{StorageName: "default", RelativePath: "testdata/scratch/another_repo"}
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
	stop, serverSocketPath := runSmartHTTPServer(t)
	defer stop()

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

func TestCacheInfoRefsUploadPack(t *testing.T) {
	clearCache(t)

	stop, serverSocketPath := runSmartHTTPServer(t)
	defer stop()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	rpcRequest := &gitalypb.InfoRefsRequest{Repository: testRepo}

	ctx := context.Background()

	assertNormalResponse := func() {
		response, err := makeInfoRefsUploadPackRequest(ctx, t, serverSocketPath, rpcRequest)
		require.NoError(t, err)

		assertGitRefAdvertisement(t, "InfoRefsUploadPack", string(response),
			"001e# service=git-upload-pack", "0000",
			[]string{
				"003ef4e6814c3e4e7a0de82a9e7cd20c626cc963a2f8 refs/tags/v1.0.0",
				"00416f6d7e7ed97bb5f0054f2b1df789b39ca89b6ff9 refs/tags/v1.0.0^{}",
			},
		)
	}

	// if feature-flag is disabled, we should not find a cached response
	assertNormalResponse()
	testhelper.AssertPathNotExists(t, pathToCachedResponse(t, rpcRequest))

	// enable feature flag, and we expect to find the cached response
	ctx = enableCacheFeatureFlag(ctx)
	assertNormalResponse()
	require.FileExists(t, pathToCachedResponse(t, rpcRequest))

	replacedContents := []string{
		"first line",
		"meow meow meow meow",
		"woof woof woof woof",
		"last line",
	}

	// replace cached response file to prove the info-ref uses the cache
	replaceCachedResponse(t, rpcRequest, strings.Join(replacedContents, "\n"))
	response, err := makeInfoRefsUploadPackRequest(ctx, t, serverSocketPath, rpcRequest)
	require.NoError(t, err)
	assertGitRefAdvertisement(t, "InfoRefsUploadPack", string(response),
		replacedContents[0], replacedContents[3], replacedContents[1:3],
	)

	// disable feature-flag to show replaced response no longer used
	ctx = context.Background()
	assertNormalResponse()

	// invalidate cache for repository
	ender, err := cache.LeaseKeyer{}.StartLease(rpcRequest.Repository)
	require.NoError(t, err)
	require.NoError(t, ender.EndLease(setInfoRefsUploadPackMethod(context.Background())))

	// replaced cache response is no longer valid
	ctx = enableCacheFeatureFlag(ctx)
	assertNormalResponse()

	// failed requests should not cache response
	invalidReq := &gitalypb.InfoRefsRequest{
		Repository: &gitalypb.Repository{
			RelativePath: "fake_repo",
			StorageName:  testRepo.StorageName,
		},
	} // invalid request because repo is empty
	invalidRepoCleanup := createInvalidRepo(t, invalidReq.Repository)
	defer invalidRepoCleanup()

	_, err = makeInfoRefsUploadPackRequest(ctx, t, serverSocketPath, invalidReq)
	testhelper.RequireGrpcError(t, err, codes.Internal)
	testhelper.AssertPathNotExists(t, pathToCachedResponse(t, invalidReq))
}

func createInvalidRepo(t testing.TB, repo *gitalypb.Repository) func() {
	repoDir, err := helper.GetPath(repo)
	require.NoError(t, err)
	for _, subDir := range []string{"objects", "refs", "HEAD"} {
		require.NoError(t, os.MkdirAll(filepath.Join(repoDir, subDir), 0755))
	}
	return func() { require.NoError(t, os.RemoveAll(repoDir)) }
}

func replaceCachedResponse(t testing.TB, req *gitalypb.InfoRefsRequest, newContents string) {
	path := pathToCachedResponse(t, req)
	require.NoError(t, ioutil.WriteFile(path, []byte(newContents), 0644))
}

func enableCacheFeatureFlag(ctx context.Context) context.Context {
	return metadata.NewOutgoingContext(ctx, metadata.New(map[string]string{
		featureflag.HeaderKey(UploadPackCacheFeatureFlagKey): "true",
		featureflag.HeaderKey(featureflag.CacheInvalidator):  "true",
	}))
}

func clearCache(t testing.TB) {
	for _, storage := range config.Config.Storages {
		require.NoError(t, os.RemoveAll(tempdir.CacheDir(storage)))
	}
}

func setInfoRefsUploadPackMethod(ctx context.Context) context.Context {
	return testhelper.SetCtxGrpcMethod(ctx, "/gitaly.SmartHTTPService/InfoRefsUploadPack")
}

func pathToCachedResponse(t testing.TB, req *gitalypb.InfoRefsRequest) string {
	ctx := setInfoRefsUploadPackMethod(context.Background())
	path, err := cache.LeaseKeyer{}.KeyPath(ctx, req.GetRepository(), req)
	require.NoError(t, err)
	return path
}
