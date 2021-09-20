package objectpool

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/backchannel"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/hooks"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testassert"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"gitlab.com/gitlab-org/labkit/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestFetchIntoObjectPool_Success(t *testing.T) {
	cfg, repo, repoPath, locator, client := setup(t)

	ctx, cancel := testhelper.Context()
	defer cancel()

	repoCommit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch(t.Name()))

	pool := initObjectPool(t, cfg, cfg.Storages[0])

	req := &gitalypb.FetchIntoObjectPoolRequest{
		ObjectPool: pool.ToProto(),
		Origin:     repo,
		Repack:     true,
	}

	_, err := client.FetchIntoObjectPool(ctx, req)
	require.NoError(t, err)

	require.True(t, pool.IsValid(), "ensure underlying repository is valid")

	// No problems
	gittest.Exec(t, cfg, "-C", pool.FullPath(), "fsck")

	packFiles, err := filepath.Glob(filepath.Join(pool.FullPath(), "objects", "pack", "pack-*.pack"))
	require.NoError(t, err)
	require.Len(t, packFiles, 1, "ensure commits got packed")

	packContents := gittest.Exec(t, cfg, "-C", pool.FullPath(), "verify-pack", "-v", packFiles[0])
	require.Contains(t, string(packContents), repoCommit)

	_, err = client.FetchIntoObjectPool(ctx, req)
	require.NoError(t, err, "calling FetchIntoObjectPool twice should be OK")
	require.True(t, pool.IsValid(), "ensure that pool is valid")

	// Simulate a broken ref
	poolPath, err := locator.GetRepoPath(pool)
	require.NoError(t, err)
	brokenRef := filepath.Join(poolPath, "refs", "heads", "broken")
	require.NoError(t, os.MkdirAll(filepath.Dir(brokenRef), 0o755))
	require.NoError(t, ioutil.WriteFile(brokenRef, []byte{}, 0o777))

	oldTime := time.Now().Add(-25 * time.Hour)
	require.NoError(t, os.Chtimes(brokenRef, oldTime, oldTime))

	_, err = client.FetchIntoObjectPool(ctx, req)
	require.NoError(t, err)

	_, err = os.Stat(brokenRef)
	require.Error(t, err, "Expected refs/heads/broken to be deleted")
}

func TestFetchIntoObjectPool_hooks(t *testing.T) {
	cfg, repo, _, _, client := setup(t)

	ctx, cancel := testhelper.Context()
	defer cancel()

	pool := initObjectPool(t, cfg, cfg.Storages[0])

	hookDir := testhelper.TempDir(t)

	defer func(oldValue string) {
		hooks.Override = oldValue
	}(hooks.Override)
	hooks.Override = hookDir

	// Set up a custom reference-transaction hook which simply exits failure. This asserts that
	// the RPC doesn't invoke any reference-transaction.
	require.NoError(t, ioutil.WriteFile(filepath.Join(hookDir, "reference-transaction"), []byte("#!/bin/sh\nexit 1\n"), 0o777))

	req := &gitalypb.FetchIntoObjectPoolRequest{
		ObjectPool: pool.ToProto(),
		Origin:     repo,
		Repack:     true,
	}

	_, err := client.FetchIntoObjectPool(ctx, req)
	testassert.GrpcEqualErr(t, status.Error(codes.Internal, "fetch into object pool: exit status 128, stderr: \"fatal: ref updates aborted by hook\\n\""), err)
}

func TestFetchIntoObjectPool_CollectLogStatistics(t *testing.T) {
	cfg, repo, _ := testcfg.BuildWithRepo(t)

	testhelper.BuildGitalyHooks(t, cfg)

	locator := config.NewLocator(cfg)
	logBuffer := &bytes.Buffer{}
	logger := &logrus.Logger{Out: logBuffer, Formatter: &logrus.JSONFormatter{}, Level: logrus.InfoLevel}
	serverSocketPath := runObjectPoolServer(t, cfg, locator, logger)

	conn, err := grpc.Dial(serverSocketPath, grpc.WithInsecure())
	require.NoError(t, err)
	t.Cleanup(func() { testhelper.MustClose(t, conn) })
	client := gitalypb.NewObjectPoolServiceClient(conn)

	ctx, cancel := testhelper.Context()
	defer cancel()
	ctx = ctxlogrus.ToContext(ctx, log.WithField("test", "logging"))

	pool := initObjectPool(t, cfg, cfg.Storages[0])

	req := &gitalypb.FetchIntoObjectPoolRequest{
		ObjectPool: pool.ToProto(),
		Origin:     repo,
		Repack:     true,
	}

	_, err = client.FetchIntoObjectPool(ctx, req)
	require.NoError(t, err)

	msgs := strings.Split(logBuffer.String(), "\n")
	const key = "count_objects"
	for _, msg := range msgs {
		if strings.Contains(msg, key) {
			var out map[string]interface{}
			require.NoError(t, json.NewDecoder(strings.NewReader(msg)).Decode(&out))
			require.Contains(t, out, key, "there is no any information about statistics")
			countObjects := out[key].(map[string]interface{})
			assert.Contains(t, countObjects, "count")
			return
		}
	}
	require.FailNow(t, "no info about statistics")
}

func TestFetchIntoObjectPool_Failure(t *testing.T) {
	cfgBuilder := testcfg.NewGitalyCfgBuilder()
	cfg, repos := cfgBuilder.BuildWithRepoAt(t, t.Name())

	locator := config.NewLocator(cfg)
	gitCmdFactory := git.NewExecCommandFactory(cfg)
	server := NewServer(
		cfg,
		locator,
		gitCmdFactory,
		catfile.NewCache(cfg),
		transaction.NewManager(cfg, backchannel.NewRegistry()),
	)

	ctx, cancel := testhelper.Context()
	defer cancel()

	pool := initObjectPool(t, cfg, cfg.Storages[0])

	poolWithDifferentStorage := pool.ToProto()
	poolWithDifferentStorage.Repository.StorageName = "some other storage"

	testCases := []struct {
		description string
		request     *gitalypb.FetchIntoObjectPoolRequest
		code        codes.Code
		errMsg      string
	}{
		{
			description: "empty origin",
			request: &gitalypb.FetchIntoObjectPoolRequest{
				ObjectPool: pool.ToProto(),
			},
			code:   codes.InvalidArgument,
			errMsg: "origin is empty",
		},
		{
			description: "empty pool",
			request: &gitalypb.FetchIntoObjectPoolRequest{
				Origin: repos[0],
			},
			code:   codes.InvalidArgument,
			errMsg: "object pool is empty",
		},
		{
			description: "origin and pool do not share the same storage",
			request: &gitalypb.FetchIntoObjectPoolRequest{
				Origin:     repos[0],
				ObjectPool: poolWithDifferentStorage,
			},
			code:   codes.InvalidArgument,
			errMsg: "origin has different storage than object pool",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			_, err := server.FetchIntoObjectPool(ctx, tc.request)
			require.Error(t, err)
			testhelper.RequireGrpcError(t, err, tc.code)
			assert.Contains(t, err.Error(), tc.errMsg)
		})
	}
}
