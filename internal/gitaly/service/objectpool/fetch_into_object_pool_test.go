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
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/internal/git/hooks"
	"gitlab.com/gitlab-org/gitaly/internal/git/objectpool"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"gitlab.com/gitlab-org/labkit/log"
	"google.golang.org/grpc/codes"
)

func TestFetchIntoObjectPool_Success(t *testing.T) {
	cfg, repo, repoPath, locator, client, cleanup := setup(t)
	defer cleanup()

	ctx, cancel := testhelper.Context()
	defer cancel()

	repoCommit := gittest.CreateCommit(t, repoPath, t.Name(), &gittest.CreateCommitOpts{Message: t.Name()})

	pool, err := objectpool.NewObjectPool(cfg, locator, git.NewExecCommandFactory(cfg), repo.GetStorageName(), gittest.NewObjectPoolName(t))
	require.NoError(t, err)
	defer pool.Remove(ctx)

	req := &gitalypb.FetchIntoObjectPoolRequest{
		ObjectPool: pool.ToProto(),
		Origin:     repo,
		Repack:     true,
	}

	_, err = client.FetchIntoObjectPool(ctx, req)
	require.NoError(t, err)

	require.True(t, pool.IsValid(), "ensure underlying repository is valid")

	// No problems
	testhelper.MustRunCommand(t, nil, "git", "-C", pool.FullPath(), "fsck")

	packFiles, err := filepath.Glob(filepath.Join(pool.FullPath(), "objects", "pack", "pack-*.pack"))
	require.NoError(t, err)
	require.Len(t, packFiles, 1, "ensure commits got packed")

	packContents := testhelper.MustRunCommand(t, nil, "git", "-C", pool.FullPath(), "verify-pack", "-v", packFiles[0])
	require.Contains(t, string(packContents), repoCommit)

	_, err = client.FetchIntoObjectPool(ctx, req)
	require.NoError(t, err, "calling FetchIntoObjectPool twice should be OK")
	require.True(t, pool.IsValid(), "ensure that pool is valid")

	// Simulate a broken ref
	poolPath, err := locator.GetRepoPath(pool)
	require.NoError(t, err)
	brokenRef := filepath.Join(poolPath, "refs", "heads", "broken")
	err = ioutil.WriteFile(brokenRef, []byte{}, 0777)
	require.NoError(t, err)

	oldTime := time.Now().Add(-25 * time.Hour)
	require.NoError(t, os.Chtimes(brokenRef, oldTime, oldTime))

	_, err = client.FetchIntoObjectPool(ctx, req)
	require.NoError(t, err)

	_, err = os.Stat(brokenRef)
	require.Error(t, err, "Expected refs/heads/broken to be deleted")
}

func TestFetchIntoObjectPool_hooksDisabled(t *testing.T) {
	cfg, repo, _, locator, client, cleanup := setup(t)
	defer cleanup()

	ctx, cancel := testhelper.Context()
	defer cancel()

	pool, err := objectpool.NewObjectPool(cfg, locator, git.NewExecCommandFactory(cfg), repo.GetStorageName(), gittest.NewObjectPoolName(t))
	require.NoError(t, err)
	defer pool.Remove(ctx)

	hookDir, cleanup := testhelper.TempDir(t)
	defer cleanup()

	defer func(oldValue string) {
		hooks.Override = oldValue
	}(hooks.Override)
	hooks.Override = hookDir

	// Set up a custom reference-transaction hook which simply exits failure. This asserts that
	// the RPC doesn't invoke any reference-transaction.
	require.NoError(t, ioutil.WriteFile(filepath.Join(hookDir, "reference-transaction"), []byte("#!/bin/sh\nexit 1\n"), 0777))

	req := &gitalypb.FetchIntoObjectPoolRequest{
		ObjectPool: pool.ToProto(),
		Origin:     repo,
		Repack:     true,
	}

	_, err = client.FetchIntoObjectPool(ctx, req)
	require.NoError(t, err)
}

func TestFetchIntoObjectPool_CollectLogStatistics(t *testing.T) {
	logBuffer := &bytes.Buffer{}
	testhelper.NewTestLogger = func(tb testing.TB) *logrus.Logger {
		return &logrus.Logger{Out: logBuffer, Formatter: &logrus.JSONFormatter{}, Level: logrus.InfoLevel}
	}

	defer func(tl func(tb testing.TB) *logrus.Logger) {
		testhelper.NewTestLogger = tl
	}(testhelper.NewTestLogger)

	cfg, repo, _, locator, client, cleanup := setup(t)
	defer cleanup()

	ctx, cancel := testhelper.Context()
	defer cancel()
	ctx = ctxlogrus.ToContext(ctx, log.WithField("test", "logging"))

	pool, err := objectpool.NewObjectPool(cfg, locator, git.NewExecCommandFactory(cfg), repo.GetStorageName(), gittest.NewObjectPoolName(t))
	require.NoError(t, err)
	defer pool.Remove(ctx)

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
	defer cfgBuilder.Cleanup()
	cfg, repos := cfgBuilder.BuildWithRepoAt(t, t.Name())

	locator := config.NewLocator(cfg)
	gitCmdFactory := git.NewExecCommandFactory(cfg)
	server := NewServer(cfg, locator, gitCmdFactory)

	ctx, cancel := testhelper.Context()
	defer cancel()

	pool, err := objectpool.NewObjectPool(cfg, locator, gitCmdFactory, repos[0].StorageName, gittest.NewObjectPoolName(t))
	require.NoError(t, err)
	defer pool.Remove(ctx)

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
