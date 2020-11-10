package objectpool

import (
	"bytes"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/git/objectpool"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	gconfig "gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"gitlab.com/gitlab-org/labkit/log"
	"google.golang.org/grpc/codes"
)

func TestFetchIntoObjectPool_Success(t *testing.T) {
	locator := gconfig.NewLocator(gconfig.Config)
	server, serverSocketPath := runObjectPoolServer(t, locator)
	defer server.Stop()

	client, conn := newObjectPoolClient(t, serverSocketPath)
	defer conn.Close()

	ctx, cancel := testhelper.Context()
	defer cancel()

	testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	repoCommit := testhelper.CreateCommit(t, testRepoPath, t.Name(), &testhelper.CreateCommitOpts{Message: t.Name()})

	pool, err := objectpool.NewObjectPool(config.Config, "default", testhelper.NewTestObjectPoolName(t))
	require.NoError(t, err)
	defer pool.Remove(ctx)

	req := &gitalypb.FetchIntoObjectPoolRequest{
		ObjectPool: pool.ToProto(),
		Origin:     testRepo,
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
}

func TestFetchIntoObjectPool_CollectLogStatistics(t *testing.T) {
	defer func(tl func(tb testing.TB) *logrus.Logger) {
		testhelper.NewTestLogger = tl
	}(testhelper.NewTestLogger)

	logBuffer := &bytes.Buffer{}
	testhelper.NewTestLogger = func(tb testing.TB) *logrus.Logger {
		return &logrus.Logger{Out: logBuffer, Formatter: &logrus.JSONFormatter{}, Level: logrus.InfoLevel}
	}

	ctx, cancel := testhelper.Context()
	defer cancel()
	ctx = ctxlogrus.ToContext(ctx, log.WithField("test", "logging"))

	locator := gconfig.NewLocator(gconfig.Config)
	server, serverSocketPath := runObjectPoolServer(t, locator)
	defer server.Stop()

	client, conn := newObjectPoolClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	pool, err := objectpool.NewObjectPool(config.Config, "default", testhelper.NewTestObjectPoolName(t))
	require.NoError(t, err)
	defer pool.Remove(ctx)

	req := &gitalypb.FetchIntoObjectPoolRequest{
		ObjectPool: pool.ToProto(),
		Origin:     testRepo,
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
	locator := gconfig.NewLocator(gconfig.Config)
	server := NewServer(locator)

	ctx, cancel := testhelper.Context()
	defer cancel()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	pool, err := objectpool.NewObjectPool(config.Config, "default", testhelper.NewTestObjectPoolName(t))
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
				Origin: testRepo,
			},
			code:   codes.InvalidArgument,
			errMsg: "object pool is empty",
		},
		{
			description: "origin and pool do not share the same storage",
			request: &gitalypb.FetchIntoObjectPoolRequest{
				Origin:     testRepo,
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
