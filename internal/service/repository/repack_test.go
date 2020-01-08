package repository

import (
	"bytes"
	"context"
	"encoding/json"
	"os/exec"
	"path"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"gitlab.com/gitlab-org/labkit/log"
	"google.golang.org/grpc/codes"
)

func TestRepackIncrementalSuccess(t *testing.T) {
	stop, serverSocketPath := runRepoServer(t)
	defer stop()

	client, conn := newRepositoryClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	packPath := path.Join(testhelper.GitlabTestStoragePath(), testRepo.GetRelativePath(), "objects", "pack")

	// Reset mtime to a long while ago since some filesystems don't have sub-second
	// precision on `mtime`.
	// Stamp taken from https://golang.org/pkg/time/#pkg-constants
	testhelper.MustRunCommand(t, nil, "touch", "-t", testTimeString, path.Join(packPath, "*"))
	testTime := time.Date(2006, 01, 02, 15, 04, 05, 0, time.UTC)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c, err := client.RepackIncremental(ctx, &gitalypb.RepackIncrementalRequest{Repository: testRepo})
	assert.NoError(t, err)
	assert.NotNil(t, c)

	// Entire `path`-folder gets updated so this is fine :D
	assertModTimeAfter(t, testTime, packPath)
}

func TestRepackIncrementalCollectLogStatistics(t *testing.T) {
	defer func(tl func(tb testing.TB) *logrus.Logger) {
		testhelper.NewTestLogger = tl
	}(testhelper.NewTestLogger)

	logBuffer := &bytes.Buffer{}
	testhelper.NewTestLogger = func(tb testing.TB) *logrus.Logger {
		return &logrus.Logger{Out: logBuffer, Formatter: new(logrus.JSONFormatter), Level: logrus.InfoLevel}
	}

	ctx, cancel := testhelper.Context()
	defer cancel()
	ctx = ctxlogrus.ToContext(ctx, log.WithField("test", "logging"))

	stop, serverSocketPath := runRepoServer(t)
	defer stop()

	client, conn := newRepositoryClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	_, err := client.RepackIncremental(ctx, &gitalypb.RepackIncrementalRequest{Repository: testRepo})
	assert.NoError(t, err)

	mustCountObjectLog(t, logBuffer.String())
}

func TestRepackLocal(t *testing.T) {
	stop, serverSocketPath := runRepoServer(t)
	defer stop()

	client, conn := newRepositoryClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, repoPath, cleanupFn := testhelper.NewTestRepoWithWorktree(t)
	defer cleanupFn()

	commiterArgs := []string{"-c", "user.name=Scrooge McDuck", "-c", "user.email=scrooge@mcduck.com"}
	cmdArgs := append(commiterArgs, "-C", repoPath, "commit", "--allow-empty", "-m", "An empty commit")
	cmd := exec.Command("git", cmdArgs...)
	altObjectsDir := "./alt-objects"
	altDirsCommit := testhelper.CreateCommitInAlternateObjectDirectory(t, repoPath, altObjectsDir, cmd)

	repoCommit := testhelper.CreateCommit(t, repoPath, t.Name(), &testhelper.CreateCommitOpts{Message: t.Name()})

	ctx, cancelFn := testhelper.Context()
	defer cancelFn()

	// Set GIT_ALTERNATE_OBJECT_DIRECTORIES on the outgoing request. The
	// intended use case of the behavior we're testing here is that
	// alternates are found through the objects/info/alternates file instead
	// of GIT_ALTERNATE_OBJECT_DIRECTORIES. But for the purpose of this test
	// it doesn't matter.
	testRepo.GitAlternateObjectDirectories = []string{altObjectsDir}
	_, err := client.RepackFull(ctx, &gitalypb.RepackFullRequest{Repository: testRepo})
	require.NoError(t, err)

	packFiles, err := filepath.Glob(path.Join(repoPath, ".git", "objects", "pack", "pack-*.pack"))
	require.NoError(t, err)
	require.Len(t, packFiles, 1)

	packContents := testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "verify-pack", "-v", packFiles[0])
	require.NotContains(t, string(packContents), string(altDirsCommit))
	require.Contains(t, string(packContents), string(repoCommit))
}

func TestRepackIncrementalFailure(t *testing.T) {
	stop, serverSocketPath := runRepoServer(t)
	defer stop()

	client, conn := newRepositoryClient(t, serverSocketPath)
	defer conn.Close()

	tests := []struct {
		repo *gitalypb.Repository
		code codes.Code
		desc string
	}{
		{desc: "nil repo", repo: nil, code: codes.InvalidArgument},
		{desc: "invalid storage name", repo: &gitalypb.Repository{StorageName: "foo"}, code: codes.InvalidArgument},
		{desc: "no storage name", repo: &gitalypb.Repository{RelativePath: "bar"}, code: codes.InvalidArgument},
		{desc: "non-existing repo", repo: &gitalypb.Repository{StorageName: testhelper.TestRepository().GetStorageName(), RelativePath: "bar"}, code: codes.NotFound},
	}

	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			_, err := client.RepackIncremental(ctx, &gitalypb.RepackIncrementalRequest{Repository: test.repo})
			testhelper.RequireGrpcError(t, err, test.code)
		})
	}
}

func TestRepackFullSuccess(t *testing.T) {
	stop, serverSocketPath := runRepoServer(t)
	defer stop()

	client, conn := newRepositoryClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	tests := []struct {
		req  *gitalypb.RepackFullRequest
		desc string
	}{
		{req: &gitalypb.RepackFullRequest{Repository: testRepo, CreateBitmap: true}, desc: "with bitmap"},
		{req: &gitalypb.RepackFullRequest{Repository: testRepo, CreateBitmap: false}, desc: "without bitmap"},
	}

	packPath := path.Join(testRepoPath, "objects", "pack")

	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			// Reset mtime to a long while ago since some filesystems don't have sub-second
			// precision on `mtime`.
			testhelper.MustRunCommand(t, nil, "touch", "-t", testTimeString, packPath)
			testTime := time.Date(2006, 01, 02, 15, 04, 05, 0, time.UTC)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			c, err := client.RepackFull(ctx, test.req)
			assert.NoError(t, err)
			assert.NotNil(t, c)

			// Entire `path`-folder gets updated so this is fine :D
			assertModTimeAfter(t, testTime, packPath)

			bmPath, err := filepath.Glob(path.Join(packPath, "pack-*.bitmap"))
			if err != nil {
				t.Fatalf("Error globbing bitmaps: %v", err)
			}
			if test.req.GetCreateBitmap() {
				if len(bmPath) == 0 {
					t.Errorf("No bitmaps found")
				}
				doBitmapsContainHashCache(t, bmPath)
			} else {
				if len(bmPath) != 0 {
					t.Errorf("Bitmap found: %v", bmPath)
				}
			}
		})
	}
}

func TestRepackFullCollectLogStatistics(t *testing.T) {
	defer func(tl func(tb testing.TB) *logrus.Logger) {
		testhelper.NewTestLogger = tl
	}(testhelper.NewTestLogger)

	logBuffer := &bytes.Buffer{}
	testhelper.NewTestLogger = func(tb testing.TB) *logrus.Logger {
		return &logrus.Logger{Out: logBuffer, Formatter: new(logrus.JSONFormatter), Level: logrus.InfoLevel}
	}

	ctx, cancel := testhelper.Context()
	defer cancel()
	ctx = ctxlogrus.ToContext(ctx, log.WithField("test", "logging"))

	stop, serverSocketPath := runRepoServer(t)
	defer stop()

	client, conn := newRepositoryClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	_, err := client.RepackFull(ctx, &gitalypb.RepackFullRequest{Repository: testRepo})
	require.NoError(t, err)

	mustCountObjectLog(t, logBuffer.String())
}

func mustCountObjectLog(t testing.TB, logData string) {
	msgs := strings.Split(logData, "\n")
	const key = "count_objects"
	for _, msg := range msgs {
		if strings.Contains(msg, key) {
			var out map[string]interface{}
			require.NoError(t, json.NewDecoder(strings.NewReader(msg)).Decode(&out))
			require.Contains(t, out, "grpc.request.glProjectPath")
			require.Contains(t, out, "grpc.request.glRepository")
			require.Contains(t, out, key, "there is no any information about statistics")
			countObjects := out[key].(map[string]interface{})
			require.Contains(t, countObjects, "count")
			return
		}
	}
	require.FailNow(t, "no info about statistics")
}

func doBitmapsContainHashCache(t *testing.T, bitmapPaths []string) {
	// for each bitmap file, check the 2-byte flag as documented in
	// https://github.com/git/git/blob/master/Documentation/technical/bitmap-format.txt
	for _, bitmapPath := range bitmapPaths {
		gittest.TestBitmapHasHashcache(t, bitmapPath)
	}
}

func TestRepackFullFailure(t *testing.T) {
	stop, serverSocketPath := runRepoServer(t)
	defer stop()

	client, conn := newRepositoryClient(t, serverSocketPath)
	defer conn.Close()

	tests := []struct {
		repo *gitalypb.Repository
		code codes.Code
		desc string
	}{
		{desc: "nil repo", repo: nil, code: codes.InvalidArgument},
		{desc: "invalid storage name", repo: &gitalypb.Repository{StorageName: "foo"}, code: codes.InvalidArgument},
		{desc: "no storage name", repo: &gitalypb.Repository{RelativePath: "bar"}, code: codes.InvalidArgument},
		{desc: "non-existing repo", repo: &gitalypb.Repository{StorageName: testhelper.TestRepository().GetStorageName(), RelativePath: "bar"}, code: codes.NotFound},
	}

	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			_, err := client.RepackFull(ctx, &gitalypb.RepackFullRequest{Repository: test.repo})
			testhelper.RequireGrpcError(t, err, test.code)
		})
	}
}

func TestRepackFullDeltaIslands(t *testing.T) {
	stop, serverSocketPath := runRepoServer(t)
	defer stop()

	client, conn := newRepositoryClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	gittest.TestDeltaIslands(t, testRepoPath, func() error {
		_, err := client.RepackFull(ctx, &gitalypb.RepackFullRequest{Repository: testRepo})
		return err
	})
}
