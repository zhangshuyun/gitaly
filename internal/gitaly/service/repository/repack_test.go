package repository

import (
	"bytes"
	"encoding/json"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
)

func TestRepackIncrementalSuccess(t *testing.T) {
	_, repo, repoPath, client := setupRepositoryService(t)

	packPath := filepath.Join(repoPath, "objects", "pack")

	// Reset mtime to a long while ago since some filesystems don't have sub-second
	// precision on `mtime`.
	// Stamp taken from https://golang.org/pkg/time/#pkg-constants
	testhelper.MustRunCommand(t, nil, "touch", "-t", testTimeString, filepath.Join(packPath, "*"))
	testTime := time.Date(2006, 01, 02, 15, 04, 05, 0, time.UTC)
	ctx, cancel := testhelper.Context()
	defer cancel()
	c, err := client.RepackIncremental(ctx, &gitalypb.RepackIncrementalRequest{Repository: repo})
	assert.NoError(t, err)
	assert.NotNil(t, c)

	// Entire `path`-folder gets updated so this is fine :D
	assertModTimeAfter(t, testTime, packPath)
}

func TestRepackIncrementalCollectLogStatistics(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	logBuffer := &bytes.Buffer{}
	logger := &logrus.Logger{Out: logBuffer, Formatter: &logrus.JSONFormatter{}, Level: logrus.InfoLevel}

	_, repo, _, client := setupRepositoryService(t, testserver.WithLogger(logger))

	_, err := client.RepackIncremental(ctx, &gitalypb.RepackIncrementalRequest{Repository: repo})
	assert.NoError(t, err)

	mustCountObjectLog(t, logBuffer.String())
}

func TestRepackLocal(t *testing.T) {
	cfg, repo, repoPath, client := setupRepositoryServiceWithWorktree(t)

	commiterArgs := []string{"-c", "user.name=Scrooge McDuck", "-c", "user.email=scrooge@mcduck.com"}
	cmdArgs := append(commiterArgs, "-C", repoPath, "commit", "--allow-empty", "-m", "An empty commit")
	cmd := exec.Command(cfg.Git.BinPath, cmdArgs...)
	altObjectsDir := "./alt-objects"
	altDirsCommit := gittest.CreateCommitInAlternateObjectDirectory(t, cfg.Git.BinPath, repoPath, altObjectsDir, cmd)

	repoCommit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch(t.Name()))

	ctx, cancelFn := testhelper.Context()
	defer cancelFn()

	// Set GIT_ALTERNATE_OBJECT_DIRECTORIES on the outgoing request. The
	// intended use case of the behavior we're testing here is that
	// alternates are found through the objects/info/alternates file instead
	// of GIT_ALTERNATE_OBJECT_DIRECTORIES. But for the purpose of this test
	// it doesn't matter.
	repo.GitAlternateObjectDirectories = []string{altObjectsDir}
	_, err := client.RepackFull(ctx, &gitalypb.RepackFullRequest{Repository: repo})
	require.NoError(t, err)

	packFiles, err := filepath.Glob(filepath.Join(repoPath, ".git", "objects", "pack", "pack-*.pack"))
	require.NoError(t, err)
	require.Len(t, packFiles, 1)

	packContents := gittest.Exec(t, cfg, "-C", repoPath, "verify-pack", "-v", packFiles[0])
	require.NotContains(t, string(packContents), string(altDirsCommit))
	require.Contains(t, string(packContents), repoCommit)
}

func TestRepackIncrementalFailure(t *testing.T) {
	cfg, client := setupRepositoryServiceWithoutRepo(t)

	tests := []struct {
		repo *gitalypb.Repository
		code codes.Code
		desc string
	}{
		{desc: "nil repo", repo: nil, code: codes.InvalidArgument},
		{desc: "invalid storage name", repo: &gitalypb.Repository{StorageName: "foo"}, code: codes.InvalidArgument},
		{desc: "no storage name", repo: &gitalypb.Repository{RelativePath: "bar"}, code: codes.InvalidArgument},
		{desc: "non-existing repo", repo: &gitalypb.Repository{StorageName: cfg.Storages[0].Name, RelativePath: "bar"}, code: codes.NotFound},
	}

	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			ctx, cancel := testhelper.Context()
			defer cancel()
			_, err := client.RepackIncremental(ctx, &gitalypb.RepackIncrementalRequest{Repository: test.repo})
			testhelper.RequireGrpcError(t, err, test.code)
		})
	}
}

func TestRepackFullSuccess(t *testing.T) {
	_, repo, repoPath, client := setupRepositoryService(t)

	tests := []struct {
		req  *gitalypb.RepackFullRequest
		desc string
	}{
		{req: &gitalypb.RepackFullRequest{Repository: repo, CreateBitmap: true}, desc: "with bitmap"},
		{req: &gitalypb.RepackFullRequest{Repository: repo, CreateBitmap: false}, desc: "without bitmap"},
	}

	packPath := filepath.Join(repoPath, "objects", "pack")

	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			// Reset mtime to a long while ago since some filesystems don't have sub-second
			// precision on `mtime`.
			testhelper.MustRunCommand(t, nil, "touch", "-t", testTimeString, packPath)
			testTime := time.Date(2006, 01, 02, 15, 04, 05, 0, time.UTC)
			ctx, cancel := testhelper.Context()
			defer cancel()
			c, err := client.RepackFull(ctx, test.req)
			assert.NoError(t, err)
			assert.NotNil(t, c)

			// Entire `path`-folder gets updated so this is fine :D
			assertModTimeAfter(t, testTime, packPath)

			bmPath, err := filepath.Glob(filepath.Join(packPath, "pack-*.bitmap"))
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
	ctx, cancel := testhelper.Context()
	defer cancel()

	logBuffer := &bytes.Buffer{}
	logger := &logrus.Logger{Out: logBuffer, Formatter: &logrus.JSONFormatter{}, Level: logrus.InfoLevel}

	_, repo, _, client := setupRepositoryService(t, testserver.WithLogger(logger))

	_, err := client.RepackFull(ctx, &gitalypb.RepackFullRequest{Repository: repo})
	require.NoError(t, err)

	mustCountObjectLog(t, logBuffer.String())
}

func mustCountObjectLog(t testing.TB, logData string) {
	t.Helper()

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
	cfg, client := setupRepositoryServiceWithoutRepo(t)

	tests := []struct {
		repo *gitalypb.Repository
		code codes.Code
		desc string
	}{
		{desc: "nil repo", repo: nil, code: codes.InvalidArgument},
		{desc: "invalid storage name", repo: &gitalypb.Repository{StorageName: "foo"}, code: codes.InvalidArgument},
		{desc: "no storage name", repo: &gitalypb.Repository{RelativePath: "bar"}, code: codes.InvalidArgument},
		{desc: "non-existing repo", repo: &gitalypb.Repository{StorageName: cfg.Storages[0].Name, RelativePath: "bar"}, code: codes.NotFound},
	}

	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			ctx, cancel := testhelper.Context()
			defer cancel()
			_, err := client.RepackFull(ctx, &gitalypb.RepackFullRequest{Repository: test.repo})
			testhelper.RequireGrpcError(t, err, test.code)
		})
	}
}

func TestRepackFullDeltaIslands(t *testing.T) {
	cfg, repo, repoPath, client := setupRepositoryService(t)

	ctx, cancel := testhelper.Context()
	defer cancel()

	gittest.TestDeltaIslands(t, cfg, repoPath, func() error {
		_, err := client.RepackFull(ctx, &gitalypb.RepackFullRequest{Repository: repo})
		return err
	})
}

func TestLog2Threads(t *testing.T) {
	for _, tt := range []struct {
		cpus    int
		threads string
	}{
		{1, "1"},
		{2, "1"},
		{3, "1"},
		{4, "2"},
		{8, "3"},
		{9, "3"},
		{13, "3"},
		{16, "4"},
		{27, "4"},
		{32, "5"},
	} {
		actualThreads := log2Threads(tt.cpus)
		require.Equal(t, tt.threads, actualThreads.Value)
	}
}
