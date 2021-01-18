package repository

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"gitlab.com/gitlab-org/labkit/log"
	"google.golang.org/grpc/codes"
)

var (
	freshTime   = time.Now()
	oldTime     = freshTime.Add(-2 * time.Hour)
	oldTreeTime = freshTime.Add(-7 * time.Hour)
)

func TestGarbageCollectCommitGraph(t *testing.T) {
	locator := config.NewLocator(config.Config)
	serverSocketPath, stop := runRepoServer(t, locator, testhelper.WithInternalSocket(config.Config))
	defer stop()

	client, conn := newRepositoryClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	ctx, cancel := testhelper.Context()
	defer cancel()

	c, err := client.GarbageCollect(ctx, &gitalypb.GarbageCollectRequest{Repository: testRepo})
	assert.NoError(t, err)
	assert.NotNil(t, c)

	assert.FileExistsf(t,
		filepath.Join(testRepoPath, "objects/info/commit-graph"),
		"pre-computed commit-graph should exist after running garbage collect",
	)

	repoCfgPath := filepath.Join(testRepoPath, "config")

	cfgF, err := os.Open(repoCfgPath)
	require.NoError(t, err)
	defer cfgF.Close()

	cfg, err := localrepo.New(git.NewExecCommandFactory(config.Config), testRepo, config.Config).Config().GetRegexp(ctx, "core.commitgraph", git.ConfigGetRegexpOpts{})
	require.NoError(t, err)
	require.Equal(t, []git.ConfigPair{{Key: "core.commitgraph", Value: "true"}}, cfg)
}

func TestGarbageCollectSuccess(t *testing.T) {
	locator := config.NewLocator(config.Config)
	serverSocketPath, stop := runRepoServer(t, locator, testhelper.WithInternalSocket(config.Config))
	defer stop()

	client, conn := newRepositoryClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	tests := []struct {
		req  *gitalypb.GarbageCollectRequest
		desc string
	}{
		{
			req:  &gitalypb.GarbageCollectRequest{Repository: testRepo, CreateBitmap: false},
			desc: "without bitmap",
		},
		{
			req:  &gitalypb.GarbageCollectRequest{Repository: testRepo, CreateBitmap: true},
			desc: "with bitmap",
		},
	}

	packPath := filepath.Join(testhelper.GitlabTestStoragePath(), testRepo.GetRelativePath(), "objects", "pack")

	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			// Reset mtime to a long while ago since some filesystems don't have sub-second
			// precision on `mtime`.
			// Stamp taken from https://golang.org/pkg/time/#pkg-constants
			testhelper.MustRunCommand(t, nil, "touch", "-t", testTimeString, packPath)
			ctx, cancel := testhelper.Context()
			defer cancel()
			c, err := client.GarbageCollect(ctx, test.req)
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
			} else {
				if len(bmPath) != 0 {
					t.Errorf("Bitmap found: %v", bmPath)
				}
			}
		})
	}
}

func TestGarbageCollectWithPrune(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	locator := config.NewLocator(config.Config)
	serverSocketPath, stop := runRepoServer(t, locator, testhelper.WithInternalSocket(config.Config))
	defer stop()

	client, conn := newRepositoryClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, repoPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	blobHashes := testhelper.WriteBlobs(t, repoPath, 3)
	oldDanglingObjFile := filepath.Join(repoPath, "objects", blobHashes[0][:2], blobHashes[0][2:])
	newDanglingObjFile := filepath.Join(repoPath, "objects", blobHashes[1][:2], blobHashes[1][2:])
	oldReferencedObjFile := filepath.Join(repoPath, "objects", blobHashes[2][:2], blobHashes[2][2:])

	// create a reference to the blob, so it should not be removed by gc
	testhelper.CommitBlobWithName(t, repoPath, blobHashes[2], t.Name(), t.Name())

	// change modification time of the blobs to make them attractive for the gc
	aBitMoreThan30MinutesAgo := time.Now().Add(-30*time.Minute - time.Second)
	farAgo := time.Date(2015, 1, 1, 1, 1, 1, 1, time.UTC)
	require.NoError(t, os.Chtimes(oldDanglingObjFile, aBitMoreThan30MinutesAgo, aBitMoreThan30MinutesAgo))
	require.NoError(t, os.Chtimes(newDanglingObjFile, time.Now(), time.Now()))
	require.NoError(t, os.Chtimes(oldReferencedObjFile, farAgo, farAgo))

	// Prune option has no effect when disabled
	c, err := client.GarbageCollect(ctx, &gitalypb.GarbageCollectRequest{Repository: testRepo, Prune: false})
	require.NoError(t, err)
	require.NotNil(t, c)
	require.FileExists(t, oldDanglingObjFile, "blob should not be removed from object storage as it was modified less then 2 weeks ago")

	// Prune option has effect when enabled
	c, err = client.GarbageCollect(ctx, &gitalypb.GarbageCollectRequest{Repository: testRepo, Prune: true})
	require.NoError(t, err)
	require.NotNil(t, c)

	_, err = os.Stat(oldDanglingObjFile)
	require.True(t, os.IsNotExist(err), "blob should be removed from object storage as it is too old and there are no references to it")
	require.FileExists(t, newDanglingObjFile, "blob should not be removed from object storage as it is fresh enough despite there are no references to it")
	require.FileExists(t, oldReferencedObjFile, "blob should not be removed from object storage as it is referenced by something despite it is too old")
}

func TestGarbageCollectLogStatistics(t *testing.T) {
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

	locator := config.NewLocator(config.Config)
	serverSocketPath, stop := runRepoServer(t, locator, testhelper.WithInternalSocket(config.Config))
	defer stop()

	client, conn := newRepositoryClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	_, err := client.GarbageCollect(ctx, &gitalypb.GarbageCollectRequest{Repository: testRepo})
	require.NoError(t, err)

	mustCountObjectLog(t, logBuffer.String())
}

func TestGarbageCollectDeletesRefsLocks(t *testing.T) {
	locator := config.NewLocator(config.Config)
	serverSocketPath, stop := runRepoServer(t, locator)
	defer stop()

	client, conn := newRepositoryClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	ctx, cancel := testhelper.Context()
	defer cancel()

	req := &gitalypb.GarbageCollectRequest{Repository: testRepo}
	refsPath := filepath.Join(testRepoPath, "refs")

	// Note: Creating refs this way makes `git gc` crash but this actually works
	// in our favor for this test since we can ensure that the files kept and
	// deleted are all due to our *.lock cleanup step before gc runs (since
	// `git gc` also deletes files from /refs when packing).
	keepRefPath := filepath.Join(refsPath, "heads", "keepthis")
	mustCreateFileWithTimes(t, keepRefPath, freshTime)
	keepOldRefPath := filepath.Join(refsPath, "heads", "keepthisalso")
	mustCreateFileWithTimes(t, keepOldRefPath, oldTime)
	keepDeceitfulRef := filepath.Join(refsPath, "heads", " .lock.not-actually-a-lock.lock ")
	mustCreateFileWithTimes(t, keepDeceitfulRef, oldTime)

	keepLockPath := filepath.Join(refsPath, "heads", "keepthis.lock")
	mustCreateFileWithTimes(t, keepLockPath, freshTime)

	deleteLockPath := filepath.Join(refsPath, "heads", "deletethis.lock")
	mustCreateFileWithTimes(t, deleteLockPath, oldTime)

	c, err := client.GarbageCollect(ctx, req)
	testhelper.RequireGrpcError(t, err, codes.Internal)
	require.Contains(t, err.Error(), "GarbageCollect: cmd wait")
	assert.Nil(t, c)

	// Sanity checks
	assert.FileExists(t, keepRefPath)
	assert.FileExists(t, keepOldRefPath)
	assert.FileExists(t, keepDeceitfulRef)

	assert.FileExists(t, keepLockPath)

	testhelper.AssertPathNotExists(t, deleteLockPath)
}

func TestGarbageCollectFailure(t *testing.T) {
	locator := config.NewLocator(config.Config)
	serverSocketPath, stop := runRepoServer(t, locator)
	defer stop()

	client, conn := newRepositoryClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	tests := []struct {
		repo *gitalypb.Repository
		code codes.Code
	}{
		{repo: nil, code: codes.InvalidArgument},
		{repo: &gitalypb.Repository{StorageName: "foo"}, code: codes.InvalidArgument},
		{repo: &gitalypb.Repository{RelativePath: "bar"}, code: codes.InvalidArgument},
		{repo: &gitalypb.Repository{StorageName: testRepo.GetStorageName(), RelativePath: "bar"}, code: codes.NotFound},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%v", test.repo), func(t *testing.T) {
			ctx, cancel := testhelper.Context()
			defer cancel()
			_, err := client.GarbageCollect(ctx, &gitalypb.GarbageCollectRequest{Repository: test.repo})
			testhelper.RequireGrpcError(t, err, test.code)
		})
	}
}

func TestCleanupInvalidKeepAroundRefs(t *testing.T) {
	locator := config.NewLocator(config.Config)
	serverSocketPath, stop := runRepoServer(t, locator, testhelper.WithInternalSocket(config.Config))
	defer stop()

	client, conn := newRepositoryClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	// Make the directory, so we can create random reflike things in it
	require.NoError(t, os.MkdirAll(filepath.Join(testRepoPath, "refs", "keep-around"), 0755))

	testCases := []struct {
		desc        string
		refName     string
		refContent  string
		shouldExist bool
	}{
		{
			desc:        "A valid ref",
			refName:     "0b4bc9a49b562e85de7cc9e834518ea6828729b9",
			refContent:  "0b4bc9a49b562e85de7cc9e834518ea6828729b9",
			shouldExist: true,
		},
		{
			desc:        "A ref that does not exist",
			refName:     "bogus",
			refContent:  "bogus",
			shouldExist: false,
		}, {
			desc:        "Filled with the blank ref",
			refName:     "0b4bc9a49b562e85de7cc9e834518ea6828729b9",
			refContent:  git.ZeroOID.String(),
			shouldExist: true,
		}, {
			desc:        "An existing ref with blank content",
			refName:     "0b4bc9a49b562e85de7cc9e834518ea6828729b9",
			refContent:  "",
			shouldExist: true,
		}, {
			desc:        "A valid sha that does not exist in the repo",
			refName:     "d669a6f1a70693058cf484318c1cee8526119938",
			refContent:  "d669a6f1a70693058cf484318c1cee8526119938",
			shouldExist: false,
		},
	}

	for _, testcase := range testCases {
		t.Run(testcase.desc, func(t *testing.T) {
			ctx, cancel := testhelper.Context()
			defer cancel()

			// Create a proper keep-around loose ref
			existingSha := "1e292f8fedd741b75372e19097c76d327140c312"
			existingRefName := fmt.Sprintf("refs/keep-around/%s", existingSha)
			testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "update-ref", existingRefName, existingSha)

			// Create an invalid ref that should should be removed with the testcase
			bogusSha := "b3f5e4adf6277b571b7943a4f0405a6dd7ee7e15"
			bogusPath := filepath.Join(testRepoPath, fmt.Sprintf("refs/keep-around/%s", bogusSha))
			require.NoError(t, ioutil.WriteFile(bogusPath, []byte(bogusSha), 0644))

			// Creating the keeparound without using git so we can create invalid ones in testcases
			refPath := filepath.Join(testRepoPath, fmt.Sprintf("refs/keep-around/%s", testcase.refName))
			require.NoError(t, ioutil.WriteFile(refPath, []byte(testcase.refContent), 0644))

			// Perform the request
			req := &gitalypb.GarbageCollectRequest{Repository: testRepo}
			_, err := client.GarbageCollect(ctx, req)
			require.NoError(t, err)

			// The existing keeparound still exists
			commitSha := testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "rev-parse", existingRefName)
			require.Equal(t, existingSha, text.ChompBytes(commitSha))

			//The invalid one was removed
			_, err = os.Stat(bogusPath)
			require.True(t, os.IsNotExist(err), "expected 'does not exist' error, got %v", err)

			if testcase.shouldExist {
				keepAroundName := fmt.Sprintf("refs/keep-around/%s", testcase.refName)
				commitSha := testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "rev-parse", keepAroundName)
				require.Equal(t, testcase.refName, text.ChompBytes(commitSha))
			} else {
				_, err := os.Stat(refPath)
				require.True(t, os.IsNotExist(err), "expected 'does not exist' error, got %v", err)
			}
		})
	}
}

func mustCreateFileWithTimes(t testing.TB, path string, mTime time.Time) {
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0755))
	require.NoError(t, ioutil.WriteFile(path, nil, 0644))
	require.NoError(t, os.Chtimes(path, mTime, mTime))
}

func TestGarbageCollectDeltaIslands(t *testing.T) {
	locator := config.NewLocator(config.Config)
	serverSocketPath, stop := runRepoServer(t, locator, testhelper.WithInternalSocket(config.Config))
	defer stop()

	client, conn := newRepositoryClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	ctx, cancel := testhelper.Context()
	defer cancel()

	gittest.TestDeltaIslands(t, testRepoPath, func() error {
		_, err := client.GarbageCollect(ctx, &gitalypb.GarbageCollectRequest{Repository: testRepo})
		return err
	})
}
