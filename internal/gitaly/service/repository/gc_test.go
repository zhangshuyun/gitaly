package repository

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
)

var (
	freshTime   = time.Now()
	oldTime     = freshTime.Add(-2 * time.Hour)
	oldTreeTime = freshTime.Add(-7 * time.Hour)
)

func TestGarbageCollectCommitGraph(t *testing.T) {
	cfg, repo, repoPath, client := setupRepositoryService(t)

	ctx, cancel := testhelper.Context()
	defer cancel()

	c, err := client.GarbageCollect(ctx, &gitalypb.GarbageCollectRequest{Repository: repo})
	assert.NoError(t, err)
	assert.NotNil(t, c)

	assert.FileExistsf(t,
		filepath.Join(repoPath, "objects/info/commit-graph"),
		"pre-computed commit-graph should exist after running garbage collect",
	)

	repoCfgPath := filepath.Join(repoPath, "config")

	cfgF, err := os.Open(repoCfgPath)
	require.NoError(t, err)
	defer cfgF.Close()

	cfgCmd, err := localrepo.New(git.NewExecCommandFactory(cfg), repo, cfg).Config().GetRegexp(ctx, "core.commitgraph", git.ConfigGetRegexpOpts{})
	require.NoError(t, err)
	require.Equal(t, []git.ConfigPair{{Key: "core.commitgraph", Value: "true"}}, cfgCmd)
}

func TestGarbageCollectSuccess(t *testing.T) {
	cfg, repo, _, client := setupRepositoryService(t)

	tests := []struct {
		req  *gitalypb.GarbageCollectRequest
		desc string
	}{
		{
			req:  &gitalypb.GarbageCollectRequest{Repository: repo, CreateBitmap: false},
			desc: "without bitmap",
		},
		{
			req:  &gitalypb.GarbageCollectRequest{Repository: repo, CreateBitmap: true},
			desc: "with bitmap",
		},
	}

	packPath := filepath.Join(cfg.Storages[0].Path, repo.GetRelativePath(), "objects", "pack")

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

	cfg, repo, repoPath, client := setupRepositoryService(t)

	blobHashes := gittest.WriteBlobs(t, cfg, repoPath, 3)
	oldDanglingObjFile := filepath.Join(repoPath, "objects", blobHashes[0][:2], blobHashes[0][2:])
	newDanglingObjFile := filepath.Join(repoPath, "objects", blobHashes[1][:2], blobHashes[1][2:])
	oldReferencedObjFile := filepath.Join(repoPath, "objects", blobHashes[2][:2], blobHashes[2][2:])

	// create a reference to the blob, so it should not be removed by gc
	gittest.CommitBlobWithName(t, cfg, repoPath, blobHashes[2], t.Name(), t.Name())

	// change modification time of the blobs to make them attractive for the gc
	aBitMoreThan30MinutesAgo := time.Now().Add(-30*time.Minute - time.Second)
	farAgo := time.Date(2015, 1, 1, 1, 1, 1, 1, time.UTC)
	require.NoError(t, os.Chtimes(oldDanglingObjFile, aBitMoreThan30MinutesAgo, aBitMoreThan30MinutesAgo))
	require.NoError(t, os.Chtimes(newDanglingObjFile, time.Now(), time.Now()))
	require.NoError(t, os.Chtimes(oldReferencedObjFile, farAgo, farAgo))

	// Prune option has no effect when disabled
	c, err := client.GarbageCollect(ctx, &gitalypb.GarbageCollectRequest{Repository: repo, Prune: false})
	require.NoError(t, err)
	require.NotNil(t, c)
	require.FileExists(t, oldDanglingObjFile, "blob should not be removed from object storage as it was modified less then 2 weeks ago")

	// Prune option has effect when enabled
	c, err = client.GarbageCollect(ctx, &gitalypb.GarbageCollectRequest{Repository: repo, Prune: true})
	require.NoError(t, err)
	require.NotNil(t, c)

	_, err = os.Stat(oldDanglingObjFile)
	require.True(t, os.IsNotExist(err), "blob should be removed from object storage as it is too old and there are no references to it")
	require.FileExists(t, newDanglingObjFile, "blob should not be removed from object storage as it is fresh enough despite there are no references to it")
	require.FileExists(t, oldReferencedObjFile, "blob should not be removed from object storage as it is referenced by something despite it is too old")
}

func TestGarbageCollectLogStatistics(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	logBuffer := &bytes.Buffer{}
	logger := &logrus.Logger{Out: logBuffer, Formatter: &logrus.JSONFormatter{}, Level: logrus.InfoLevel}

	_, repo, _, client := setupRepositoryService(t, testserver.WithLogger(logger))

	_, err := client.GarbageCollect(ctx, &gitalypb.GarbageCollectRequest{Repository: repo})
	require.NoError(t, err)

	mustCountObjectLog(t, logBuffer.String())
}

func TestGarbageCollectDeletesRefsLocks(t *testing.T) {
	_, repo, repoPath, client := setupRepositoryService(t)

	ctx, cancel := testhelper.Context()
	defer cancel()

	req := &gitalypb.GarbageCollectRequest{Repository: repo}
	refsPath := filepath.Join(repoPath, "refs")

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
	_, repo, _, client := setupRepositoryService(t)

	tests := []struct {
		repo *gitalypb.Repository
		code codes.Code
	}{
		{repo: nil, code: codes.InvalidArgument},
		{repo: &gitalypb.Repository{StorageName: "foo"}, code: codes.InvalidArgument},
		{repo: &gitalypb.Repository{RelativePath: "bar"}, code: codes.InvalidArgument},
		{repo: &gitalypb.Repository{StorageName: repo.GetStorageName(), RelativePath: "bar"}, code: codes.NotFound},
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
	_, repo, repoPath, client := setupRepositoryService(t)

	// Make the directory, so we can create random reflike things in it
	require.NoError(t, os.MkdirAll(filepath.Join(repoPath, "refs", "keep-around"), 0755))

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
			testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "update-ref", existingRefName, existingSha)

			// Create an invalid ref that should should be removed with the testcase
			bogusSha := "b3f5e4adf6277b571b7943a4f0405a6dd7ee7e15"
			bogusPath := filepath.Join(repoPath, fmt.Sprintf("refs/keep-around/%s", bogusSha))
			require.NoError(t, ioutil.WriteFile(bogusPath, []byte(bogusSha), 0644))

			// Creating the keeparound without using git so we can create invalid ones in testcases
			refPath := filepath.Join(repoPath, fmt.Sprintf("refs/keep-around/%s", testcase.refName))
			require.NoError(t, ioutil.WriteFile(refPath, []byte(testcase.refContent), 0644))

			// Perform the request
			req := &gitalypb.GarbageCollectRequest{Repository: repo}
			_, err := client.GarbageCollect(ctx, req)
			require.NoError(t, err)

			// The existing keeparound still exists
			commitSha := testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "rev-parse", existingRefName)
			require.Equal(t, existingSha, text.ChompBytes(commitSha))

			//The invalid one was removed
			_, err = os.Stat(bogusPath)
			require.True(t, os.IsNotExist(err), "expected 'does not exist' error, got %v", err)

			if testcase.shouldExist {
				keepAroundName := fmt.Sprintf("refs/keep-around/%s", testcase.refName)
				commitSha := testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "rev-parse", keepAroundName)
				require.Equal(t, testcase.refName, text.ChompBytes(commitSha))
			} else {
				_, err := os.Stat(refPath)
				require.True(t, os.IsNotExist(err), "expected 'does not exist' error, got %v", err)
			}
		})
	}
}

func mustCreateFileWithTimes(t testing.TB, path string, mTime time.Time) {
	t.Helper()

	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0755))
	require.NoError(t, ioutil.WriteFile(path, nil, 0644))
	require.NoError(t, os.Chtimes(path, mTime, mTime))
}

func TestGarbageCollectDeltaIslands(t *testing.T) {
	cfg, repo, repoPath, client := setupRepositoryService(t)

	ctx, cancel := testhelper.Context()
	defer cancel()

	gittest.TestDeltaIslands(t, cfg, repoPath, func() error {
		_, err := client.GarbageCollect(ctx, &gitalypb.GarbageCollectRequest{Repository: repo})
		return err
	})
}
