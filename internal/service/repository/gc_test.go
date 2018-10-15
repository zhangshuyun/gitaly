package repository

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly-proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"google.golang.org/grpc/codes"
)

var (
	freshTime   = time.Now()
	oldTime     = freshTime.Add(-2 * time.Hour)
	oldTreeTime = freshTime.Add(-7 * time.Hour)
)

func TestGarbageCollectSuccess(t *testing.T) {
	server, serverSocketPath := runRepoServer(t)
	defer server.Stop()

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

	packPath := path.Join(testhelper.GitlabTestStoragePath(), testRepo.GetRelativePath(), "objects", "pack")

	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			// Reset mtime to a long while ago since some filesystems don't have sub-second
			// precision on `mtime`.
			// Stamp taken from https://golang.org/pkg/time/#pkg-constants
			testhelper.MustRunCommand(t, nil, "touch", "-t", testTimeString, packPath)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			c, err := client.GarbageCollect(ctx, test.req)
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
			} else {
				if len(bmPath) != 0 {
					t.Errorf("Bitmap found: %v", bmPath)
				}
			}
		})
	}
}

func TestGarbageCollectDeletesRefsLocks(t *testing.T) {
	server, serverSocketPath := runRepoServer(t)
	defer server.Stop()

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
	createFileWithTimes(keepRefPath, freshTime)
	keepOldRefPath := filepath.Join(refsPath, "heads", "keepthisalso")
	createFileWithTimes(keepOldRefPath, oldTime)
	keepDeceitfulRef := filepath.Join(refsPath, "heads", " .lock.not-actually-a-lock.lock ")
	createFileWithTimes(keepDeceitfulRef, oldTime)

	keepLockPath := filepath.Join(refsPath, "heads", "keepthis.lock")
	createFileWithTimes(keepLockPath, freshTime)

	deleteLockPath := filepath.Join(refsPath, "heads", "deletethis.lock")
	createFileWithTimes(deleteLockPath, oldTime)

	c, err := client.GarbageCollect(ctx, req)
	testhelper.RequireGrpcError(t, err, codes.Internal)
	require.Contains(t, err.Error(), "GarbageCollect: cmd wait")
	assert.Nil(t, c)

	// Sanity checks
	assert.FileExists(t, keepRefPath)
	assert.FileExists(t, keepOldRefPath)
	assert.FileExists(t, keepDeceitfulRef)

	assert.FileExists(t, keepLockPath)

	testhelper.AssertFileNotExists(t, deleteLockPath)
}

func TestGarbageCollectFailure(t *testing.T) {
	server, serverSocketPath := runRepoServer(t)
	defer server.Stop()

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
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			_, err := client.GarbageCollect(ctx, &gitalypb.GarbageCollectRequest{Repository: test.repo})
			testhelper.RequireGrpcError(t, err, test.code)
		})
	}

}

func createFileWithTimes(path string, mTime time.Time) {
	ioutil.WriteFile(path, nil, 0644)
	os.Chtimes(path, mTime, mTime)
}
