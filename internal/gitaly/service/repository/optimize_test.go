package repository

import (
	"bytes"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/internal/git/stats"
	"gitlab.com/gitlab-org/gitaly/internal/git/updateref"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
)

func getNewestPackfileModtime(t *testing.T, repoPath string) time.Time {
	packFiles, err := filepath.Glob(filepath.Join(repoPath, "objects", "pack", "*.pack"))
	require.NoError(t, err)
	if len(packFiles) == 0 {
		t.Error("no packfiles exist")
	}

	var newestPackfileModtime time.Time

	for _, packFile := range packFiles {
		info, err := os.Stat(packFile)
		require.NoError(t, err)
		if info.ModTime().After(newestPackfileModtime) {
			newestPackfileModtime = info.ModTime()
		}
	}

	return newestPackfileModtime
}

func TestOptimizeRepository(t *testing.T) {
	testRepo, testRepoPath, cleanup := gittest.CloneRepo(t)
	defer cleanup()

	testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "repack", "-A", "-b")

	ctx, cancel := testhelper.Context()
	defer cancel()

	hasBitmap, err := stats.HasBitmap(testRepoPath)
	require.NoError(t, err)
	require.True(t, hasBitmap, "expect a bitmap since we just repacked with -b")

	// get timestamp of latest packfile
	newestsPackfileTime := getNewestPackfileModtime(t, testRepoPath)

	gittest.CreateCommit(t, config.Config, testRepoPath, "master", nil)

	testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "config", "http.http://localhost:51744/60631c8695bf041a808759a05de53e36a73316aacb502824fabbb0c6055637c1.git.extraHeader", "Authorization: Basic secret-password")
	testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "config", "http.http://localhost:51744/60631c8695bf041a808759a05de53e36a73316aacb502824fabbb0c6055637c2.git.extraHeader", "Authorization: Basic secret-password")
	testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "config", "randomStart-http.http://localhost:51744/60631c8695bf041a808759a05de53e36a73316aacb502824fabbb0c6055637c3.git.extraHeader", "Authorization: Basic secret-password")
	testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "config", "http.http://localhost:51744/60631c8695bf041a808759a05de53e36a73316aacb502824fabbb0c6055637c4.git.extraHeader-randomEnd", "Authorization: Basic secret-password")
	testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "config", "hTTp.http://localhost:51744/60631c8695bf041a808759a05de53e36a73316aacb502824fabbb0c6055637c5.git.ExtrAheaDeR", "Authorization: Basic secret-password")
	testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "config", "http.http://extraHeader/extraheader/EXTRAHEADER.git.extraHeader", "Authorization: Basic secret-password")
	testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "config", "https.https://localhost:51744/60631c8695bf041a808759a05de53e36a73316aacb502824fabbb0c6055637c5.git.extraHeader", "Authorization: Basic secret-password")
	confFileData, err := ioutil.ReadFile(filepath.Join(testRepoPath, "config"))
	require.NoError(t, err)
	require.True(t, bytes.Contains(confFileData, []byte("http://localhost:51744/60631c8695bf041a808759a05de53e36a73316aacb502824fabbb0c6055637c1.git")))
	require.True(t, bytes.Contains(confFileData, []byte("http://localhost:51744/60631c8695bf041a808759a05de53e36a73316aacb502824fabbb0c6055637c2.git")))
	require.True(t, bytes.Contains(confFileData, []byte("http://localhost:51744/60631c8695bf041a808759a05de53e36a73316aacb502824fabbb0c6055637c3")))
	require.True(t, bytes.Contains(confFileData, []byte("http://localhost:51744/60631c8695bf041a808759a05de53e36a73316aacb502824fabbb0c6055637c4.git")))
	require.True(t, bytes.Contains(confFileData, []byte("http://localhost:51744/60631c8695bf041a808759a05de53e36a73316aacb502824fabbb0c6055637c5.git")))
	require.True(t, bytes.Contains(confFileData, []byte("http://extraHeader/extraheader/EXTRAHEADER.git")))
	require.True(t, bytes.Contains(confFileData, []byte("https://localhost:51744/60631c8695bf041a808759a05de53e36a73316aacb502824fabbb0c6055637c5.git")))

	locator := config.NewLocator(config.Config)
	serverSocketPath, stop := runRepoServer(t, locator, testhelper.WithInternalSocket(config.Config))
	defer stop()

	repoClient, conn := NewRepositoryClient(t, serverSocketPath)
	defer conn.Close()

	_, err = repoClient.OptimizeRepository(ctx, &gitalypb.OptimizeRepositoryRequest{Repository: testRepo})
	require.NoError(t, err)

	confFileData, err = ioutil.ReadFile(filepath.Join(testRepoPath, "config"))
	require.NoError(t, err)
	require.False(t, bytes.Contains(confFileData, []byte("http://localhost:51744/60631c8695bf041a808759a05de53e36a73316aacb502824fabbb0c6055637c1.git")))
	require.False(t, bytes.Contains(confFileData, []byte("http://localhost:51744/60631c8695bf041a808759a05de53e36a73316aacb502824fabbb0c6055637c2.git")))
	require.True(t, bytes.Contains(confFileData, []byte("http://localhost:51744/60631c8695bf041a808759a05de53e36a73316aacb502824fabbb0c6055637c3")))
	require.True(t, bytes.Contains(confFileData, []byte("http://localhost:51744/60631c8695bf041a808759a05de53e36a73316aacb502824fabbb0c6055637c4.git")))
	require.False(t, bytes.Contains(confFileData, []byte("http://localhost:51744/60631c8695bf041a808759a05de53e36a73316aacb502824fabbb0c6055637c5.git")))
	require.False(t, bytes.Contains(confFileData, []byte("http://extraHeader/extraheader/EXTRAHEADER.git.extraHeader")))
	require.True(t, bytes.Contains(confFileData, []byte("https://localhost:51744/60631c8695bf041a808759a05de53e36a73316aacb502824fabbb0c6055637c5.git")))

	require.Equal(t, getNewestPackfileModtime(t, testRepoPath), newestsPackfileTime, "there should not have been a new packfile created")

	testRepo, testRepoPath, cleanupBare := gittest.InitBareRepo(t)
	defer cleanupBare()

	blobs := 10
	blobIDs := gittest.WriteBlobs(t, testRepoPath, blobs)

	updater, err := updateref.New(ctx, config.Config, git.NewExecCommandFactory(config.Config), testRepo)
	require.NoError(t, err)

	for _, blobID := range blobIDs {
		commitID := gittest.CommitBlobWithName(t, config.Config, testRepoPath, blobID, blobID, "adding another blob....")
		require.NoError(t, updater.Create(git.ReferenceName("refs/heads/"+blobID), commitID))
	}

	require.NoError(t, updater.Wait())

	bitmaps, err := filepath.Glob(filepath.Join(testRepoPath, "objects", "pack", "*.bitmap"))
	require.NoError(t, err)
	require.Empty(t, bitmaps)

	mrRefs := filepath.Join(testRepoPath, "refs/merge-requests")
	emptyRef := filepath.Join(mrRefs, "1")
	require.NoError(t, os.MkdirAll(emptyRef, 0755))
	require.DirExists(t, emptyRef, "sanity check for empty ref dir existence")

	// optimize repository on a repository without a bitmap should call repack full
	_, err = repoClient.OptimizeRepository(ctx, &gitalypb.OptimizeRepositoryRequest{Repository: testRepo})
	require.NoError(t, err)

	bitmaps, err = filepath.Glob(filepath.Join(testRepoPath, "objects", "pack", "*.bitmap"))
	require.NoError(t, err)
	require.NotEmpty(t, bitmaps)

	// Empty directories should exist because they're too recent.
	require.DirExists(t, emptyRef)
	require.DirExists(t, mrRefs)
	require.FileExists(t,
		filepath.Join(testRepoPath, "refs/heads", blobIDs[0]),
		"unpacked refs should never be removed",
	)

	// Change the modification time to me older than a day and retry the call. Empty
	// directories must now be deleted.
	oneDayAgo := time.Now().Add(-24 * time.Hour)
	require.NoError(t, os.Chtimes(emptyRef, oneDayAgo, oneDayAgo))
	require.NoError(t, os.Chtimes(mrRefs, oneDayAgo, oneDayAgo))

	_, err = repoClient.OptimizeRepository(ctx, &gitalypb.OptimizeRepositoryRequest{Repository: testRepo})
	require.NoError(t, err)

	testhelper.AssertPathNotExists(t, emptyRef)
	testhelper.AssertPathNotExists(t, mrRefs)
}

func TestOptimizeRepositoryValidation(t *testing.T) {
	testRepo, _, cleanup := gittest.CloneRepo(t)
	defer cleanup()

	testCases := []struct {
		desc              string
		repo              *gitalypb.Repository
		expectedErrorCode codes.Code
	}{
		{
			desc:              "empty repository",
			repo:              nil,
			expectedErrorCode: codes.InvalidArgument,
		},
		{
			desc:              "invalid repository storage",
			repo:              &gitalypb.Repository{StorageName: "non-existent", RelativePath: testRepo.GetRelativePath()},
			expectedErrorCode: codes.InvalidArgument,
		},
		{
			desc:              "invalid repository path",
			repo:              &gitalypb.Repository{StorageName: testRepo.GetStorageName(), RelativePath: "/path/not/exist"},
			expectedErrorCode: codes.NotFound,
		},
	}

	locator := config.NewLocator(config.Config)
	serverSocketPath, stop := runRepoServer(t, locator)
	defer stop()

	repoClient, conn := NewRepositoryClient(t, serverSocketPath)
	defer conn.Close()

	ctx, cancel := testhelper.Context()
	defer cancel()

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			_, err := repoClient.OptimizeRepository(ctx, &gitalypb.OptimizeRepositoryRequest{Repository: tc.repo})
			require.Error(t, err)
			testhelper.RequireGrpcError(t, err, tc.expectedErrorCode)
		})
	}

	_, err := repoClient.OptimizeRepository(ctx, &gitalypb.OptimizeRepositoryRequest{Repository: testRepo})
	require.NoError(t, err)
}
