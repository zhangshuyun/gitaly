package repository

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/stats"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
)

func getNewestPackfileModtime(t *testing.T, repoPath string) time.Time {
	t.Helper()

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
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, repoProto, repoPath, client := setupRepositoryService(ctx, t)

	gittest.Exec(t, cfg, "-C", repoPath, "repack", "-A", "-b")
	gittest.Exec(t, cfg, "-C", repoPath, "commit-graph", "write", "--size-multiple=4", "--split=replace", "--reachable", "--changed-paths")

	hasBitmap, err := stats.HasBitmap(repoPath)
	require.NoError(t, err)
	require.True(t, hasBitmap, "expect a bitmap since we just repacked with -b")

	missingBloomFilters, err := stats.IsMissingBloomFilters(repoPath)
	require.NoError(t, err)
	require.False(t, missingBloomFilters)

	// get timestamp of latest packfile
	newestsPackfileTime := getNewestPackfileModtime(t, repoPath)

	gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("master"))

	gittest.Exec(t, cfg, "-C", repoPath, "config", "http.http://localhost:51744/60631c8695bf041a808759a05de53e36a73316aacb502824fabbb0c6055637c1.git.extraHeader", "Authorization: Basic secret-password")
	gittest.Exec(t, cfg, "-C", repoPath, "config", "http.http://localhost:51744/60631c8695bf041a808759a05de53e36a73316aacb502824fabbb0c6055637c2.git.extraHeader", "Authorization: Basic secret-password")
	gittest.Exec(t, cfg, "-C", repoPath, "config", "randomStart-http.http://localhost:51744/60631c8695bf041a808759a05de53e36a73316aacb502824fabbb0c6055637c3.git.extraHeader", "Authorization: Basic secret-password")
	gittest.Exec(t, cfg, "-C", repoPath, "config", "http.http://localhost:51744/60631c8695bf041a808759a05de53e36a73316aacb502824fabbb0c6055637c4.git.extraHeader-randomEnd", "Authorization: Basic secret-password")
	gittest.Exec(t, cfg, "-C", repoPath, "config", "hTTp.http://localhost:51744/60631c8695bf041a808759a05de53e36a73316aacb502824fabbb0c6055637c5.git.ExtrAheaDeR", "Authorization: Basic secret-password")
	gittest.Exec(t, cfg, "-C", repoPath, "config", "http.http://extraHeader/extraheader/EXTRAHEADER.git.extraHeader", "Authorization: Basic secret-password")
	gittest.Exec(t, cfg, "-C", repoPath, "config", "https.https://localhost:51744/60631c8695bf041a808759a05de53e36a73316aacb502824fabbb0c6055637c5.git.extraHeader", "Authorization: Basic secret-password")
	confFileData := testhelper.MustReadFile(t, filepath.Join(repoPath, "config"))
	require.True(t, bytes.Contains(confFileData, []byte("http://localhost:51744/60631c8695bf041a808759a05de53e36a73316aacb502824fabbb0c6055637c1.git")))
	require.True(t, bytes.Contains(confFileData, []byte("http://localhost:51744/60631c8695bf041a808759a05de53e36a73316aacb502824fabbb0c6055637c2.git")))
	require.True(t, bytes.Contains(confFileData, []byte("http://localhost:51744/60631c8695bf041a808759a05de53e36a73316aacb502824fabbb0c6055637c3")))
	require.True(t, bytes.Contains(confFileData, []byte("http://localhost:51744/60631c8695bf041a808759a05de53e36a73316aacb502824fabbb0c6055637c4.git")))
	require.True(t, bytes.Contains(confFileData, []byte("http://localhost:51744/60631c8695bf041a808759a05de53e36a73316aacb502824fabbb0c6055637c5.git")))
	require.True(t, bytes.Contains(confFileData, []byte("http://extraHeader/extraheader/EXTRAHEADER.git")))
	require.True(t, bytes.Contains(confFileData, []byte("https://localhost:51744/60631c8695bf041a808759a05de53e36a73316aacb502824fabbb0c6055637c5.git")))

	_, err = client.OptimizeRepository(ctx, &gitalypb.OptimizeRepositoryRequest{Repository: repoProto})
	require.NoError(t, err)

	confFileData = testhelper.MustReadFile(t, filepath.Join(repoPath, "config"))
	require.False(t, bytes.Contains(confFileData, []byte("http://localhost:51744/60631c8695bf041a808759a05de53e36a73316aacb502824fabbb0c6055637c1.git")))
	require.False(t, bytes.Contains(confFileData, []byte("http://localhost:51744/60631c8695bf041a808759a05de53e36a73316aacb502824fabbb0c6055637c2.git")))
	require.True(t, bytes.Contains(confFileData, []byte("http://localhost:51744/60631c8695bf041a808759a05de53e36a73316aacb502824fabbb0c6055637c3")))
	require.True(t, bytes.Contains(confFileData, []byte("http://localhost:51744/60631c8695bf041a808759a05de53e36a73316aacb502824fabbb0c6055637c4.git")))
	require.False(t, bytes.Contains(confFileData, []byte("http://localhost:51744/60631c8695bf041a808759a05de53e36a73316aacb502824fabbb0c6055637c5.git")))
	require.False(t, bytes.Contains(confFileData, []byte("http://extraHeader/extraheader/EXTRAHEADER.git.extraHeader")))
	require.True(t, bytes.Contains(confFileData, []byte("https://localhost:51744/60631c8695bf041a808759a05de53e36a73316aacb502824fabbb0c6055637c5.git")))

	require.Equal(t, getNewestPackfileModtime(t, repoPath), newestsPackfileTime, "there should not have been a new packfile created")

	testRepoProto, testRepoPath := gittest.CreateRepository(ctx, t, cfg)

	blobs := 10
	blobIDs := gittest.WriteBlobs(t, cfg, testRepoPath, blobs)

	for _, blobID := range blobIDs {
		gittest.WriteCommit(t, cfg, testRepoPath,
			gittest.WithTreeEntries(gittest.TreeEntry{
				OID: git.ObjectID(blobID), Mode: "100644", Path: "blob",
			}),
			gittest.WithBranch(blobID),
			gittest.WithParents(),
		)
	}

	bitmaps, err := filepath.Glob(filepath.Join(testRepoPath, "objects", "pack", "*.bitmap"))
	require.NoError(t, err)
	require.Empty(t, bitmaps)

	mrRefs := filepath.Join(testRepoPath, "refs/merge-requests")
	emptyRef := filepath.Join(mrRefs, "1")
	require.NoError(t, os.MkdirAll(emptyRef, 0o755))
	require.DirExists(t, emptyRef, "sanity check for empty ref dir existence")

	// optimize repository on a repository without a bitmap should call repack full
	_, err = client.OptimizeRepository(ctx, &gitalypb.OptimizeRepositoryRequest{Repository: testRepoProto})
	require.NoError(t, err)

	bitmaps, err = filepath.Glob(filepath.Join(testRepoPath, "objects", "pack", "*.bitmap"))
	require.NoError(t, err)
	require.NotEmpty(t, bitmaps)

	missingBloomFilters, err = stats.IsMissingBloomFilters(testRepoPath)
	require.NoError(t, err)
	require.False(t, missingBloomFilters)

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

	_, err = client.OptimizeRepository(ctx, &gitalypb.OptimizeRepositoryRequest{Repository: testRepoProto})
	require.NoError(t, err)

	require.NoFileExists(t, emptyRef)
	require.NoFileExists(t, mrRefs)
}

func TestOptimizeRepositoryValidation(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	_, repo, _, client := setupRepositoryService(ctx, t)

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
			repo:              &gitalypb.Repository{StorageName: "non-existent", RelativePath: repo.GetRelativePath()},
			expectedErrorCode: codes.InvalidArgument,
		},
		{
			desc:              "invalid repository path",
			repo:              &gitalypb.Repository{StorageName: repo.GetStorageName(), RelativePath: "/path/not/exist"},
			expectedErrorCode: codes.NotFound,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			_, err := client.OptimizeRepository(ctx, &gitalypb.OptimizeRepositoryRequest{Repository: tc.repo})
			require.Error(t, err)
			testhelper.RequireGrpcCode(t, err, tc.expectedErrorCode)
		})
	}

	_, err := client.OptimizeRepository(ctx, &gitalypb.OptimizeRepositoryRequest{Repository: repo})
	require.NoError(t, err)
}

func TestNeedsRepacking(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, _ := setupRepositoryServiceWithoutRepo(t)

	for _, tc := range []struct {
		desc           string
		setup          func(t *testing.T) *gitalypb.Repository
		expectedErr    error
		expectedNeeded bool
		expectedConfig repackCommandConfig
	}{
		{
			desc: "empty repo",
			setup: func(t *testing.T) *gitalypb.Repository {
				repoProto, _ := gittest.CreateRepository(ctx, t, cfg)
				return repoProto
			},
			// This is a bug: if the repo is empty then we wouldn't ever generate a
			// packfile, but we determine a repack is needed because it's missing a
			// bitmap. It's a rather benign bug though: git-repack(1) will exit
			// immediately because it knows that there's nothing to repack.
			expectedNeeded: true,
			expectedConfig: repackCommandConfig{
				fullRepack:  true,
				writeBitmap: true,
			},
		},
		{
			desc: "missing bitmap",
			setup: func(t *testing.T) *gitalypb.Repository {
				repoProto, _ := gittest.CreateRepository(ctx, t, cfg, gittest.CreateRepositoryConfig{
					Seed: gittest.SeedGitLabTest,
				})
				return repoProto
			},
			expectedNeeded: true,
			expectedConfig: repackCommandConfig{
				fullRepack:  true,
				writeBitmap: true,
			},
		},
		{
			desc: "missing bitmap with alternate",
			setup: func(t *testing.T) *gitalypb.Repository {
				repoProto, repoPath := gittest.CreateRepository(ctx, t, cfg, gittest.CreateRepositoryConfig{
					Seed: gittest.SeedGitLabTest,
				})

				// Create the alternates file. If it exists, then we shouldn't try
				// to generate a bitmap.
				require.NoError(t, os.WriteFile(filepath.Join(repoPath, "objects", "info", "alternates"), nil, 0o755))

				return repoProto
			},
			expectedNeeded: true,
			expectedConfig: repackCommandConfig{
				fullRepack:  true,
				writeBitmap: false,
			},
		},
		{
			desc: "missing commit-graph",
			setup: func(t *testing.T) *gitalypb.Repository {
				repoProto, repoPath := gittest.CreateRepository(ctx, t, cfg, gittest.CreateRepositoryConfig{
					Seed: gittest.SeedGitLabTest,
				})

				gittest.Exec(t, cfg, "-C", repoPath, "repack", "-Ad", "--write-bitmap-index")

				return repoProto
			},
			expectedNeeded: true,
			expectedConfig: repackCommandConfig{
				fullRepack:  true,
				writeBitmap: true,
			},
		},
		{
			desc: "commit-graph without bloom filters",
			setup: func(t *testing.T) *gitalypb.Repository {
				repoProto, repoPath := gittest.CreateRepository(ctx, t, cfg, gittest.CreateRepositoryConfig{
					Seed: gittest.SeedGitLabTest,
				})

				gittest.Exec(t, cfg, "-C", repoPath, "repack", "-Ad", "--write-bitmap-index")
				gittest.Exec(t, cfg, "-C", repoPath, "commit-graph", "write")

				return repoProto
			},
			expectedNeeded: true,
			expectedConfig: repackCommandConfig{
				fullRepack:  true,
				writeBitmap: true,
			},
		},
		{
			desc: "no repack needed",
			setup: func(t *testing.T) *gitalypb.Repository {
				repoProto, repoPath := gittest.CreateRepository(ctx, t, cfg, gittest.CreateRepositoryConfig{
					Seed: gittest.SeedGitLabTest,
				})

				gittest.Exec(t, cfg, "-C", repoPath, "repack", "-Ad", "--write-bitmap-index")
				gittest.Exec(t, cfg, "-C", repoPath, "commit-graph", "write", "--changed-paths", "--split")

				return repoProto
			},
			expectedNeeded: false,
		},
	} {
		tc := tc
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()

			repoProto := tc.setup(t)
			repo := localrepo.NewTestRepo(t, cfg, repoProto)

			repackNeeded, repackCfg, err := needsRepacking(repo)
			require.Equal(t, tc.expectedErr, err)
			require.Equal(t, tc.expectedNeeded, repackNeeded)
			require.Equal(t, tc.expectedConfig, repackCfg)
		})
	}
}
