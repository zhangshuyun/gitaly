package repository

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/internal/git/stats"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

func TestMidxWrite(t *testing.T) {
	cfg, repo, repoPath, client := setupRepositoryService(t)

	ctx, cancel := testhelper.Context()
	defer cancel()

	_, err := client.MidxRepack(ctx, &gitalypb.MidxRepackRequest{Repository: repo})
	assert.NoError(t, err)

	require.FileExists(t,
		filepath.Join(repoPath, MidxRelPath),
		"multi-pack-index should exist after running MidxRepack",
	)

	repoCfgPath := filepath.Join(repoPath, "config")

	cfgF, err := os.Open(repoCfgPath)
	require.NoError(t, err)
	defer cfgF.Close()

	cfgCmd, err := localrepo.NewTestRepo(t, cfg, repo).Config().GetRegexp(ctx, "core.multipackindex", git.ConfigGetRegexpOpts{})
	require.NoError(t, err)
	require.Equal(t, []git.ConfigPair{{Key: "core.multipackindex", Value: "true"}}, cfgCmd)
}

func TestMidxRewrite(t *testing.T) {
	_, repo, repoPath, client := setupRepositoryService(t)

	ctx, cancel := testhelper.Context()
	defer cancel()

	midxPath := filepath.Join(repoPath, MidxRelPath)

	// Create an invalid multi-pack-index file
	// with mtime update being the basis for comparison
	require.NoError(t, ioutil.WriteFile(midxPath, nil, 0644))
	require.NoError(t, os.Chtimes(midxPath, time.Time{}, time.Time{}))
	info, err := os.Stat(midxPath)
	require.NoError(t, err)
	mt := info.ModTime()

	_, err = client.MidxRepack(ctx, &gitalypb.MidxRepackRequest{Repository: repo})
	require.NoError(t, err)

	require.FileExists(t,
		filepath.Join(repoPath, MidxRelPath),
		"multi-pack-index should exist after running MidxRepack",
	)

	assertModTimeAfter(t, mt, midxPath)
}

func TestMidxRepack(t *testing.T) {
	cfg, repo, repoPath, client := setupRepositoryService(t)

	ctx, cancel := testhelper.Context()
	defer cancel()

	// add some pack files with different sizes
	packsAdded := 5
	addPackFiles(t, ctx, cfg, client, repo, repoPath, packsAdded, true)

	// record pack count
	actualCount, err := stats.PackfilesCount(repoPath)
	require.NoError(t, err)
	require.Equal(t,
		packsAdded+1, // expect
		actualCount,  // actual
		"New pack files should have been created",
	)

	_, err = client.MidxRepack(
		ctx,
		&gitalypb.MidxRepackRequest{
			Repository: repo,
		},
	)
	require.NoError(t, err)

	actualCount, err = stats.PackfilesCount(repoPath)
	require.NoError(t, err)
	require.Equal(t,
		packsAdded+2, // expect
		actualCount,  // actual
		"At least 1 pack file should have been created",
	)

	newPackFile := findNewestPackFile(t, repoPath)
	assert.True(t, newPackFile.ModTime().After(time.Time{}))
}

func TestMidxRepackExpire(t *testing.T) {
	cfg, client := setupRepositoryServiceWithoutRepo(t)

	for _, packsAdded := range []int{3, 5, 11, 20} {
		t.Run(fmt.Sprintf("Test repack expire with %d added packs", packsAdded),
			func(t *testing.T) {
				repo, repoPath, cleanupFn := gittest.CloneRepoAtStorage(t, cfg.Storages[0], t.Name())
				t.Cleanup(cleanupFn)

				ctx, cancel := testhelper.Context()
				defer cancel()

				// add some pack files with different sizes
				addPackFiles(t, ctx, cfg, client, repo, repoPath, packsAdded, false)

				// record pack count
				actualCount, err := stats.PackfilesCount(repoPath)
				require.NoError(t, err)
				require.Equal(t,
					packsAdded+1, // expect
					actualCount,  // actual
					"New pack files should have been created",
				)

				// here we assure that for n packCount
				// we should need no more than n interation(s)
				// for the pack files to be consolidated into
				// a new second biggest pack
				i := 0
				packCount := packsAdded + 1
				for {
					if i > packsAdded+1 {
						break
					}
					i++

					_, err := client.MidxRepack(
						ctx,
						&gitalypb.MidxRepackRequest{
							Repository: repo,
						},
					)
					require.NoError(t, err)

					packCount, err = stats.PackfilesCount(repoPath)
					require.NoError(t, err)

					if packCount == 2 {
						break
					}
				}

				require.Equal(t,
					2,         // expect
					packCount, // actual
					fmt.Sprintf(
						"all small packs should be consolidated to a second biggest pack "+
							"after at most %d iterations (actual %d))",
						packCount,
						i,
					),
				)
			})
	}
}

// findNewestPackFile returns the latest created pack file in repo's odb
func findNewestPackFile(t *testing.T, repoPath string) os.FileInfo {
	t.Helper()

	files, err := stats.GetPackfiles(repoPath)
	require.NoError(t, err)

	var newestPack os.FileInfo
	for _, f := range files {
		if newestPack == nil || f.ModTime().After(newestPack.ModTime()) {
			newestPack = f
		}
	}
	require.NotNil(t, newestPack)

	return newestPack
}

// addPackFiles creates some packfiles by
// creating some commits objects and repack them.
func addPackFiles(
	t *testing.T,
	ctx context.Context,
	cfg config.Cfg,
	client gitalypb.RepositoryServiceClient,
	repo *gitalypb.Repository,
	repoPath string,
	packCount int,
	resetModTime bool,
) {
	t.Helper()

	// do a full repack to ensure we start with 1 pack
	_, err := client.RepackFull(ctx, &gitalypb.RepackFullRequest{Repository: repo, CreateBitmap: true})
	require.NoError(t, err)

	// create some pack files with different sizes
	for i := 0; i < packCount; i++ {
		for y := packCount + 1 - i; y > 0; y-- {
			gittest.CreateCommitOnNewBranch(t, cfg, repoPath)
		}

		_, err = client.RepackIncremental(ctx, &gitalypb.RepackIncrementalRequest{Repository: repo})
		require.NoError(t, err)
	}

	// reset mtime of packfile to mark them separately
	// for comparison purpose
	if resetModTime {
		packDir := filepath.Join(repoPath, "objects/pack/")

		files, err := stats.GetPackfiles(repoPath)
		require.NoError(t, err)

		for _, f := range files {
			require.NoError(t, os.Chtimes(filepath.Join(packDir, f.Name()), time.Time{}, time.Time{}))
		}
	}
}
