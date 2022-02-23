package repository

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/backchannel"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/stats"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/v14/internal/metadata"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v14/internal/transaction/txinfo"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

func TestMidxWrite(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, repo, repoPath, client := setupRepositoryService(ctx, t)

	//nolint:staticcheck
	_, err := client.MidxRepack(ctx, &gitalypb.MidxRepackRequest{Repository: repo})
	assert.NoError(t, err)

	require.FileExists(t,
		filepath.Join(repoPath, MidxRelPath),
		"multi-pack-index should exist after running MidxRepack",
	)

	configEntries := gittest.Exec(t, cfg, "-C", repoPath, "config", "--local", "--list")
	require.NotContains(t, configEntries, "core.muiltipackindex")
}

func TestMidxRewrite(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	_, repo, repoPath, client := setupRepositoryService(ctx, t)

	midxPath := filepath.Join(repoPath, MidxRelPath)

	// Create an invalid multi-pack-index file
	// with mtime update being the basis for comparison
	require.NoError(t, os.WriteFile(midxPath, nil, 0o644))
	require.NoError(t, os.Chtimes(midxPath, time.Time{}, time.Time{}))
	info, err := os.Stat(midxPath)
	require.NoError(t, err)
	mt := info.ModTime()

	//nolint:staticcheck
	_, err = client.MidxRepack(ctx, &gitalypb.MidxRepackRequest{Repository: repo})
	require.NoError(t, err)

	require.FileExists(t,
		filepath.Join(repoPath, MidxRelPath),
		"multi-pack-index should exist after running MidxRepack",
	)

	assertModTimeAfter(t, mt, midxPath)
}

func TestMidxRepack(t *testing.T) {
	t.Parallel()

	ctx := testhelper.Context(t)
	cfg, repo, repoPath, client := setupRepositoryService(ctx, t)

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

	//nolint:staticcheck
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

func TestMidxRepack_transactional(t *testing.T) {
	t.Parallel()
	ctx := testhelper.Context(t)

	txManager := transaction.NewTrackingManager()

	cfg, repo, repoPath, client := setupRepositoryService(ctx, t, testserver.WithTransactionManager(txManager))

	// Reset the votes after creating the test repository.
	txManager.Reset()

	ctx, err := txinfo.InjectTransaction(ctx, 1, "node", true)
	require.NoError(t, err)
	ctx = peer.NewContext(ctx, &peer.Peer{
		AuthInfo: backchannel.WithID(nil, 1234),
	})
	ctx = metadata.IncomingToOutgoing(ctx)

	//nolint:staticcheck
	_, err = client.MidxRepack(ctx, &gitalypb.MidxRepackRequest{
		Repository: repo,
	})
	require.NoError(t, err)

	require.Equal(t, 2, len(txManager.Votes()))

	multiPackIndex := gittest.Exec(t, cfg, "-C", repoPath, "config", "core.multiPackIndex")
	require.Equal(t, "true", text.ChompBytes(multiPackIndex))
}

func TestMidxRepackExpire(t *testing.T) {
	t.Parallel()
	cfg, client := setupRepositoryServiceWithoutRepo(t)

	for _, packsAdded := range []int{3, 5, 11, 20} {
		t.Run(fmt.Sprintf("Test repack expire with %d added packs", packsAdded),
			func(t *testing.T) {
				ctx := testhelper.Context(t)
				repo, repoPath := gittest.CreateRepository(ctx, t, cfg, gittest.CreateRepositoryConfig{
					Seed: gittest.SeedGitLabTest,
				})

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

					//nolint:staticcheck
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
	//nolint:staticcheck
	_, err := client.RepackFull(ctx, &gitalypb.RepackFullRequest{Repository: repo, CreateBitmap: true})
	require.NoError(t, err)

	// create some pack files with different sizes
	for i := 0; i < packCount; i++ {
		for y := packCount + 1 - i; y > 0; y-- {
			branch := fmt.Sprintf("branch-%d-%d", i, y)
			gittest.WriteCommit(t, cfg, repoPath, gittest.WithMessage(branch), gittest.WithBranch(branch))
		}

		//nolint:staticcheck
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

func TestMidxRepack_validationChecks(t *testing.T) {
	t.Parallel()
	cfg, client := setupRepositoryServiceWithoutRepo(t, testserver.WithDisablePraefect())
	ctx := testhelper.Context(t)

	for _, tc := range []struct {
		desc   string
		req    *gitalypb.MidxRepackRequest
		expErr error
	}{
		{
			desc:   "no repository",
			req:    &gitalypb.MidxRepackRequest{},
			expErr: status.Error(codes.InvalidArgument, "empty Repository"),
		},
		{
			desc:   "invalid storage",
			req:    &gitalypb.MidxRepackRequest{Repository: &gitalypb.Repository{StorageName: "invalid"}},
			expErr: status.Error(codes.InvalidArgument, `GetStorageByName: no such storage: "invalid"`),
		},
		{
			desc:   "not existing repository",
			req:    &gitalypb.MidxRepackRequest{Repository: &gitalypb.Repository{StorageName: cfg.Storages[0].Name, RelativePath: "invalid"}},
			expErr: status.Error(codes.NotFound, fmt.Sprintf(`GetRepoPath: not a git repository: "%s/invalid"`, cfg.Storages[0].Path)),
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			//nolint:staticcheck
			_, err := client.MidxRepack(ctx, tc.req)
			testhelper.RequireGrpcError(t, tc.expErr, err)
		})
	}
}
