package repository

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/v14/internal/safe"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v14/internal/transaction/txinfo"
	"gitlab.com/gitlab-org/gitaly/v14/internal/transaction/voting"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"google.golang.org/grpc/peer"
)

func TestCreateRepository(t *testing.T) {
	t.Parallel()

	ctx, cancel := testhelper.Context()
	defer cancel()

	cfg := testcfg.Build(t)

	txManager := &transaction.MockManager{}
	locator := config.NewLocator(cfg)
	gitCmdFactory := gittest.NewCommandFactory(t, cfg)

	server := &server{
		cfg:           cfg,
		locator:       locator,
		txManager:     txManager,
		gitCmdFactory: gitCmdFactory,
	}

	var votesByPhase map[voting.Phase]int

	for _, tc := range []struct {
		desc   string
		setup  func(t *testing.T, repo *gitalypb.Repository, repoPath string)
		seed   func(t *testing.T, repo *gitalypb.Repository, repoPath string) error
		verify func(
			t *testing.T,
			tempRepo *gitalypb.Repository,
			tempRepoPath string,
			realRepo *gitalypb.Repository,
			realRepoPath string,
		)
		transactional bool
		expectedErr   error
	}{
		{
			desc: "no seeding",
			verify: func(t *testing.T, tempRepo *gitalypb.Repository, tempRepoPath string, realRepo *gitalypb.Repository, realRepoPath string) {
				// Assert that the temporary repository does not exist anymore.
				require.NoDirExists(t, tempRepoPath)

				// But the new repository must exist.
				isBareRepo := gittest.Exec(t, cfg, "-C", realRepoPath, "rev-parse", "--is-bare-repository")
				require.Equal(t, "true", text.ChompBytes(isBareRepo))
			},
		},
		{
			desc: "seeding",
			seed: func(t *testing.T, repo *gitalypb.Repository, _ string) error {
				// We're using the command factory on purpose here to assert that we
				// can execute regular Git commands on the temporary repository.
				cmd, err := gitCmdFactory.New(ctx, repo, git.SubCmd{
					Name: "config",
					Args: []string{"custom.key", "value"},
				})
				require.NoError(t, err)
				require.NoError(t, cmd.Wait())
				return nil
			},
			verify: func(t *testing.T, tempRepo *gitalypb.Repository, tempRepoPath string, realRepo *gitalypb.Repository, realRepoPath string) {
				value := gittest.Exec(t, cfg, "-C", realRepoPath, "config", "custom.key")
				require.Equal(t, "value", text.ChompBytes(value))
			},
		},
		{
			desc: "error while seeding",
			seed: func(t *testing.T, repo *gitalypb.Repository, _ string) error {
				return errors.New("some error")
			},
			verify: func(t *testing.T, tempRepo *gitalypb.Repository, tempRepoPath string, realRepo *gitalypb.Repository, realRepoPath string) {
				require.NoDirExists(t, realRepoPath)
				require.NoDirExists(t, tempRepoPath)
			},
			expectedErr: errors.New("some error"),
		},
		{
			desc: "preexisting directory",
			setup: func(t *testing.T, repo *gitalypb.Repository, repoPath string) {
				require.NoError(t, os.MkdirAll(repoPath, 0o777))
			},
			verify: func(t *testing.T, tempRepo *gitalypb.Repository, tempRepoPath string, realRepo *gitalypb.Repository, realRepoPath string) {
				require.NoDirExists(t, tempRepoPath)

				require.DirExists(t, realRepoPath)
				dirEntries, err := os.ReadDir(realRepoPath)
				require.NoError(t, err)
				require.Empty(t, dirEntries, "directory should not have been modified")
			},
			expectedErr: helper.ErrAlreadyExistsf("repository exists already"),
		},
		{
			desc: "locked",
			setup: func(t *testing.T, repo *gitalypb.Repository, repoPath string) {
				require.NoError(t, os.MkdirAll(filepath.Dir(repoPath), 0o777))

				// Lock the target repository such that we must fail.
				lock, err := os.Create(repoPath + ".lock")
				require.NoError(t, err)
				require.NoError(t, lock.Close())
			},
			verify: func(t *testing.T, tempRepo *gitalypb.Repository, tempRepoPath string, realRepo *gitalypb.Repository, realRepoPath string) {
				require.NoDirExists(t, tempRepoPath)
				require.NoDirExists(t, realRepoPath)
				require.FileExists(t, realRepoPath+".lock")
			},
			expectedErr: fmt.Errorf("locking repository: %w", safe.ErrFileAlreadyLocked),
		},
		{
			desc:          "successful transaction",
			transactional: true,
			setup: func(t *testing.T, repo *gitalypb.Repository, repoPath string) {
				votesByPhase = map[voting.Phase]int{}
				txManager.VoteFn = func(_ context.Context, _ txinfo.Transaction, _ voting.Vote, phase voting.Phase) error {
					votesByPhase[phase]++
					return nil
				}
			},
			verify: func(t *testing.T, tempRepo *gitalypb.Repository, tempRepoPath string, realRepo *gitalypb.Repository, realRepoPath string) {
				require.Equal(t, map[voting.Phase]int{
					voting.Prepared:  1,
					voting.Committed: 1,
				}, votesByPhase)
			},
		},
		{
			desc:          "failing preparatory vote",
			transactional: true,
			setup: func(t *testing.T, repo *gitalypb.Repository, repoPath string) {
				txManager.VoteFn = func(context.Context, txinfo.Transaction, voting.Vote, voting.Phase) error {
					return errors.New("vote failed")
				}
			},
			verify: func(t *testing.T, tempRepo *gitalypb.Repository, tempRepoPath string, realRepo *gitalypb.Repository, realRepoPath string) {
				require.NoDirExists(t, tempRepoPath)
				require.NoDirExists(t, realRepoPath)
			},
			expectedErr: helper.ErrFailedPreconditionf("preparatory vote: %w", errors.New("vote failed")),
		},
		{
			desc:          "failing post-commit vote",
			transactional: true,
			setup: func(t *testing.T, repo *gitalypb.Repository, repoPath string) {
				txManager.VoteFn = func(_ context.Context, _ txinfo.Transaction, _ voting.Vote, phase voting.Phase) error {
					if phase == voting.Prepared {
						return nil
					}
					return errors.New("vote failed")
				}
			},
			verify: func(t *testing.T, tempRepo *gitalypb.Repository, tempRepoPath string, realRepo *gitalypb.Repository, realRepoPath string) {
				require.NoDirExists(t, tempRepoPath)

				// The second vote is only a confirming vote that the node did the
				// change. So if the second vote fails, then the change must have
				// been performed and thus we'd see the repository.
				require.DirExists(t, realRepoPath)
			},
			expectedErr: helper.ErrFailedPreconditionf("committing vote: %w", errors.New("vote failed")),
		},
		{
			desc:          "voting happens after lock",
			transactional: true,
			setup: func(t *testing.T, repo *gitalypb.Repository, repoPath string) {
				// We both set up transactions and create the lock. Given that we
				// should try locking the repository before casting any votes, we do
				// not expect to see a voting error.

				require.NoError(t, os.MkdirAll(filepath.Dir(repoPath), 0o777))
				lock, err := os.Create(repoPath + ".lock")
				require.NoError(t, err)
				require.NoError(t, lock.Close())

				txManager.VoteFn = func(context.Context, txinfo.Transaction, voting.Vote, voting.Phase) error {
					require.FailNow(t, "no votes should have happened")
					return nil
				}
			},
			verify: func(t *testing.T, tempRepo *gitalypb.Repository, tempRepoPath string, realRepo *gitalypb.Repository, realRepoPath string) {
				require.NoDirExists(t, tempRepoPath)
				require.NoDirExists(t, realRepoPath)
			},
			expectedErr: fmt.Errorf("locking repository: %w", errors.New("file already locked")),
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			repo := &gitalypb.Repository{
				StorageName:  cfg.Storages[0].Name,
				RelativePath: gittest.NewRepositoryName(t, true),
			}

			if tc.transactional {
				var err error
				ctx, err = txinfo.InjectTransaction(ctx, 1, "node", true)
				require.NoError(t, err)
				ctx = peer.NewContext(ctx, &peer.Peer{})
			}

			repoPath, err := locator.GetPath(repo)
			require.NoError(t, err)

			if tc.setup != nil {
				tc.setup(t, repo, repoPath)
			}

			var tempRepo *gitalypb.Repository
			require.Equal(t, tc.expectedErr, server.createRepository(ctx, repo, func(tr *gitalypb.Repository) error {
				tempRepo = tr

				// The temporary repository must have been created in Gitaly's
				// temporary storage path.
				require.Equal(t, repo.StorageName, tempRepo.StorageName)
				require.True(t, strings.HasPrefix(tempRepo.RelativePath, "+gitaly/tmp/repo"))

				// Verify that the temporary repository exists and is a real Git
				// repository.
				tempRepoPath, err := locator.GetRepoPath(tempRepo)
				require.NoError(t, err)
				isBareRepo := gittest.Exec(t, cfg, "-C", tempRepoPath, "rev-parse", "--is-bare-repository")
				require.Equal(t, "true", text.ChompBytes(isBareRepo))

				if tc.seed != nil {
					return tc.seed(t, tempRepo, tempRepoPath)
				}

				return nil
			}))

			var tempRepoPath string
			if tempRepo != nil {
				tempRepoPath, err = locator.GetPath(tempRepo)
				require.NoError(t, err)
			}

			require.NotNil(t, tc.verify, "test must verify results")
			tc.verify(t, tempRepo, tempRepoPath, repo, repoPath)
		})
	}
}
