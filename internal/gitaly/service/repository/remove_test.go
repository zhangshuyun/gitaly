package repository

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/v14/internal/metadata"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v14/internal/transaction/txinfo"
	"gitlab.com/gitlab-org/gitaly/v14/internal/transaction/voting"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

func TestRemoveRepository(t *testing.T) {
	t.Parallel()
	_, repo, repoPath, client := setupRepositoryService(t)

	ctx, cancel := testhelper.Context()
	defer cancel()

	_, err := client.RemoveRepository(ctx, &gitalypb.RemoveRepositoryRequest{Repository: repo})
	require.NoError(t, err)

	require.NoFileExists(t, repoPath)
}

func TestRemoveRepositoryDoesNotExist(t *testing.T) {
	t.Parallel()
	cfg, client := setupRepositoryServiceWithoutRepo(t)

	ctx, cancel := testhelper.Context()
	defer cancel()

	_, err := client.RemoveRepository(ctx, &gitalypb.RemoveRepositoryRequest{
		Repository: &gitalypb.Repository{StorageName: cfg.Storages[0].Name, RelativePath: "/does/not/exist"},
	})
	require.NoError(t, err)
}

func TestRemoveRepositoryTransactional(t *testing.T) {
	var votes []voting.Vote
	txManager := transaction.MockManager{
		VoteFn: func(ctx context.Context, tx txinfo.Transaction, vote voting.Vote) error {
			votes = append(votes, vote)
			return nil
		},
	}

	cfg, repo, repoPath, client := setupRepositoryService(t, testserver.WithTransactionManager(&txManager))

	ctx, cancel := testhelper.Context()
	defer cancel()
	ctx, err := txinfo.InjectTransaction(ctx, 1, "primary", true)
	require.NoError(t, err)
	ctx = metadata.IncomingToOutgoing(ctx)

	t.Run("with existing repository", func(t *testing.T) {
		votes = []voting.Vote{}

		_, err := client.RemoveRepository(ctx, &gitalypb.RemoveRepositoryRequest{Repository: repo})
		require.NoError(t, err)
		require.NoFileExists(t, repoPath)

		require.Equal(t, []voting.Vote{
			voting.VoteFromData([]byte(fmt.Sprintf("pre-remove %s", repo.GetRelativePath()))),
			voting.VoteFromData([]byte(fmt.Sprintf("post-remove %s", repo.GetRelativePath()))),
		}, votes)
	})

	t.Run("with nonexistent repository", func(t *testing.T) {
		votes = []voting.Vote{}

		_, err := client.RemoveRepository(ctx, &gitalypb.RemoveRepositoryRequest{
			Repository: &gitalypb.Repository{
				StorageName: cfg.Storages[0].Name, RelativePath: "/does/not/exist",
			},
		},
		)
		require.NoError(t, err)

		require.Equal(t, []voting.Vote{
			voting.VoteFromData([]byte("pre-remove /does/not/exist")),
			voting.VoteFromData([]byte("post-remove /does/not/exist")),
		}, votes)
	})
}
