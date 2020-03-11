package git

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
)

func TestTransaction(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	repo, testRepoPath, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()

	refName, err := text.RandomHex(10)
	require.NoError(t, err)

	require.NoError(t, err)

	transaction := NewTransaction(repo, []string{"git", "-C", testRepoPath, "update-ref", fmt.Sprintf("refs/heads/%s", refName), "master"})
	require.NoError(t, transaction.Begin(ctx))

	transactionID, err := GetCurrentTransactionID(ctx, repo)
	require.NoError(t, err)

	tx, err := GetCurrentTransaction(repo, transactionID)
	require.NoError(t, err)
	require.Equal(t, transaction, tx)

	// try to create a second transaction
	refName, err = text.RandomHex(10)
	require.NoError(t, err)

	anotherTransaction := NewTransaction(repo, []string{"git", "-C", testRepoPath, "update-ref", fmt.Sprintf("refs/heads/%s", refName), "master"})
	require.Error(t, anotherTransaction.Begin(ctx))

	// try to commit with an incorrect transaction id
	require.Error(t, tx.Commit(ctx, "67d4350058a6f76a8a3d4133aaaaf96bf8a5698b"))

	// commit the transaction
	require.NoError(t, tx.Commit(ctx, transactionID))

	// now another transaction can begin
	require.NoError(t, anotherTransaction.Begin(ctx))
}
