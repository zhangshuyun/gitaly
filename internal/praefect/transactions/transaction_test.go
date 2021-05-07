package transactions

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/internal/transaction/voting"
)

func TestTransactionCancellationWithEmptyTransaction(t *testing.T) {
	ctx, cleanup := testhelper.Context()
	defer cleanup()

	tx, err := newTransaction(1, []Voter{
		{Name: "voter", Votes: 1},
	}, 1)
	require.NoError(t, err)

	tx.cancel()

	// When canceling a transaction, no more votes may happen.
	err = tx.vote(ctx, "voter", voting.VoteFromData([]byte{}))
	require.Error(t, err)
	require.Equal(t, err, ErrTransactionCanceled)
}
