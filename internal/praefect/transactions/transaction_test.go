package transactions

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/transaction/voting"
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

func TestTransaction_DidVote(t *testing.T) {
	ctx, cleanup := testhelper.Context()
	defer cleanup()

	tx, err := newTransaction(1, []Voter{
		{Name: "v1", Votes: 1},
		{Name: "v2", Votes: 0},
	}, 1)
	require.NoError(t, err)

	// An unregistered voter did not vote.
	require.False(t, tx.DidVote("unregistered"))
	// And neither of the registered ones did cast a vote yet.
	require.False(t, tx.DidVote("v1"))
	require.False(t, tx.DidVote("v2"))

	// One of both nodes does cast a vote.
	require.NoError(t, tx.vote(ctx, "v1", voting.VoteFromData([]byte{})))
	require.True(t, tx.DidVote("v1"))
	require.False(t, tx.DidVote("v2"))

	// And now the second node does cast a vote, too.
	require.NoError(t, tx.vote(ctx, "v2", voting.VoteFromData([]byte{})))
	require.True(t, tx.DidVote("v1"))
	require.True(t, tx.DidVote("v2"))
}
