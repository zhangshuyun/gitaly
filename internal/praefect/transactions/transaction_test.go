package transactions

import (
	"crypto/sha1"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v13/internal/testhelper"
)

func TestTransactionCancellationWithEmptyTransaction(t *testing.T) {
	ctx, cleanup := testhelper.Context()
	defer cleanup()

	tx, err := newTransaction(1, []Voter{
		{Name: "voter", Votes: 1},
	}, 1)
	require.NoError(t, err)

	hash := sha1.Sum([]byte{})

	tx.cancel()

	// When canceling a transaction, no more votes may happen.
	err = tx.vote(ctx, "voter", hash[:])
	require.Error(t, err)
	require.Equal(t, err, ErrTransactionCanceled)
}
