package transaction

import (
	"context"
	"errors"

	"gitlab.com/gitlab-org/gitaly/v14/internal/transaction/txinfo"
	"gitlab.com/gitlab-org/gitaly/v14/internal/transaction/voting"
)

// RunOnContext runs the given function if the context identifies a transaction.
func RunOnContext(ctx context.Context, fn func(txinfo.Transaction) error) error {
	transaction, err := txinfo.TransactionFromContext(ctx)
	if err != nil {
		if errors.Is(err, txinfo.ErrTransactionNotFound) {
			return nil
		}
		return err
	}
	return fn(transaction)
}

// VoteOnContext casts the vote on a transaction identified by the context, if there is any.
func VoteOnContext(ctx context.Context, m Manager, vote voting.Vote) error {
	return RunOnContext(ctx, func(transaction txinfo.Transaction) error {
		return m.Vote(ctx, transaction, vote)
	})
}
