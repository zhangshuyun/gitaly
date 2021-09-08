package transaction

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"

	"gitlab.com/gitlab-org/gitaly/v14/internal/safe"
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

// CommitLockedFile will lock, vote and commit the LockingFileWriter in a race-free manner.
func CommitLockedFile(ctx context.Context, m Manager, writer *safe.LockingFileWriter) (returnedErr error) {
	if err := writer.Lock(); err != nil {
		return fmt.Errorf("locking file: %w", err)
	}

	var vote voting.Vote
	if err := RunOnContext(ctx, func(tx txinfo.Transaction) error {
		hasher := voting.NewVoteHash()

		lockedFile, err := os.Open(writer.Path())
		if err != nil {
			return fmt.Errorf("opening locked file: %w", err)
		}
		defer lockedFile.Close()

		if _, err := io.Copy(hasher, lockedFile); err != nil {
			return fmt.Errorf("hashing locked file: %w", err)
		}

		vote, err = hasher.Vote()
		if err != nil {
			return fmt.Errorf("computing vote for locked file: %w", err)
		}

		if err := m.Vote(ctx, tx, vote); err != nil {
			return fmt.Errorf("preimage vote: %w", err)
		}

		return nil
	}); err != nil {
		return fmt.Errorf("voting on locked file: %w", err)
	}

	if err := writer.Commit(); err != nil {
		return fmt.Errorf("committing file: %w", err)
	}

	if err := VoteOnContext(ctx, m, vote); err != nil {
		return fmt.Errorf("postimage vote: %w", err)
	}

	return nil
}
