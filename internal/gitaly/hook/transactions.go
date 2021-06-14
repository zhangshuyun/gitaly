package hook

import (
	"context"

	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/transaction/txinfo"
	"gitlab.com/gitlab-org/gitaly/v14/internal/transaction/voting"
)

func isPrimary(payload git.HooksPayload) bool {
	if payload.Transaction == nil {
		return true
	}
	return payload.Transaction.Primary
}

// transactionHandler is a callback invoked on a transaction if it exists.
type transactionHandler func(ctx context.Context, tx txinfo.Transaction) error

// runWithTransaction runs the given function if the payload identifies a transaction. No error
// is returned if no transaction exists. If a transaction exists and the function is executed on it,
// then its error will ber returned directly.
func (m *GitLabHookManager) runWithTransaction(ctx context.Context, payload git.HooksPayload, handler transactionHandler) error {
	if payload.Transaction == nil {
		return nil
	}
	if err := handler(ctx, *payload.Transaction); err != nil {
		return err
	}

	return nil
}

func (m *GitLabHookManager) voteOnTransaction(ctx context.Context, vote voting.Vote, payload git.HooksPayload) error {
	return m.runWithTransaction(ctx, payload, func(ctx context.Context, tx txinfo.Transaction) error {
		return m.txManager.Vote(ctx, tx, vote)
	})
}

func (m *GitLabHookManager) stopTransaction(ctx context.Context, payload git.HooksPayload) error {
	return m.runWithTransaction(ctx, payload, func(ctx context.Context, tx txinfo.Transaction) error {
		return m.txManager.Stop(ctx, tx)
	})
}
