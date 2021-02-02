package hook

import (
	"context"
	"errors"
	"time"

	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/metadata"
)

func isPrimary(payload git.HooksPayload) bool {
	if payload.Transaction == nil {
		return true
	}
	return payload.Transaction.Primary
}

// transactionHandler is a callback invoked on a transaction if it exists.
type transactionHandler func(ctx context.Context, tx metadata.Transaction, praefect metadata.PraefectServer) error

// runWithTransaction runs the given function if the payload identifies a transaction. No error
// is returned if no transaction exists. If a transaction exists and the function is executed on it,
// then its error will ber returned directly.
func (m *GitLabHookManager) runWithTransaction(ctx context.Context, payload git.HooksPayload, handler transactionHandler) error {
	if payload.Transaction == nil {
		return nil
	}
	if payload.Praefect == nil {
		return errors.New("transaction without Praefect server")
	}

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	if err := handler(ctx, *payload.Transaction, *payload.Praefect); err != nil {
		return err
	}

	return nil
}

func (m *GitLabHookManager) voteOnTransaction(ctx context.Context, hash []byte, payload git.HooksPayload) error {
	return m.runWithTransaction(ctx, payload, func(ctx context.Context, tx metadata.Transaction, praefect metadata.PraefectServer) error {
		return m.txManager.Vote(ctx, tx, praefect, hash)
	})
}

func (m *GitLabHookManager) stopTransaction(ctx context.Context, payload git.HooksPayload) error {
	return m.runWithTransaction(ctx, payload, func(ctx context.Context, tx metadata.Transaction, praefect metadata.PraefectServer) error {
		return m.txManager.Stop(ctx, tx, praefect)
	})
}
