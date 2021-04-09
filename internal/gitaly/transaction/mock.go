package transaction

import (
	"context"
	"errors"

	"gitlab.com/gitlab-org/gitaly/internal/praefect/metadata"
)

// MockManager is a mock Manager for use in tests.
type MockManager struct {
	VoteFn func(context.Context, metadata.Transaction, metadata.PraefectServer, Vote) error
	StopFn func(context.Context, metadata.Transaction, metadata.PraefectServer) error
}

// Vote calls the MockManager's Vote function, if set. Otherwise, it returns an error.
func (m *MockManager) Vote(ctx context.Context, tx metadata.Transaction, praefect metadata.PraefectServer, vote Vote) error {
	if m.VoteFn == nil {
		return errors.New("mock does not implement Vote function")
	}
	return m.VoteFn(ctx, tx, praefect, vote)
}

// Stop calls the MockManager's Stop function, if set. Otherwise, it returns an error.
func (m *MockManager) Stop(ctx context.Context, tx metadata.Transaction, praefect metadata.PraefectServer) error {
	if m.StopFn == nil {
		return errors.New("mock does not implement Stop function")
	}
	return m.StopFn(ctx, tx, praefect)
}
