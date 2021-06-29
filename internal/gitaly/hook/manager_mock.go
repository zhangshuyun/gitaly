package hook

import (
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

// MockManager mocks the Manager interface for Git hooks (e.g. pre-receive, post-receive)
type MockManager struct {
	t                    *testing.T
	preReceive           func(t *testing.T, ctx context.Context, repo *gitalypb.Repository, pushOptions, env []string, stdin io.Reader, stdout, stderr io.Writer) error
	postReceive          func(t *testing.T, ctx context.Context, repo *gitalypb.Repository, pushOptions, env []string, stdin io.Reader, stdout, stderr io.Writer) error
	update               func(t *testing.T, ctx context.Context, repo *gitalypb.Repository, ref, oldValue, newValue string, env []string, stdout, stderr io.Writer) error
	referenceTransaction func(t *testing.T, ctx context.Context, state ReferenceTransactionState, env []string, stdin io.Reader) error
}

var (
	// NopPreReceive does nothing for the pre-receive hook
	NopPreReceive = func(t *testing.T, ctx context.Context, repo *gitalypb.Repository, pushOptions, env []string, stdin io.Reader, stdout, stderr io.Writer) error {
		return nil
	}

	// NopPostReceive does nothing for the post-receive hook
	NopPostReceive = func(t *testing.T, ctx context.Context, repo *gitalypb.Repository, pushOptions, env []string, stdin io.Reader, stdout, stderr io.Writer) error {
		return nil
	}

	// NopUpdate does nothing for the update hook
	NopUpdate = func(t *testing.T, ctx context.Context, repo *gitalypb.Repository, ref, oldValue, newValue string, env []string, stdout, stderr io.Writer) error {
		return nil
	}

	// NopReferenceTransaction does nothing for the reference transaction hook
	NopReferenceTransaction = func(t *testing.T, ctx context.Context, state ReferenceTransactionState, env []string, stdin io.Reader) error {
		return nil
	}
)

// NewMockManager returns a mocked hook Manager with the stubbed functions
func NewMockManager(
	t *testing.T,
	preReceive func(t *testing.T, ctx context.Context, repo *gitalypb.Repository, pushOptions, env []string, stdin io.Reader, stdout, stderr io.Writer) error,
	postReceive func(t *testing.T, ctx context.Context, repo *gitalypb.Repository, pushOptions, env []string, stdin io.Reader, stdout, stderr io.Writer) error,
	update func(t *testing.T, ctx context.Context, repo *gitalypb.Repository, ref, oldValue, newValue string, env []string, stdout, stderr io.Writer) error,
	referenceTransaction func(t *testing.T, ctx context.Context, state ReferenceTransactionState, env []string, stdin io.Reader) error,
) Manager {
	return &MockManager{
		t:                    t,
		preReceive:           preReceive,
		postReceive:          postReceive,
		update:               update,
		referenceTransaction: referenceTransaction,
	}
}

// PreReceiveHook executes the mocked pre-receive hook
func (m *MockManager) PreReceiveHook(ctx context.Context, repo *gitalypb.Repository, pushOptions, env []string, stdin io.Reader, stdout, stderr io.Writer) error {
	require.NotNil(m.t, m.preReceive, "preReceive not implemented")

	return m.preReceive(m.t, ctx, repo, pushOptions, env, stdin, stdout, stderr)
}

// PostReceiveHook executes the mocked post-receive hook
func (m *MockManager) PostReceiveHook(ctx context.Context, repo *gitalypb.Repository, pushOptions, env []string, stdin io.Reader, stdout, stderr io.Writer) error {
	require.NotNil(m.t, m.postReceive, "postReceive not implemented")

	return m.postReceive(m.t, ctx, repo, pushOptions, env, stdin, stdout, stderr)
}

// UpdateHook executes the mocked update hook
func (m *MockManager) UpdateHook(ctx context.Context, repo *gitalypb.Repository, ref, oldValue, newValue string, env []string, stdout, stderr io.Writer) error {
	require.NotNil(m.t, m.update, "update not implemented")

	return m.update(m.t, ctx, repo, ref, oldValue, newValue, env, stdout, stderr)
}

// ReferenceTransactionHook executes the mocked reference transaction hook
func (m *MockManager) ReferenceTransactionHook(ctx context.Context, state ReferenceTransactionState, env []string, stdin io.Reader) error {
	require.NotNil(m.t, m.referenceTransaction, "referenceTransaction not implemented")

	return m.referenceTransaction(m.t, ctx, state, env, stdin)
}
