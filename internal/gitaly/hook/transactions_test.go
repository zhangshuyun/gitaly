package hook

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/metadata"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
)

type mockTransactionManager struct {
	vote func(context.Context, metadata.Transaction, metadata.PraefectServer, []byte) error
	stop func(context.Context, metadata.Transaction, metadata.PraefectServer) error
}

func (m *mockTransactionManager) Vote(ctx context.Context, tx metadata.Transaction, praefect metadata.PraefectServer, vote []byte) error {
	return m.vote(ctx, tx, praefect, vote)
}

func (m *mockTransactionManager) Stop(ctx context.Context, tx metadata.Transaction, praefect metadata.PraefectServer) error {
	return m.stop(ctx, tx, praefect)
}

func TestHookManager_stopCalled(t *testing.T) {
	expectedTx := metadata.Transaction{
		ID: 1234, Node: "primary", Primary: true,
	}

	expectedPraefect := metadata.PraefectServer{
		SocketPath: "socket",
		Token:      "foo",
	}

	repo, repoPath, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()

	var mockTxMgr mockTransactionManager
	hookManager := NewManager(config.NewLocator(config.Config), &mockTxMgr, GitlabAPIStub, config.Config)

	hooksPayload, err := git.NewHooksPayload(
		config.Config,
		repo,
		&expectedTx,
		&expectedPraefect,
		&git.ReceiveHooksPayload{
			UserID:   "1234",
			Username: "user",
			Protocol: "web",
		},
		git.ReferenceTransactionHook,
	).Env()
	require.NoError(t, err)

	ctx, cleanup := testhelper.Context()
	defer cleanup()

	for _, hook := range []string{"pre-receive", "update", "post-receive"} {
		cleanup = testhelper.WriteCustomHook(t, repoPath, hook, []byte("#!/bin/sh\nexit 1\n"))
		defer cleanup()
	}

	preReceiveFunc := func(t *testing.T) error {
		return hookManager.PreReceiveHook(ctx, repo, nil, []string{hooksPayload}, strings.NewReader("changes"), ioutil.Discard, ioutil.Discard)
	}
	updateFunc := func(t *testing.T) error {
		return hookManager.UpdateHook(ctx, repo, "ref", git.ZeroOID.String(), git.ZeroOID.String(), []string{hooksPayload}, ioutil.Discard, ioutil.Discard)
	}
	postReceiveFunc := func(t *testing.T) error {
		return hookManager.PostReceiveHook(ctx, repo, nil, []string{hooksPayload}, strings.NewReader("changes"), ioutil.Discard, ioutil.Discard)
	}

	for _, tc := range []struct {
		desc     string
		hookFunc func(*testing.T) error
		stopErr  error
	}{
		{
			desc:     "pre-receive gets successfully stopped",
			hookFunc: preReceiveFunc,
		},
		{
			desc:     "pre-receive with stop error does not clobber real error",
			hookFunc: preReceiveFunc,
			stopErr:  errors.New("stop error"),
		},
		{
			desc:     "post-receive gets successfully stopped",
			hookFunc: postReceiveFunc,
		},
		{
			desc:     "post-receive with stop error does not clobber real error",
			hookFunc: postReceiveFunc,
			stopErr:  errors.New("stop error"),
		},
		{
			desc:     "update gets successfully stopped",
			hookFunc: updateFunc,
		},
		{
			desc:     "update with stop error does not clobber real error",
			hookFunc: updateFunc,
			stopErr:  errors.New("stop error"),
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			wasInvoked := false
			mockTxMgr.stop = func(ctx context.Context, tx metadata.Transaction, praefect metadata.PraefectServer) error {
				require.Equal(t, expectedTx, tx)
				require.Equal(t, expectedPraefect, praefect)
				wasInvoked = true
				return tc.stopErr
			}

			err := tc.hookFunc(t)
			require.Equal(t, "executing custom hooks: exit status 1", err.Error())
			require.True(t, wasInvoked, "expected stop to have been invoked")
		})
	}
}

func TestHookManager_contextCancellationCancelsVote(t *testing.T) {
	mockTxMgr := mockTransactionManager{
		vote: func(ctx context.Context, tx metadata.Transaction, praefect metadata.PraefectServer, vote []byte) error {
			<-ctx.Done()
			return fmt.Errorf("mock error: %s", ctx.Err())
		},
	}

	hookManager := NewManager(config.NewLocator(config.Config), &mockTxMgr, GitlabAPIStub, config.Config)

	repo, _, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()

	hooksPayload, err := git.NewHooksPayload(
		config.Config,
		repo,
		&metadata.Transaction{
			ID: 1234, Node: "primary", Primary: true,
		},
		&metadata.PraefectServer{
			SocketPath: "does_not",
			Token:      "matter",
		},
		nil,
		git.ReferenceTransactionHook,
	).Env()
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
	defer cancel()

	changes := fmt.Sprintf("%s %s refs/heads/master", strings.Repeat("1", 40), git.ZeroOID)

	err = hookManager.ReferenceTransactionHook(ctx, ReferenceTransactionPrepared, []string{hooksPayload}, strings.NewReader(changes))
	require.Equal(t, "error voting on transaction: mock error: context deadline exceeded", err.Error())
}

func TestIsForceDeletionsOnly(t *testing.T) {
	anyOID := strings.Repeat("1", 40)
	zeroOID := git.ZeroOID.String()

	forceDeletion := fmt.Sprintf("%s %s refs/heads/force-delete", zeroOID, zeroOID)
	forceUpdate := fmt.Sprintf("%s %s refs/heads/force-update", zeroOID, anyOID)
	deletion := fmt.Sprintf("%s %s refs/heads/delete", anyOID, zeroOID)

	for _, tc := range []struct {
		desc     string
		changes  string
		expected bool
	}{
		{
			desc:     "single force deletion",
			changes:  forceDeletion + "\n",
			expected: true,
		},
		{
			desc:     "single force deletion with missing newline",
			changes:  forceDeletion,
			expected: true,
		},
		{
			desc:     "multiple force deletions",
			changes:  strings.Join([]string{forceDeletion, forceDeletion}, "\n"),
			expected: true,
		},
		{
			desc:     "single non-force deletion",
			changes:  deletion + "\n",
			expected: false,
		},
		{
			desc:     "single force update",
			changes:  forceUpdate + "\n",
			expected: false,
		},
		{
			desc:     "mixed deletions and updates",
			changes:  strings.Join([]string{forceDeletion, forceUpdate}, "\n"),
			expected: false,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			actual := isForceDeletionsOnly(strings.NewReader(tc.changes))
			require.Equal(t, tc.expected, actual)
		})
	}
}
