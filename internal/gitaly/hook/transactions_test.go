package hook

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitlab"
	"gitlab.com/gitlab-org/gitaly/v14/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v14/internal/transaction/txinfo"
	"gitlab.com/gitlab-org/gitaly/v14/internal/transaction/voting"
)

func TestHookManager_stopCalled(t *testing.T) {
	cfg, repo, repoPath := testcfg.BuildWithRepo(t)

	expectedTx := txinfo.Transaction{
		ID: 1234, Node: "primary", Primary: true,
	}

	var mockTxMgr transaction.MockManager
	hookManager := NewManager(cfg, config.NewLocator(cfg), gittest.NewCommandFactory(t, cfg), &mockTxMgr, gitlab.NewMockClient(
		t, gitlab.MockAllowed, gitlab.MockPreReceive, gitlab.MockPostReceive,
	))

	ctx, cleanup := testhelper.Context()
	defer cleanup()

	hooksPayload, err := git.NewHooksPayload(
		cfg,
		repo,
		&expectedTx,
		&git.ReceiveHooksPayload{
			UserID:   "1234",
			Username: "user",
			Protocol: "web",
		},
		git.ReferenceTransactionHook,
		featureflag.RawFromContext(ctx),
	).Env()
	require.NoError(t, err)

	hookPaths := make([]string, 3)
	for i, hook := range []string{"pre-receive", "update", "post-receive"} {
		hookPaths[i] = gittest.WriteCustomHook(t, repoPath, hook, []byte("#!/bin/sh\nexit 1\n"))
	}

	preReceiveFunc := func(t *testing.T) error {
		return hookManager.PreReceiveHook(ctx, repo, nil, []string{hooksPayload}, strings.NewReader("changes"), io.Discard, io.Discard)
	}
	updateFunc := func(t *testing.T) error {
		return hookManager.UpdateHook(ctx, repo, "ref", git.ZeroOID.String(), git.ZeroOID.String(), []string{hooksPayload}, io.Discard, io.Discard)
	}
	postReceiveFunc := func(t *testing.T) error {
		return hookManager.PostReceiveHook(ctx, repo, nil, []string{hooksPayload}, strings.NewReader("changes"), io.Discard, io.Discard)
	}

	for _, tc := range []struct {
		desc     string
		hookFunc func(*testing.T) error
		hookPath string
		stopErr  error
	}{
		{
			desc:     "pre-receive gets successfully stopped",
			hookFunc: preReceiveFunc,
			hookPath: hookPaths[0],
		},
		{
			desc:     "pre-receive with stop error does not clobber real error",
			hookFunc: preReceiveFunc,
			stopErr:  errors.New("stop error"),
			hookPath: hookPaths[0],
		},
		{
			desc:     "post-receive gets successfully stopped",
			hookFunc: postReceiveFunc,
			hookPath: hookPaths[2],
		},
		{
			desc:     "post-receive with stop error does not clobber real error",
			hookFunc: postReceiveFunc,
			stopErr:  errors.New("stop error"),
			hookPath: hookPaths[2],
		},
		{
			desc:     "update gets successfully stopped",
			hookFunc: updateFunc,
			hookPath: hookPaths[1],
		},
		{
			desc:     "update with stop error does not clobber real error",
			hookFunc: updateFunc,
			stopErr:  errors.New("stop error"),
			hookPath: hookPaths[1],
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			wasInvoked := false
			mockTxMgr.StopFn = func(ctx context.Context, tx txinfo.Transaction) error {
				require.Equal(t, expectedTx, tx)
				wasInvoked = true
				return tc.stopErr
			}

			err := tc.hookFunc(t)
			require.Equal(t, fmt.Sprintf("executing custom hooks: error executing \"%s\": exit status 1", tc.hookPath), err.Error())
			require.True(t, wasInvoked, "expected stop to have been invoked")
		})
	}
}

func TestHookManager_contextCancellationCancelsVote(t *testing.T) {
	cfg, repo, _ := testcfg.BuildWithRepo(t)

	mockTxMgr := transaction.MockManager{
		VoteFn: func(ctx context.Context, _ txinfo.Transaction, _ voting.Vote, _ voting.Phase) error {
			<-ctx.Done()
			return fmt.Errorf("mock error: %s", ctx.Err())
		},
	}

	hookManager := NewManager(cfg, config.NewLocator(cfg), gittest.NewCommandFactory(t, cfg), &mockTxMgr, gitlab.NewMockClient(
		t, gitlab.MockAllowed, gitlab.MockPreReceive, gitlab.MockPostReceive,
	))

	hooksPayload, err := git.NewHooksPayload(
		cfg,
		repo,
		&txinfo.Transaction{
			ID: 1234, Node: "primary", Primary: true,
		},
		nil,
		git.ReferenceTransactionHook,
		nil,
	).Env()
	require.NoError(t, err)

	ctx, cancel := testhelper.Context()
	defer cancel()

	changes := fmt.Sprintf("%s %s refs/heads/master", strings.Repeat("1", 40), git.ZeroOID)

	cancel()

	err = hookManager.ReferenceTransactionHook(ctx, ReferenceTransactionPrepared, []string{hooksPayload}, strings.NewReader(changes))
	require.Equal(t, "error voting on transaction: mock error: context canceled", err.Error())
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
