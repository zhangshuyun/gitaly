package hook

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

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
	hookManager := NewManager(config.NewLocator(cfg), &mockTxMgr, gitlab.NewMockClient(
		t, gitlab.MockAllowed, gitlab.MockPreReceive, gitlab.MockPostReceive,
	), cfg)

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

	for _, hook := range []string{"pre-receive", "update", "post-receive"} {
		gittest.WriteCustomHook(t, repoPath, hook, []byte("#!/bin/sh\nexit 1\n"))
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
			mockTxMgr.StopFn = func(ctx context.Context, tx txinfo.Transaction) error {
				require.Equal(t, expectedTx, tx)
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
	cfg, repo, _ := testcfg.BuildWithRepo(t)

	mockTxMgr := transaction.MockManager{
		VoteFn: func(ctx context.Context, tx txinfo.Transaction, vote voting.Vote) error {
			<-ctx.Done()
			return fmt.Errorf("mock error: %s", ctx.Err())
		},
	}

	hookManager := NewManager(config.NewLocator(cfg), &mockTxMgr, gitlab.NewMockClient(
		t, gitlab.MockAllowed, gitlab.MockPreReceive, gitlab.MockPostReceive,
	), cfg)

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
