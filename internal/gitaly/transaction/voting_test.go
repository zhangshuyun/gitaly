package transaction

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/backchannel"
	"gitlab.com/gitlab-org/gitaly/v14/internal/safe"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/transaction/txinfo"
	"gitlab.com/gitlab-org/gitaly/v14/internal/transaction/voting"
	"google.golang.org/grpc/peer"
)

func TestRunOnContext(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	backchannelPeer := &peer.Peer{
		AuthInfo: backchannel.WithID(nil, 1234),
	}

	t.Run("without transaction", func(t *testing.T) {
		require.NoError(t, RunOnContext(ctx, func(tx txinfo.Transaction) error {
			t.Fatal("this function should not be executed")
			return nil
		}))
	})

	t.Run("with transaction and no error", func(t *testing.T) {
		ctx, err := txinfo.InjectTransaction(ctx, 5678, "node", true)
		require.NoError(t, err)
		ctx = peer.NewContext(ctx, backchannelPeer)

		callbackExecuted := false
		require.NoError(t, RunOnContext(ctx, func(tx txinfo.Transaction) error {
			require.Equal(t, txinfo.Transaction{
				ID:            5678,
				Node:          "node",
				Primary:       true,
				BackchannelID: 1234,
			}, tx)
			callbackExecuted = true
			return nil
		}))
		require.True(t, callbackExecuted, "callback should have been executed")
	})

	t.Run("with transaction and error", func(t *testing.T) {
		ctx, err := txinfo.InjectTransaction(ctx, 5678, "node", true)
		require.NoError(t, err)
		ctx = peer.NewContext(ctx, backchannelPeer)

		expectedErr := fmt.Errorf("any error")
		require.Equal(t, expectedErr, RunOnContext(ctx, func(txinfo.Transaction) error {
			return expectedErr
		}))
	})

	t.Run("with transaction but missing peer", func(t *testing.T) {
		ctx, err := txinfo.InjectTransaction(ctx, 5678, "node", true)
		require.NoError(t, err)
		require.EqualError(t, RunOnContext(ctx, nil), "get peer id: no peer info in context")
	})
}

func TestVoteOnContext(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	backchannelPeer := &peer.Peer{
		AuthInfo: backchannel.WithID(nil, 1234),
	}

	vote := voting.VoteFromData([]byte("1"))

	t.Run("without transaction", func(t *testing.T) {
		require.NoError(t, VoteOnContext(ctx, &MockManager{}, voting.Vote{}))
	})

	t.Run("successful vote", func(t *testing.T) {
		ctx, err := txinfo.InjectTransaction(ctx, 5678, "node", true)
		require.NoError(t, err)
		ctx = peer.NewContext(ctx, backchannelPeer)

		callbackExecuted := false
		require.NoError(t, VoteOnContext(ctx, &MockManager{
			VoteFn: func(ctx context.Context, tx txinfo.Transaction, vote voting.Vote) error {
				require.Equal(t, txinfo.Transaction{
					ID:            5678,
					Node:          "node",
					Primary:       true,
					BackchannelID: 1234,
				}, tx)
				callbackExecuted = true
				return nil
			},
		}, vote))
		require.True(t, callbackExecuted, "callback should have been executed")
	})

	t.Run("failing vote", func(t *testing.T) {
		ctx, err := txinfo.InjectTransaction(ctx, 5678, "node", true)
		require.NoError(t, err)
		ctx = peer.NewContext(ctx, backchannelPeer)

		expectedErr := fmt.Errorf("any error")
		require.Equal(t, expectedErr, VoteOnContext(ctx, &MockManager{
			VoteFn: func(ctx context.Context, tx txinfo.Transaction, vote voting.Vote) error {
				return expectedErr
			},
		}, vote))
	})
}

func TestCommitLockedFile(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	backchannelPeer := &peer.Peer{
		AuthInfo: backchannel.WithID(nil, 1234),
	}

	t.Run("without transaction", func(t *testing.T) {
		file := filepath.Join(testhelper.TempDir(t), "file")

		writer, err := safe.NewLockingFileWriter(file)
		require.NoError(t, err)
		_, err = writer.Write([]byte("contents"))
		require.NoError(t, err)

		require.NoError(t, CommitLockedFile(ctx, &MockManager{}, writer))
		require.Equal(t, []byte("contents"), testhelper.MustReadFile(t, file))
	})

	ctx, err := txinfo.InjectTransaction(ctx, 5678, "node", true)
	require.NoError(t, err)
	ctx = peer.NewContext(ctx, backchannelPeer)

	t.Run("successful transaction", func(t *testing.T) {
		file := filepath.Join(testhelper.TempDir(t), "file")

		writer, err := safe.NewLockingFileWriter(file)
		require.NoError(t, err)
		_, err = writer.Write([]byte("contents"))
		require.NoError(t, err)

		calls := 0
		require.NoError(t, CommitLockedFile(ctx, &MockManager{
			VoteFn: func(ctx context.Context, tx txinfo.Transaction, vote voting.Vote) error {
				require.Equal(t, txinfo.Transaction{
					ID:            5678,
					Node:          "node",
					Primary:       true,
					BackchannelID: 1234,
				}, tx)
				require.Equal(t, voting.VoteFromData([]byte("contents")), vote)
				calls++
				return nil
			},
		}, writer))
		require.Equal(t, 2, calls, "expected two votes")

		require.Equal(t, []byte("contents"), testhelper.MustReadFile(t, file))
	})

	t.Run("failing transaction", func(t *testing.T) {
		file := filepath.Join(testhelper.TempDir(t), "file")

		writer, err := safe.NewLockingFileWriter(file)
		require.NoError(t, err)
		_, err = writer.Write([]byte("contents"))
		require.NoError(t, err)

		err = CommitLockedFile(ctx, &MockManager{
			VoteFn: func(context.Context, txinfo.Transaction, voting.Vote) error {
				return fmt.Errorf("some error")
			},
		}, writer)
		require.EqualError(t, err, "voting on locked file: preimage vote: some error")

		require.NoFileExists(t, file)
	})

	t.Run("concurrent modification", func(t *testing.T) {
		file := filepath.Join(testhelper.TempDir(t), "file")

		writer, err := safe.NewLockingFileWriter(file)
		require.NoError(t, err)
		_, err = writer.Write([]byte("contents"))
		require.NoError(t, err)

		err = CommitLockedFile(ctx, &MockManager{
			VoteFn: func(context.Context, txinfo.Transaction, voting.Vote) error {
				// This shouldn't typically happen given that the file is locked,
				// but we concurrently update the file after our first vote.
				require.NoError(t, os.WriteFile(file, []byte("something"),
					0o666))
				return nil
			},
		}, writer)
		require.EqualError(t, err, "committing file: file concurrently created")

		require.Equal(t, []byte("something"), testhelper.MustReadFile(t, file))
	})
}
