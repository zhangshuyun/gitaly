package hook

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"gitlab.com/gitlab-org/gitaly/v13/internal/praefect/metadata"
	"gitlab.com/gitlab-org/gitaly/v13/proto/go/gitalypb"
	"google.golang.org/grpc"
)

func isPrimary(env []string) (bool, error) {
	tx, err := metadata.TransactionFromEnv(env)
	if err != nil {
		if errors.Is(err, metadata.ErrTransactionNotFound) {
			// If there is no transaction, then we only ever write
			// to the primary. Thus, we return true.
			return true, nil
		}
		return false, err
	}

	return tx.Primary, nil
}

func (m *GitLabHookManager) getPraefectConn(ctx context.Context, server *metadata.PraefectServer) (*grpc.ClientConn, error) {
	address, err := server.Address()
	if err != nil {
		return nil, err
	}
	return m.conns.Dial(ctx, address, server.Token)
}

func (m *GitLabHookManager) voteOnTransaction(ctx context.Context, hash []byte, env []string) error {
	tx, err := metadata.TransactionFromEnv(env)
	if err != nil {
		if errors.Is(err, metadata.ErrTransactionNotFound) {
			// No transaction being present is valid, e.g. in case
			// there is no Praefect server or the transactions
			// feature flag is not set.
			return nil
		}
		return fmt.Errorf("could not extract transaction: %w", err)
	}

	praefectServer, err := metadata.PraefectFromEnv(env)
	if err != nil {
		return fmt.Errorf("could not extract Praefect server: %w", err)
	}

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	praefectConn, err := m.getPraefectConn(ctx, praefectServer)
	if err != nil {
		return err
	}

	praefectClient := gitalypb.NewRefTransactionClient(praefectConn)

	defer prometheus.NewTimer(m.votingDelayMetric).ObserveDuration()
	response, err := praefectClient.VoteTransaction(ctx, &gitalypb.VoteTransactionRequest{
		TransactionId:        tx.ID,
		Node:                 tx.Node,
		ReferenceUpdatesHash: hash,
	})
	if err != nil {
		return err
	}

	if response.State != gitalypb.VoteTransactionResponse_COMMIT {
		return errors.New("transaction was aborted")
	}

	return nil
}
