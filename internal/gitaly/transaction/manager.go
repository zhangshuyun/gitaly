package transaction

import (
	"context"
	"errors"

	"github.com/prometheus/client_golang/prometheus"
	"gitlab.com/gitlab-org/gitaly/client"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/metadata"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

// Manager is an interface for handling voting on transactions.
type Manager interface {
	// Vote casts a vote on the given transaction which is hosted by the
	// given Praefect server.
	Vote(context.Context, metadata.Transaction, metadata.PraefectServer, []byte) error

	// Stop gracefully stops the given transaction which is hosted by the
	// given Praefect server.
	Stop(context.Context, metadata.Transaction, metadata.PraefectServer) error
}

// PoolManager is an implementation of the Manager interface using a pool to
// connect to the transaction hosts.
type PoolManager struct {
	conns             *client.Pool
	votingDelayMetric prometheus.Histogram
}

// NewManager creates a new PoolManager to handle transactional voting.
func NewManager(cfg config.Cfg) *PoolManager {
	return &PoolManager{
		conns: client.NewPoolWithOptions(client.WithDialOptions(client.FailOnNonTempDialError()...)),
		votingDelayMetric: prometheus.NewHistogram(
			prometheus.HistogramOpts{
				Name:    "gitaly_hook_transaction_voting_delay_seconds",
				Help:    "Delay between calling out to transaction service and receiving a response",
				Buckets: cfg.Prometheus.GRPCLatencyBuckets,
			},
		),
	}
}

// Describe is used to describe Prometheus metrics.
func (m *PoolManager) Describe(descs chan<- *prometheus.Desc) {
	prometheus.DescribeByCollect(m, descs)
}

// Collect is used to collect Prometheus metrics.
func (m *PoolManager) Collect(metrics chan<- prometheus.Metric) {
	m.votingDelayMetric.Collect(metrics)
}

func (m *PoolManager) getTransactionClient(ctx context.Context, server metadata.PraefectServer) (gitalypb.RefTransactionClient, error) {
	address, err := server.Address()
	if err != nil {
		return nil, err
	}

	conn, err := m.conns.Dial(ctx, address, server.Token)
	if err != nil {
		return nil, err
	}

	return gitalypb.NewRefTransactionClient(conn), nil
}

// Vote connects to the given server and casts hash as a vote for the
// transaction identified by tx.
func (m *PoolManager) Vote(ctx context.Context, tx metadata.Transaction, server metadata.PraefectServer, hash []byte) error {
	client, err := m.getTransactionClient(ctx, server)
	if err != nil {
		return err
	}

	defer prometheus.NewTimer(m.votingDelayMetric).ObserveDuration()

	response, err := client.VoteTransaction(ctx, &gitalypb.VoteTransactionRequest{
		TransactionId:        tx.ID,
		Node:                 tx.Node,
		ReferenceUpdatesHash: hash,
	})
	if err != nil {
		return err
	}

	switch response.State {
	case gitalypb.VoteTransactionResponse_COMMIT:
		return nil
	case gitalypb.VoteTransactionResponse_ABORT:
		return errors.New("transaction was aborted")
	case gitalypb.VoteTransactionResponse_STOP:
		return errors.New("transaction was stopped")
	default:
		return errors.New("invalid transaction state")
	}
}

// Stop connects to the given server and stops the transaction identified by tx.
func (m *PoolManager) Stop(ctx context.Context, tx metadata.Transaction, server metadata.PraefectServer) error {
	client, err := m.getTransactionClient(ctx, server)
	if err != nil {
		return err
	}

	if _, err := client.StopTransaction(ctx, &gitalypb.StopTransactionRequest{
		TransactionId: tx.ID,
	}); err != nil {
		return err
	}

	return nil
}
