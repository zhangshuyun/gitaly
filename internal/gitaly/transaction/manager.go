package transaction

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/v14/client"
	"gitlab.com/gitlab-org/gitaly/v14/internal/backchannel"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/transaction/txinfo"
	"gitlab.com/gitlab-org/gitaly/v14/internal/transaction/voting"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

const (
	// transactionTimeout is the timeout used for all transactional
	// actions like voting and stopping of transactions. This timeout is
	// quite high: usually, a transaction should finish in at most a few
	// milliseconds. There are cases though where it may take a lot longer,
	// like when executing logic on the primary node only: the primary's
	// vote will be delayed until that logic finishes while secondaries are
	// waiting for the primary to cast its vote on the transaction. Given
	// that the primary-only logic's execution time scales with repository
	// size for the access checks and that it is potentially even unbounded
	// due to custom hooks, we thus use a high timeout. It shouldn't
	// normally be hit, but if it is hit then it indicates a real problem.
	transactionTimeout = 5 * time.Minute
)

var (
	// ErrTransactionAborted indicates a transaction was aborted, either
	// because it timed out or because the vote failed to reach quorum.
	ErrTransactionAborted = errors.New("transaction was aborted")
	// ErrTransactionStopped indicates a transaction was gracefully
	// stopped. This only happens in case the transaction was terminated
	// because of an external condition, e.g. access checks or hooks
	// rejected a change.
	ErrTransactionStopped = errors.New("transaction was stopped")
)

// Manager is an interface for handling voting on transactions.
type Manager interface {
	// Vote casts a vote on the given transaction which is hosted by the
	// given Praefect server.
	Vote(context.Context, txinfo.Transaction, voting.Vote) error

	// Stop gracefully stops the given transaction which is hosted by the
	// given Praefect server.
	Stop(context.Context, txinfo.Transaction) error
}

// PoolManager is an implementation of the Manager interface using a pool to
// connect to the transaction hosts.
type PoolManager struct {
	backchannels      *backchannel.Registry
	conns             *client.Pool
	votingDelayMetric prometheus.Histogram
}

// NewManager creates a new PoolManager to handle transactional voting.
func NewManager(cfg config.Cfg, backchannels *backchannel.Registry) *PoolManager {
	return &PoolManager{
		backchannels: backchannels,
		conns:        client.NewPoolWithOptions(client.WithDialOptions(client.FailOnNonTempDialError()...)),
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

func (m *PoolManager) getTransactionClient(ctx context.Context, tx txinfo.Transaction) (gitalypb.RefTransactionClient, error) {
	conn, err := m.backchannels.Backchannel(tx.BackchannelID)
	if err != nil {
		return nil, fmt.Errorf("get backchannel: %w", err)
	}

	return gitalypb.NewRefTransactionClient(conn), nil
}

// Vote connects to the given server and casts vote as a vote for the transaction identified by tx.
func (m *PoolManager) Vote(ctx context.Context, tx txinfo.Transaction, vote voting.Vote) error {
	client, err := m.getTransactionClient(ctx, tx)
	if err != nil {
		return err
	}

	logger := m.log(ctx).WithFields(logrus.Fields{
		"transaction.id":    tx.ID,
		"transaction.voter": tx.Node,
		"transaction.hash":  vote.String(),
	})

	defer prometheus.NewTimer(m.votingDelayMetric).ObserveDuration()

	transactionCtx, cancel := context.WithTimeout(ctx, transactionTimeout)
	defer cancel()

	response, err := client.VoteTransaction(transactionCtx, &gitalypb.VoteTransactionRequest{
		TransactionId:        tx.ID,
		Node:                 tx.Node,
		ReferenceUpdatesHash: vote.Bytes(),
	})
	if err != nil {
		// Add some additional context to cancellation errors so that
		// we know which of the contexts got canceled.
		if errors.Is(err, context.Canceled) && errors.Is(transactionCtx.Err(), context.Canceled) && ctx.Err() == nil {
			return fmt.Errorf("transaction timeout %s exceeded: %w", transactionTimeout, err)
		}

		logger.WithError(err).Error("vote failed")
		return err
	}

	switch response.State {
	case gitalypb.VoteTransactionResponse_COMMIT:
		return nil
	case gitalypb.VoteTransactionResponse_ABORT:
		logger.Error("transaction was aborted")
		return ErrTransactionAborted
	case gitalypb.VoteTransactionResponse_STOP:
		logger.Error("transaction was stopped")
		return ErrTransactionStopped
	default:
		return errors.New("invalid transaction state")
	}
}

// Stop connects to the given server and stops the transaction identified by tx.
func (m *PoolManager) Stop(ctx context.Context, tx txinfo.Transaction) error {
	client, err := m.getTransactionClient(ctx, tx)
	if err != nil {
		return err
	}

	if _, err := client.StopTransaction(ctx, &gitalypb.StopTransactionRequest{
		TransactionId: tx.ID,
	}); err != nil {
		m.log(ctx).WithFields(logrus.Fields{
			"transaction.id":    tx.ID,
			"transaction.voter": tx.Node,
		}).Error("stopping transaction failed")

		return err
	}

	return nil
}

func (m *PoolManager) log(ctx context.Context) logrus.FieldLogger {
	return ctxlogrus.Extract(ctx).WithField("component", "transaction.PoolManager")
}

// RunOnContext runs the given function if the context identifies a transaction.
func RunOnContext(ctx context.Context, fn func(txinfo.Transaction) error) error {
	transaction, err := txinfo.TransactionFromContext(ctx)
	if err != nil {
		if errors.Is(err, txinfo.ErrTransactionNotFound) {
			return nil
		}
		return err
	}
	return fn(transaction)
}

// VoteOnContext casts the vote on a transaction identified by the context, if there is any.
func VoteOnContext(ctx context.Context, m Manager, vote voting.Vote) error {
	return RunOnContext(ctx, func(transaction txinfo.Transaction) error {
		return m.Vote(ctx, transaction, vote)
	})
}
