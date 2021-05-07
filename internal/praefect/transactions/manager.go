package transactions

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/internal/transaction/voting"
)

var ErrNotFound = errors.New("transaction not found")

// Manager handles reference transactions for Praefect. It is required in order
// for Praefect to handle transactions directly instead of having to reach out
// to reference transaction RPCs.
type Manager struct {
	idSequence            uint64
	lock                  sync.Mutex
	transactions          map[uint64]*transaction
	counterMetric         *prometheus.CounterVec
	delayMetric           *prometheus.HistogramVec
	subtransactionsMetric prometheus.Histogram
}

// NewManager creates a new transactions Manager.
func NewManager(cfg config.Config) *Manager {
	return &Manager{
		transactions: make(map[uint64]*transaction),
		counterMetric: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "gitaly",
				Subsystem: "praefect",
				Name:      "transactions_total",
				Help:      "Total number of transaction actions",
			},
			[]string{"action"},
		),
		delayMetric: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: "gitaly",
				Subsystem: "praefect",
				Name:      "transactions_delay_seconds",
				Help:      "Delay between casting a vote and reaching quorum",
				Buckets:   cfg.Prometheus.GRPCLatencyBuckets,
			},
			[]string{"action"},
		),
		subtransactionsMetric: prometheus.NewHistogram(
			prometheus.HistogramOpts{
				Name:    "gitaly_praefect_subtransactions_per_transaction_total",
				Help:    "The number of subtransactions created for a single registered transaction",
				Buckets: []float64{0.0, 1.0, 2.0, 4.0, 8.0, 16.0, 32.0},
			},
		),
	}
}

func (mgr *Manager) Describe(descs chan<- *prometheus.Desc) {
	prometheus.DescribeByCollect(mgr, descs)
}

func (mgr *Manager) Collect(metrics chan<- prometheus.Metric) {
	mgr.counterMetric.Collect(metrics)
	mgr.delayMetric.Collect(metrics)
	mgr.subtransactionsMetric.Collect(metrics)
}

func (mgr *Manager) log(ctx context.Context) logrus.FieldLogger {
	return ctxlogrus.Extract(ctx).WithField("component", "transactions.Manager")
}

// CancelFunc is the transaction cancellation function returned by
// `RegisterTransaction`. Calling it will cause the transaction to be removed
// from the transaction manager.
type CancelFunc func() error

// RegisterTransaction registers a new reference transaction for a set of nodes
// taking part in the transaction. `threshold` is the threshold at which an
// election will succeed. It needs to be in the range `weight(voters)/2 <
// threshold <= weight(voters) to avoid indecidable votes.
func (mgr *Manager) RegisterTransaction(ctx context.Context, voters []Voter, threshold uint) (Transaction, CancelFunc, error) {
	mgr.lock.Lock()
	defer mgr.lock.Unlock()

	transactionID := atomic.AddUint64(&mgr.idSequence, 1)

	transaction, err := newTransaction(transactionID, voters, threshold)
	if err != nil {
		return nil, nil, err
	}

	if _, ok := mgr.transactions[transactionID]; ok {
		return nil, nil, errors.New("transaction exists already")
	}
	mgr.transactions[transactionID] = transaction

	mgr.log(ctx).WithFields(logrus.Fields{
		"transaction.id":     transactionID,
		"transaction.voters": voters,
	}).Debug("RegisterTransaction")

	mgr.counterMetric.WithLabelValues("registered").Add(float64(len(voters)))

	return transaction, func() error {
		return mgr.cancelTransaction(ctx, transaction)
	}, nil
}

func (mgr *Manager) cancelTransaction(ctx context.Context, transaction *transaction) error {
	mgr.lock.Lock()
	defer mgr.lock.Unlock()

	delete(mgr.transactions, transaction.ID())

	transaction.cancel()
	mgr.subtransactionsMetric.Observe(float64(transaction.CountSubtransactions()))

	var committed uint64
	state, err := transaction.State()
	if err != nil {
		return err
	}

	for _, result := range state {
		if result == VoteCommitted {
			committed++
		}
	}

	mgr.log(ctx).WithFields(logrus.Fields{
		"transaction.id":              transaction.ID(),
		"transaction.committed":       fmt.Sprintf("%d/%d", committed, len(state)),
		"transaction.subtransactions": transaction.CountSubtransactions(),
	}).Info("transaction completed")

	return nil
}

func (mgr *Manager) voteTransaction(ctx context.Context, transactionID uint64, node string, vote voting.Vote) error {
	mgr.lock.Lock()
	transaction, ok := mgr.transactions[transactionID]
	mgr.lock.Unlock()

	if !ok {
		return fmt.Errorf("%w: %d", ErrNotFound, transactionID)
	}

	if err := transaction.vote(ctx, node, vote); err != nil {
		return err
	}

	return nil
}

// VoteTransaction is called by a client who's casting a vote on a reference
// transaction. It waits until quorum was reached on the given transaction.
func (mgr *Manager) VoteTransaction(ctx context.Context, transactionID uint64, node string, vote voting.Vote) error {
	start := time.Now()
	defer func() {
		delay := time.Since(start)
		mgr.delayMetric.WithLabelValues("vote").Observe(delay.Seconds())
	}()

	logger := mgr.log(ctx).WithFields(logrus.Fields{
		"transaction.id":    transactionID,
		"transaction.voter": node,
		"transaction.hash":  vote.String(),
	})

	mgr.counterMetric.WithLabelValues("started").Inc()
	logger.Debug("VoteTransaction")

	if err := mgr.voteTransaction(ctx, transactionID, node, vote); err != nil {
		var counterLabel string

		if errors.Is(err, ErrTransactionStopped) {
			counterLabel = "stopped"
			// Stopped transactions indicate a graceful
			// termination, so we should not log an error here.
		} else if errors.Is(err, ErrTransactionFailed) {
			counterLabel = "failed"
			logger.WithError(err).Error("VoteTransaction: did not reach quorum")
		} else if errors.Is(err, ErrTransactionCanceled) {
			counterLabel = "canceled"
			logger.WithError(err).Error("VoteTransaction: transaction was canceled")
		} else {
			counterLabel = "invalid"
			logger.WithError(err).Error("VoteTransaction: failure")
		}

		mgr.counterMetric.WithLabelValues(counterLabel).Inc()

		return err
	}

	logger.Info("VoteTransaction: transaction committed")
	mgr.counterMetric.WithLabelValues("committed").Inc()

	return nil
}

// StopTransaction will gracefully stop a transaction.
func (mgr *Manager) StopTransaction(ctx context.Context, transactionID uint64) error {
	mgr.lock.Lock()
	transaction, ok := mgr.transactions[transactionID]
	mgr.lock.Unlock()

	if !ok {
		return fmt.Errorf("%w: %d", ErrNotFound, transactionID)
	}

	if err := transaction.stop(); err != nil {
		return err
	}

	mgr.log(ctx).WithFields(logrus.Fields{
		"transaction.id": transactionID,
	}).Debug("VoteTransaction: transaction stopped")
	mgr.counterMetric.WithLabelValues("stopped").Inc()

	return nil
}
