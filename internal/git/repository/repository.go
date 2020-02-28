package repository

import (
	"errors"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/internal/command"
	"gitlab.com/gitlab-org/gitaly/internal/log"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

// GitRepo supplies an interface for executing `git.Command`s
type GitRepo interface {
	GetStorageName() string
	GetRelativePath() string
	GetGitObjectDirectory() string
	GetGitAlternateObjectDirectories() []string
}

func NewTransactions() *Transactions {
	return &Transactions{
		transactions: make(map[string]*Transaction),
		repositories: make(map[string]*sync.Mutex),
		log:          log.Default(),
	}
}

type Transactions struct {
	txMutex, repoMutex sync.RWMutex
	transactions       map[string]*Transaction
	repositories       map[string]*sync.Mutex
	log                *logrus.Entry
}

func (t *Transactions) NewTransaction(transactionID string, repo *gitalypb.Repository) {
	logrus.WithField("transaction_id", transactionID).Info("creating new transaction")
	tx := Transaction{
		Repo: repo,
	}

	t.txMutex.Lock()
	defer t.txMutex.Unlock()
	t.transactions[transactionID] = &tx

	t.repoMutex.Lock()

	_, ok := t.repositories[repo.RelativePath]
	if !ok {
		t.repositories[repo.RelativePath] = &sync.Mutex{}
	}

	t.repositories[repo.RelativePath].Lock()

	t.repoMutex.Unlock()
}

func (t *Transactions) Start(transactionID string) {
	t.txMutex.Lock()
	defer t.txMutex.Unlock()

	_, ok := t.transactions[transactionID]
	if !ok {
		return
	}

	t.transactions[transactionID].inProgress = true

	go func() {
		<-time.NewTimer(1 * time.Second).C
		t.txMutex.Lock()
		tx, ok := t.transactions[transactionID]
		if !ok {
			return
		}
		if tx.prepared {
			t.log.WithField("transaction_id", transactionID).Info("transaction has already been prepared")
			return
		}
		t.txMutex.Unlock()

		t.log.WithField("transaction_id", transactionID).Info("transaction has not been prepared and is timing out")
		t.Unlock(transactionID)
	}()
}

func (t *Transactions) PreCommit(transactionID string, c command.Cmd) {
	t.txMutex.Lock()
	defer t.txMutex.Unlock()

	tx, ok := t.transactions[transactionID]
	if !ok {
		return
	}

	go tx.PrepareCommitAndWait(c, 2*time.Second)
}

func (t *Transactions) SetRollback(transactionID string, rollback command.Cmd) {
	t.txMutex.Lock()
	defer t.txMutex.Unlock()
	tx, ok := t.transactions[transactionID]
	if !ok {
		return
	}

	tx.Rollback = rollback
}

func (t *Transactions) Commit(transactionID string) error {
	t.txMutex.Lock()
	defer t.txMutex.Unlock()

	tx, ok := t.transactions[transactionID]
	if !ok {
		return errors.New("request_id not found")
	}

	tx.Commit()

	t.log.WithField("transaction_id", transactionID).Info("commited")
	return nil
}

func (t *Transactions) Unlock(transactionID string) error {
	t.txMutex.Lock()
	defer t.txMutex.Unlock()

	tx, ok := t.transactions[transactionID]
	if !ok {
		return errors.New("request_id not found")
	}

	if !tx.inProgress {
		t.repoMutex.Lock()
		defer t.repoMutex.Unlock()

		repoLock, ok := t.repositories[tx.Repo.GetRelativePath()]
		if !ok {
			return nil
		}
		repoLock.Unlock()

		delete(t.transactions, transactionID)
		t.log.WithField("transaction_id", transactionID).Info("unlocked")
	}

	return nil
}

func (t *Transactions) Rollback(transactionID string) error {
	t.txMutex.Lock()
	defer t.txMutex.Unlock()

	tx, ok := t.transactions[transactionID]
	if !ok {
		return errors.New("request_id not found")
	}

	if err := tx.Rollback.Wait(); err != nil {
		return err
	}

	return nil
}

func (t *Transactions) TransactionStarted(transactionID string) bool {
	t.txMutex.Lock()
	defer t.txMutex.Unlock()

	_, ok := t.transactions[transactionID]

	return ok
}

func (t *Transaction) PrepareCommitAndWait(c command.Cmd, d time.Duration) {
	t.prepared = true

	select {
	case <-time.NewTimer(d).C:
	case <-t.commitCh:
	}

	t.commitErr = c.Wait()
	t.inProgress = false
}

func (t *Transaction) Commit() {
	close(t.commitCh)
}

type Transaction struct {
	Repo                 GitRepo
	Rollback             command.Cmd
	commitErr            error
	commitCh             chan struct{}
	inProgress, prepared bool
}
