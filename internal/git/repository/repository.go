package repository

import (
	"errors"
	"sync"

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

func (t *Transactions) Start(transactionID string, rollback command.Cmd) {
	t.txMutex.Lock()
	defer t.txMutex.Unlock()

	_, ok := t.transactions[transactionID]
	if !ok {
		return
	}

	t.transactions[transactionID].inProgress = true
	t.transactions[transactionID].Rollback = rollback
}

func (t *Transactions) Commit(transactionID string) error {
	t.txMutex.Lock()
	defer t.txMutex.Unlock()

	_, ok := t.transactions[transactionID]
	if !ok {
		return errors.New("request_id not found")
	}

	t.transactions[transactionID].inProgress = false

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

type Transaction struct {
	Repo       GitRepo
	Rollback   command.Cmd
	inProgress bool
}
