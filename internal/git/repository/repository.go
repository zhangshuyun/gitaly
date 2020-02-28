package repository

import (
	"errors"
	"os/exec"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
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

func NewTransactionManager() *TransactionManager {
	return &TransactionManager{
		transactions: make(map[string]*Transaction),
		repositories: make(map[string]*sync.Mutex),
		log:          log.Default(),
	}
}

type TransactionManager struct {
	txMutex, repoMutex sync.RWMutex
	transactions       map[string]*Transaction
	repositories       map[string]*sync.Mutex
	log                *logrus.Entry
}

func (t *TransactionManager) NewTransaction(transactionID string, repo *gitalypb.Repository) {
	logrus.WithField("transaction_id", transactionID).Info("creating new transaction")
	tx := Transaction{
		repo:     repo,
		commitCh: make(chan error),
	}

	t.txMutex.Lock()
	defer t.txMutex.Unlock()
	t.transactions[transactionID] = &tx

	t.repoMutex.Lock()
	_, ok := t.repositories[tx.repo.GetRelativePath()]
	if !ok {
		t.repositories[tx.repo.GetRelativePath()] = &sync.Mutex{}
	}
	t.repositories[tx.repo.GetRelativePath()].Lock()
	t.repoMutex.Unlock()
}

func (t *TransactionManager) Begin(transactionID string) {
	t.txMutex.Lock()
	defer t.txMutex.Unlock()

	tx, ok := t.transactions[transactionID]
	if !ok {
		return
	}

	tx.inProgress = true

	go func() {
		<-time.NewTimer(2 * time.Second).C
		t.txMutex.Lock()

		tx, ok := t.transactions[transactionID]
		if !ok {
			t.txMutex.Unlock()
			return
		}
		if tx.prepared {
			t.log.WithField("transaction_id", transactionID).Info("transaction has already been prepared")
			t.txMutex.Unlock()
			return
		}

		t.txMutex.Unlock()
		t.log.WithField("transaction_id", transactionID).Info("transaction has not been prepared and is timing out")
		t.Release(transactionID)
	}()
}

func (t *TransactionManager) PreCommit(transactionID string, c *exec.Cmd) {
	t.txMutex.Lock()
	defer t.txMutex.Unlock()

	tx, ok := t.transactions[transactionID]
	if !ok {
		return
	}

	go func() {
		tx.PrepareCommitAndWait(c, 2*time.Second)
		t.Release(transactionID)
	}()
}

func (t *TransactionManager) SetRollback(transactionID string, rollback *exec.Cmd) {
	t.txMutex.Lock()
	defer t.txMutex.Unlock()
	tx, ok := t.transactions[transactionID]
	if !ok {
		return
	}

	tx.rollback = rollback
}

func (t *TransactionManager) Commit(transactionID string) error {
	t.txMutex.Lock()
	defer t.txMutex.Unlock()

	tx, ok := t.transactions[transactionID]
	if !ok {
		return errors.New("request_id not found")
	}

	t.log.WithField("transaction_id", transactionID).Info("commited")
	return tx.Commit()
}

func (t *TransactionManager) Release(transactionID string) error {
	t.log.WithField("transaction_id", transactionID).Info("unlocking")
	t.txMutex.Lock()
	defer t.txMutex.Unlock()

	tx, ok := t.transactions[transactionID]
	if !ok {
		return errors.New("request_id not found")
	}

	if !tx.inProgress {
		t.repoMutex.Lock()
		defer t.repoMutex.Unlock()

		repoLock, ok := t.repositories[tx.repo.GetRelativePath()]
		if !ok {
			return nil
		}
		repoLock.Unlock()

		delete(t.transactions, transactionID)
		t.log.WithField("transaction_id", transactionID).Info("unlocked")
	}

	return nil
}

func (t *TransactionManager) Rollback(transactionID string) error {
	t.txMutex.Lock()
	defer t.txMutex.Unlock()

	tx, ok := t.transactions[transactionID]
	if !ok {
		return errors.New("request_id not found")
	}

	if err := tx.rollback.Run(); err != nil {
		return err
	}

	return nil
}

func (t *TransactionManager) TransactionStarted(transactionID string) bool {
	t.txMutex.Lock()
	defer t.txMutex.Unlock()

	_, ok := t.transactions[transactionID]

	return ok
}

func (t *Transaction) PrepareCommitAndWait(c *exec.Cmd, d time.Duration) {
	logrus.WithError(t.commitErr).Info("precommit started")
	t.prepared = true
	t.commit = c

	<-time.NewTimer(d).C
	if !t.inProgress {
		return
	}
	logrus.Info("timer went off...i'm doin it!")
	t.Commit()

	logrus.WithError(t.commitErr).Info("precommit completed")
}

func (t *Transaction) Commit() error {
	if t.commitErr != nil {
		return t.commitErr
	}
	if !t.inProgress {
		return nil
	}

	defer func() {
		t.inProgress = false
	}()

	if t.commitErr = t.commit.Start(); t.commitErr != nil {
		return t.commitErr
	}

	if t.commitErr = t.commit.Wait(); t.commitErr != nil {
		return t.commitErr
	}

	logrus.WithError(t.commitErr).WithField("args", t.commit.Args).Info("Finished Commit")

	return nil
}

type Transaction struct {
	repo                 GitRepo
	commit               *exec.Cmd
	rollback             *exec.Cmd
	commitErr            error
	commitCh             chan error
	inProgress, prepared bool
}
