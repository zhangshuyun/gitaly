package repository

import (
	"errors"
	"sync"

	"gitlab.com/gitlab-org/gitaly/internal/log"

	"github.com/sirupsen/logrus"

	"github.com/google/uuid"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"

	"gitlab.com/gitlab-org/gitaly/internal/command"
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

func (t *Transactions) StartTransaction(repo *gitalypb.Repository, rollback command.Cmd) string {
	tx := Transaction{
		Repo:     repo,
		Rollback: rollback,
	}

	transactionID := uuid.New().String()

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

	t.log.WithField("relative_path", repo.RelativePath)

	t.transactions[transactionID].Commit = t.repositories[repo.RelativePath].Unlock

	return transactionID
}

func (t *Transactions) Commit(transactionID string) error {
	t.txMutex.Lock()
	defer t.txMutex.Unlock()

	tx, ok := t.transactions[transactionID]
	if !ok {
		return errors.New("request_id not found")
	}

	t.repoMutex.Lock()
	tx.Commit()
	t.repoMutex.Unlock()

	delete(t.transactions, transactionID)
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

type Transaction struct {
	//	serviceName string
	//	methodName  string
	//	transactionID   string
	Repo     GitRepo
	Rollback command.Cmd
	Commit   func()
}

type RepoLock interface {
	Lock(relativePath string)
	Unlock(relativePath string)
}

type repoLock struct {
	m     sync.RWMutex
	locks map[string]sync.RWMutex
}

func (r *repoLock) Lock(relativePath string) {
	l, ok := r.locks[relativePath]
	if !ok {
		l = sync.RWMutex{}
		r.m.Lock()
		defer r.m.Unlock()
		r.locks[relativePath] = l
	}
	l.Lock()
}

func (r *repoLock) Unlock(relativePath string) {
	l, ok := r.locks[relativePath]
	if ok {
		l.Lock()
	}
}

func NewRepoLock() *repoLock {
	return &repoLock{
		locks: make(map[string]sync.RWMutex),
	}
}
