package repository

import "sync"

// GitRepo supplies an interface for executing `git.Command`s
type GitRepo interface {
	GetStorageName() string
	GetRelativePath() string
	GetGitObjectDirectory() string
	GetGitAlternateObjectDirectories() []string
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
