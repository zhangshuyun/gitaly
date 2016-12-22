package caching

import (
	"os"
	"path"
	"syscall"
)

type RefsCache struct {
	repositoryPath string
	cacheDir string
	lockFd int
}

/* Ensure the folder for the log and cache files exists. Return the path to that
	 folder */
func EnsureCacheDir(repositoryPath string) string {
	dirPath := path.Join(repositoryPath, "gitlab-cache", "info-refs")

	if _, err := os.Stat(dirPath); os.IsNotExist(err) {
		if err := os.MkdirAll(dirPath, 0750); err != nil {
			panic(err)
		}
	}

	return dirPath
}

func NewRefsCache(repositoryPath string) *RefsCache {
	cacheDir := EnsureCacheDir(repositoryPath)

	// Open lock file
	lockFd, err := syscall.Open(path.Join(cacheDir, "lock"), syscall.O_CREAT | syscall.O_RDONLY, 0750)
	if err != nil {
		panic(err)
	}

	return &RefsCache{
		repositoryPath: repositoryPath,
		cacheDir: cacheDir,
		lockFd: lockFd,
	}
}

func (r *RefsCache) Lock() {
	if err := syscall.Flock(r.lockFd, syscall.LOCK_EX); err != nil {
		panic(err)
	}
}

func (r *RefsCache) Unlock() {
	if err := syscall.Flock(r.lockFd, syscall.LOCK_UN); err != nil {
		panic(err)
	}
}
