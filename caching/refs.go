package caching

import (
	"os"
	"path"
	"syscall"

	"github.com/satori/go.uuid"
)

type RefsCache struct {
	repositoryPath string
	cacheDir string
	uploadCachePath string
	receiveCachePath string
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
	lockFd, err := syscall.Open(path.Join(cacheDir, "lock"), syscall.O_CREAT | syscall.O_RDWR, 0750)
	if err != nil {
		panic(err)
	}

	return &RefsCache{
		repositoryPath: repositoryPath,
		cacheDir: cacheDir,
		lockFd: lockFd,
		uploadCachePath: path.Join(cacheDir, "upload-pack"),
		receiveCachePath: path.Join(cacheDir, "receive-pack"),
	}
}

func (r *RefsCache) CloseLock() {
	 if err := syscall.Close(r.lockFd); err != nil {
		 panic(err)
	 }
}

func (r *RefsCache) Lock() {
	if err := syscall.Flock(r.lockFd, syscall.LOCK_EX); err != nil {
		panic(err)
	}
}

func (r *RefsCache) Unlock() {
	defer r.CloseLock()

	if err := syscall.Flock(r.lockFd, syscall.LOCK_UN); err != nil {
		panic(err)
	}
}

func (r *RefsCache) InvalidateCache() {
	r.Lock()
	defer r.Unlock()

	newUuid := uuid.NewV4()

	// Truncate and write a new UUID in the lock file
	syscall.Ftruncate(r.lockFd, 0)
	syscall.Write(r.lockFd, []byte(newUuid.String()))

	// Remove cache files
	os.Remove(r.uploadCachePath)
	os.Remove(r.receiveCachePath)
}
