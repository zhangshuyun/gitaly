package caching

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"syscall"

	"github.com/satori/go.uuid"
)

type RefsCache struct {
	repositoryPath string
	cacheDir string
	cachePaths map[string]string
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
		cachePaths: map[string]string {
			"upload-pack": path.Join(cacheDir, "upload-pack"),
			"receive-pack": path.Join(cacheDir, "receive-pack"),
		},
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

func (r *RefsCache) GetLockUuid() string {
	var buf [4096]byte

	syscall.Seek(r.lockFd, 0, 0) // Rewind to the start of the file
	n, err := syscall.Read(r.lockFd, buf[:])
	if err != nil {
		panic(err)
	}

	return string(buf[:n])
}

func (r *RefsCache) InvalidateCache() {
	r.Lock()
	defer r.Unlock()

	newUuid := uuid.NewV4()

	// Truncate and write a new UUID in the lock file
	syscall.Ftruncate(r.lockFd, 0)
	syscall.Write(r.lockFd, []byte(newUuid.String()))

	// Remove cache files
	for _, cachePath := range r.cachePaths {
		os.Remove(cachePath)
	}
}

func (r *RefsCache) ExecuteToFile(command string, f *os.File) {
	var out bytes.Buffer
	cmd := exec.Command("git", command, "--stateless-rpc", "--advertise-refs", r.repositoryPath)
	cmd.Stdout = &out

	err := cmd.Run()
	if err != nil {
		panic(err)
	}

	// Print to Stdout
	out.WriteTo(os.Stdout)

	// Print to file
	_, err = f.Write(out.Bytes())
	if err != nil {
		panic(err)
	}
}

func (r *RefsCache) Cache(command string) error {
	if command != "upload-pack" && command != "receive-pack" {
		return fmt.Errorf("Invalid command")
	}

	r.Lock()
	defer r.Unlock()

	currentUuid := r.GetLockUuid()
	fmt.Printf(path.Join(os.TempDir(), "command_output"))
	tempFile, err := ioutil.TempFile(os.TempDir(), "command_output")
	if err != nil {
		panic(err)
	}
	defer tempFile.Close()

	r.ExecuteToFile(command, tempFile)

	if newUuid := r.GetLockUuid(); currentUuid == newUuid {
		cacheFile, err := os.Create(r.cachePaths[command])
		if err != nil {
			panic(err)
		}
		defer cacheFile.Close()
		tempFile.Seek(0, 0) // Reset tempFile to the start of the file
		io.Copy(cacheFile, tempFile)
	}

	return nil
}
