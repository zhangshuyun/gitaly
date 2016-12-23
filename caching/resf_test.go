package caching

import (
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"syscall"
	"testing"
)

const tmpDir string = "tmp"

func TestNewRefsCache(t *testing.T) {
	NewRefsCache(tmpDir)
	defer os.RemoveAll(tmpDir)

	if _, err := os.Stat(path.Join(tmpDir, "gitlab-cache", "info-refs")); os.IsNotExist(err) {
		t.Error("Expected NewRefsCache to create refs folder")
	}
	if _, err := os.Stat(path.Join(tmpDir, "gitlab-cache", "info-refs", "lock")); os.IsNotExist(err) {
		t.Error("Expected NewRefsCache to create lock file")
	}
}

func TestInvalidateCache(t *testing.T) {
	r := NewRefsCache(tmpDir)
	defer os.RemoveAll(tmpDir)

	syscall.Write(r.lockFd, []byte("rewrite-lock"))
	ioutil.WriteFile(path.Join(r.cacheDir, "upload-pack"), []byte(""), 0644)
	ioutil.WriteFile(path.Join(r.cacheDir, "receive-pack"), []byte(""), 0644)

	r.InvalidateCache()

	if dat, _ := ioutil.ReadFile(path.Join(r.cacheDir, "lock")); string(dat) == "rewrite-lock" {
		t.Error("Expected InvalidateCache to rewrite lock")
	}

	if _, err := os.Stat(r.cachePaths["upload-pack"]); !os.IsNotExist(err) {
		t.Error("Expected InvalidateCache to remove upload-pack cache")
	}
	if _, err := os.Stat(r.cachePaths["receive-pack"]); !os.IsNotExist(err) {
		t.Error("Expected InvalidateCache to remove receive-pack cache")
	}
}

func TestCache(t *testing.T) {
	source := "https://gitlab.com/gitlab-org/gitlab-test.git"
	repoDir := path.Join(tmpDir, "gitlab-test")
	exec.Command("git", "clone", "--bare", source, repoDir).Run()
	r := NewRefsCache(repoDir)
	defer os.RemoveAll(tmpDir)

	if err := r.Cache("non-existing-command"); err.Error() != "Invalid command" {
		t.Error("Expected Cache to fail with \"Invalid command\"")
	}

	r.Cache("upload-pack")

	if _, err := os.Stat(r.cachePaths["upload-pack"]); os.IsNotExist(err) {
		t.Error("Expected Cache to create upload-pack cache")
	}
}
