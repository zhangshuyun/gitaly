package caching

import (
	"os"
	"path"
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
