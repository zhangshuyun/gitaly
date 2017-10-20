package housekeeping

import (
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus"
	"golang.org/x/net/context"
)

const deleteTempFilesOlderThanDuration = 7 * 24 * time.Hour

// Perform will perform housekeeping duties on a repository
func Perform(ctx context.Context, repoPath string) error {
	log := grpc_logrus.Extract(ctx)

	return filepath.Walk(repoPath, func(path string, info os.FileInfo, err error) error {
		if repoPath == path {
			// Never consider the root path
			return nil
		}

		if info == nil || !shouldRemove(path, info.ModTime(), info.Mode(), err) {
			return nil
		}

		err = forceRemove(path)
		if err != nil {
			log.WithError(err).WithField("path", path).Warn("Unable to remove stray file")
		}

		if info.IsDir() {
			// Do not walk removed directories
			return filepath.SkipDir
		}

		return nil
	})
}

func fixPermissions(path string) {
	filepath.Walk(path, func(path string, info os.FileInfo, err error) error {
		if info.IsDir() {
			os.Chmod(path, 0700)
		}

		return nil
	})
}

// Delete a directory structure while ensuring the current user has permission to delete the directory structure
func forceRemove(path string) error {
	err := os.RemoveAll(path)
	if err == nil {
		return nil
	}

	// Delete failed. Try again after chmod'ing directories recursively
	fixPermissions(path)

	return os.RemoveAll(path)
}

func shouldRemove(path string, modTime time.Time, mode os.FileMode, err error) bool {
	base := filepath.Base(path)

	// Only delete entries starting with `tmp_` and older than a week
	return strings.HasPrefix(base, "tmp_") && time.Since(modTime) >= deleteTempFilesOlderThanDuration
}
