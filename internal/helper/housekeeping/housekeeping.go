package housekeeping

import (
	"os"
	"path/filepath"
	"strings"
	"time"

	grpc_logrus "github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/context"
)

const deleteTempFilesOlderThanDuration = 7 * 24 * time.Hour

// Perform will perform housekeeping duties on a repository
func Perform(ctx context.Context, repoPath string) error {
	logger := grpc_logrus.Extract(ctx).WithField("system", "housekeeping")

	filesMarkedForRemoval := 0
	unremovableFiles := 0

	err := filepath.Walk(repoPath, func(path string, info os.FileInfo, _ error) error {
		if repoPath == path {
			// Never consider the root path
			return nil
		}

		if info == nil || !shouldRemove(path, info.ModTime(), info.Mode()) {
			return nil
		}

		filesMarkedForRemoval++

		if err := forceRemove(path); err != nil {
			unremovableFiles++
			logger.WithError(err).WithField("path", path).Warn("unable to remove stray file")
		}

		if info.IsDir() {
			// Do not walk removed directories
			return filepath.SkipDir
		}

		return nil
	})

	if filesMarkedForRemoval > 0 {
		logger.WithFields(log.Fields{
			"files":    filesMarkedForRemoval,
			"failures": unremovableFiles,
		}).Info("removed files")
	}

	return err
}

// FixDirectoryPermissions does a recursive directory walk to look for
// directories that cannot be accessed by the current user, and tries to
// fix those with chmod. The motivating problem is that directories with mode
// 0 break os.RemoveAll.
func FixDirectoryPermissions(path string) error {
	return fixDirectoryPermissions(path, make(map[string]struct{}))
}

const minimumDirPerm = 0700

func fixDirectoryPermissions(path string, retriedPaths map[string]struct{}) error {
	return filepath.Walk(path, func(path string, info os.FileInfo, errIncoming error) error {
		if !info.IsDir() || info.Mode()&minimumDirPerm == minimumDirPerm {
			return nil
		}

		if err := os.Chmod(path, info.Mode()|minimumDirPerm); err != nil {
			return err
		}

		if _, retried := retriedPaths[path]; !retried && os.IsPermission(errIncoming) {
			retriedPaths[path] = struct{}{}
			return fixDirectoryPermissions(path, retriedPaths)
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
	if err := FixDirectoryPermissions(path); err != nil {
		return err
	}

	return os.RemoveAll(path)
}

func shouldRemove(path string, modTime time.Time, mode os.FileMode) bool {
	base := filepath.Base(path)

	// Only delete entries starting with `tmp_` and older than a week
	return strings.HasPrefix(base, "tmp_") && time.Since(modTime) >= deleteTempFilesOlderThanDuration
}
