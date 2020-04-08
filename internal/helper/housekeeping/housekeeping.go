package housekeeping

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	log "github.com/sirupsen/logrus"
)

const deleteTempFilesOlderThanDuration = 7 * 24 * time.Hour

// Perform will perform housekeeping duties on a repository
func Perform(ctx context.Context, repoPath string) error {
	logger := myLogger(ctx)

	filesMarkedForRemoval := 0
	unremovableFiles := 0

	err := filepath.Walk(repoPath, func(path string, info os.FileInfo, err error) error {
		if info == nil {
			logger.WithFields(log.Fields{
				"path": path,
			}).WithError(err).Error("nil FileInfo in housekeeping.Perform")

			return nil
		}

		if repoPath == path {
			// Never consider the root path
			return nil
		}

		if !shouldRemove(path, info.ModTime(), info.Mode()) {
			return nil
		}

		filesMarkedForRemoval++

		if err := forceRemove(ctx, path); err != nil {
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

func myLogger(ctx context.Context) *log.Entry {
	return ctxlogrus.Extract(ctx).WithField("system", "housekeeping")
}

// FixDirectoryPermissions does a recursive directory walk to look for
// directories that cannot be accessed by the current user, and tries to
// fix those with chmod. The motivating problem is that directories with mode
// 0 break os.RemoveAll.
func FixDirectoryPermissions(ctx context.Context, path string) error {
	return fixDirectoryPermissions(ctx, path, make(map[string]struct{}))
}

const minimumDirPerm = 0700

func fixDirectoryPermissions(ctx context.Context, path string, retriedPaths map[string]struct{}) error {
	logger := myLogger(ctx)
	return filepath.Walk(path, func(path string, info os.FileInfo, errIncoming error) error {
		if info == nil {
			logger.WithFields(log.Fields{
				"path": path,
			}).WithError(errIncoming).Error("nil FileInfo in housekeeping.fixDirectoryPermissions")

			return nil
		}

		if !info.IsDir() || info.Mode()&minimumDirPerm == minimumDirPerm {
			return nil
		}

		if err := os.Chmod(path, info.Mode()|minimumDirPerm); err != nil {
			return err
		}

		if _, retried := retriedPaths[path]; !retried && os.IsPermission(errIncoming) {
			retriedPaths[path] = struct{}{}
			return fixDirectoryPermissions(ctx, path, retriedPaths)
		}

		return nil
	})
}

// Delete a directory structure while ensuring the current user has permission to delete the directory structure
func forceRemove(ctx context.Context, path string) error {
	err := os.RemoveAll(path)
	if err == nil {
		return nil
	}

	// Delete failed. Try again after chmod'ing directories recursively
	if err := FixDirectoryPermissions(ctx, path); err != nil {
		return err
	}

	return os.RemoveAll(path)
}

func shouldRemove(path string, modTime time.Time, mode os.FileMode) bool {
	base := filepath.Base(path)

	// Only delete entries starting with `tmp_` and older than a week
	return strings.HasPrefix(base, "tmp_") && time.Since(modTime) >= deleteTempFilesOlderThanDuration
}
