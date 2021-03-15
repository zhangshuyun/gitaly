package housekeeping

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"google.golang.org/grpc/codes"
)

const (
	emptyRefsGracePeriod             = 24 * time.Hour
	deleteTempFilesOlderThanDuration = 7 * 24 * time.Hour
	brokenRefsGracePeriod            = 24 * time.Hour
	minimumDirPerm                   = 0700
	lockfileGracePeriod              = 15 * time.Minute
	referenceLockfileGracePeriod     = 1 * time.Hour
	packedRefsLockGracePeriod        = 1 * time.Hour
	packedRefsNewGracePeriod         = 15 * time.Minute
)

var (
	lockfiles = []string{
		"config.lock",
		"HEAD.lock",
		"objects/info/commit-graphs/commit-graph-chain.lock",
	}

	optimizeEmptyDirRemovalTotals = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "gitaly",
			Subsystem: "repository",
			Name:      "optimizerepository_empty_dir_removal_total",
			Help:      "Total number of empty directories removed by OptimizeRepository RPC",
		},
	)
)

func init() {
	prometheus.MustRegister(optimizeEmptyDirRemovalTotals)
}

type staleFileFinderFn func(context.Context, string) ([]string, error)

// Perform will perform housekeeping duties on a repository
func Perform(ctx context.Context, repo *localrepo.Repo) error {
	repoPath, err := repo.Path()
	if err != nil {
		myLogger(ctx).WithError(err).Warn("housekeeping failed to get repo path")
		if helper.GrpcCode(err) == codes.NotFound {
			return nil
		}
		return fmt.Errorf("housekeeping failed to get repo path: %w", err)
	}

	logEntry := myLogger(ctx)
	var filesToPrune []string

	for field, staleFileFinder := range map[string]staleFileFinderFn{
		"objects":        findTemporaryObjects,
		"locks":          findStaleLockfiles,
		"refs":           findBrokenLooseReferences,
		"reflocks":       findStaleReferenceLocks,
		"packedrefslock": findPackedRefsLock,
		"packedrefsnew":  findPackedRefsNew,
	} {
		staleFiles, err := staleFileFinder(ctx, repoPath)
		if err != nil {
			return fmt.Errorf("housekeeping failed to find %s: %w", field, err)
		}

		filesToPrune = append(filesToPrune, staleFiles...)
		logEntry = logEntry.WithField(field, len(staleFiles))
	}

	unremovableFiles := 0
	for _, path := range filesToPrune {
		if err := os.Remove(path); err != nil {
			if os.IsNotExist(err) {
				continue
			}
			unremovableFiles++
			// We cannot use `logEntry` here as it's already seeded
			// with the statistics fields.
			myLogger(ctx).WithError(err).WithField("path", path).Warn("unable to remove stale file")
		}
	}

	if len(filesToPrune) > 0 {
		logEntry.WithField("failures", unremovableFiles).Info("removed files")
	}

	if err := removeRefEmptyDirs(ctx, repo); err != nil {
		return fmt.Errorf("housekeeping could not remove empty refs: %w", err)
	}

	// TODO: https://gitlab.com/gitlab-org/gitaly/-/issues/3138
	// This is a temporary code and needs to be removed once it will be run on all repositories at least once.
	if err := unsetAllConfigsByRegexp(ctx, repo, "^http\\..+\\.extraHeader$"); err != nil {
		return fmt.Errorf("housekeeping could not unset extreHeaders: %w", err)
	}

	return nil
}

// findStaleFiles determines whether any of the given files rooted at repoPath
// are stale or not. A file is considered stale if it exists and if it has not
// been modified during the gracePeriod. A nonexistent file is not considered
// to be a stale file and will not cause an error.
func findStaleFiles(repoPath string, gracePeriod time.Duration, files ...string) ([]string, error) {
	var staleFiles []string

	for _, file := range files {
		path := filepath.Join(repoPath, file)

		fileInfo, err := os.Stat(path)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return nil, err
		}

		if time.Since(fileInfo.ModTime()) < gracePeriod {
			continue
		}

		staleFiles = append(staleFiles, path)
	}

	return staleFiles, nil
}

// findStaleLockfiles finds a subset of lockfiles which may be created by git
// commands. We're quite conservative with what we're removing, we certaintly
// don't just scan the repo for `*.lock` files. Instead, we only remove a known
// set of lockfiles which have caused problems in the past.
func findStaleLockfiles(ctx context.Context, repoPath string) ([]string, error) {
	return findStaleFiles(repoPath, lockfileGracePeriod, lockfiles...)
}

func findTemporaryObjects(ctx context.Context, repoPath string) ([]string, error) {
	var temporaryObjects []string

	logger := myLogger(ctx)

	err := filepath.Walk(filepath.Join(repoPath, "objects"), func(path string, info os.FileInfo, err error) error {
		if info == nil {
			logger.WithFields(log.Fields{
				"path": path,
			}).WithError(err).Error("nil FileInfo in housekeeping.Perform")

			return nil
		}

		// Git will never create temporary directories, but only
		// temporary objects, packfiles and packfile indices.
		if info.IsDir() {
			return nil
		}

		if !isStaleTemporaryObject(path, info.ModTime(), info.Mode()) {
			return nil
		}

		temporaryObjects = append(temporaryObjects, path)

		return nil
	})
	if err != nil {
		return nil, err
	}

	return temporaryObjects, nil
}

func isStaleTemporaryObject(path string, modTime time.Time, mode os.FileMode) bool {
	base := filepath.Base(path)

	// Only delete entries starting with `tmp_` and older than a week
	return strings.HasPrefix(base, "tmp_") && time.Since(modTime) >= deleteTempFilesOlderThanDuration
}

func findBrokenLooseReferences(ctx context.Context, repoPath string) ([]string, error) {
	logger := myLogger(ctx)

	var brokenRefs []string
	err := filepath.Walk(filepath.Join(repoPath, "refs"), func(path string, info os.FileInfo, err error) error {
		if info == nil {
			logger.WithFields(log.Fields{
				"path": path,
			}).WithError(err).Error("nil FileInfo in housekeeping.Perform")

			return nil
		}

		// When git crashes or a node reboots, it may happen that it leaves behind empty
		// references. These references break various assumptions made by git and cause it
		// to error in various circumstances. We thus clean them up to work around the
		// issue.
		if info.IsDir() || info.Size() > 0 || time.Since(info.ModTime()) < brokenRefsGracePeriod {
			return nil
		}

		brokenRefs = append(brokenRefs, path)

		return nil
	})
	if err != nil {
		return nil, err
	}

	return brokenRefs, nil
}

// findStaleReferenceLocks scans the refdb for stale locks for loose references.
func findStaleReferenceLocks(ctx context.Context, repoPath string) ([]string, error) {
	var staleReferenceLocks []string

	err := filepath.Walk(filepath.Join(repoPath, "refs"), func(path string, info os.FileInfo, err error) error {
		if os.IsNotExist(err) {
			// Race condition: somebody already deleted the file for us. Ignore this file.
			return nil
		}

		if err != nil {
			return err
		}

		if info.IsDir() {
			return nil
		}

		if !strings.HasSuffix(info.Name(), ".lock") || time.Since(info.ModTime()) < referenceLockfileGracePeriod {
			return nil
		}

		staleReferenceLocks = append(staleReferenceLocks, path)
		return nil
	})
	if err != nil {
		return nil, err
	}

	return staleReferenceLocks, nil
}

// findPackedRefsLock returns stale lockfiles for the packed-refs file.
func findPackedRefsLock(ctx context.Context, repoPath string) ([]string, error) {
	return findStaleFiles(repoPath, packedRefsLockGracePeriod, "packed-refs.lock")
}

// findPackedRefsNew returns stale temporary packed-refs files.
func findPackedRefsNew(ctx context.Context, repoPath string) ([]string, error) {
	return findStaleFiles(repoPath, packedRefsNewGracePeriod, "packed-refs.new")
}

// FixDirectoryPermissions does a recursive directory walk to look for
// directories that cannot be accessed by the current user, and tries to
// fix those with chmod. The motivating problem is that directories with mode
// 0 break os.RemoveAll.
func FixDirectoryPermissions(ctx context.Context, path string) error {
	return fixDirectoryPermissions(ctx, path, make(map[string]struct{}))
}

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

func removeRefEmptyDirs(ctx context.Context, repository *localrepo.Repo) error {
	rPath, err := repository.Path()
	if err != nil {
		return err
	}
	repoRefsPath := filepath.Join(rPath, "refs")

	// we never want to delete the actual "refs" directory, so we start the
	// recursive functions for each subdirectory
	entries, err := ioutil.ReadDir(repoRefsPath)
	if err != nil {
		return err
	}

	for _, e := range entries {
		if !e.IsDir() {
			continue
		}

		ePath := filepath.Join(repoRefsPath, e.Name())
		if err := removeEmptyDirs(ctx, ePath); err != nil {
			return err
		}
	}

	return nil
}

func removeEmptyDirs(ctx context.Context, target string) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	// We need to stat the directory early on in order to get its current mtime. If we
	// did this after we have removed empty child directories, then its mtime would've
	// changed and we wouldn't consider it for deletion.
	dirStat, err := os.Stat(target)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	entries, err := ioutil.ReadDir(target)
	switch {
	case os.IsNotExist(err):
		return nil // race condition: someone else deleted it first
	case err != nil:
		return err
	}

	for _, e := range entries {
		if !e.IsDir() {
			continue
		}

		ePath := filepath.Join(target, e.Name())
		if err := removeEmptyDirs(ctx, ePath); err != nil {
			return err
		}
	}

	// If the directory is older than the grace period for empty refs, then we can
	// consider it for deletion in case it's empty.
	if time.Since(dirStat.ModTime()) < emptyRefsGracePeriod {
		return nil
	}

	// recheck entries now that we have potentially removed some dirs
	entries, err = ioutil.ReadDir(target)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	if len(entries) > 0 {
		return nil
	}

	switch err := os.Remove(target); {
	case os.IsNotExist(err):
		return nil // race condition: someone else deleted it first
	case err != nil:
		return err
	}
	optimizeEmptyDirRemovalTotals.Inc()

	return nil
}

func unsetAllConfigsByRegexp(ctx context.Context, repository *localrepo.Repo, regexp string) error {
	config := repository.Config()

	configPairs, err := config.GetRegexp(ctx, regexp, git.ConfigGetRegexpOpts{})
	if err != nil {
		return fmt.Errorf("get config keys: %w", err)
	}

	for _, configPair := range configPairs {
		if err := config.Unset(ctx, configPair.Key, git.ConfigUnsetOpts{
			All: true,
		}); err != nil {
			return fmt.Errorf("unset all: %w", err)
		}
	}

	return nil
}

func myLogger(ctx context.Context) *log.Entry {
	return ctxlogrus.Extract(ctx).WithField("system", "housekeeping")
}
