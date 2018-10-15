package repository

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"time"

	"gitlab.com/gitlab-org/gitaly-proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var lockFiles = []string{"config.lock", "HEAD.lock"}

func (server) Cleanup(_ctx context.Context, in *gitalypb.CleanupRequest) (*gitalypb.CleanupResponse, error) {
	repoPath, err := helper.GetRepoPath(in.GetRepository())
	if err != nil {
		return nil, err
	}

	if err := cleanupRepo(repoPath); err != nil {
		return nil, err
	}

	return &gitalypb.CleanupResponse{}, nil
}

func cleanupRepo(repoPath string) error {
	threshold := time.Now().Add(-1 * time.Hour)
	if err := cleanRefsLocks(filepath.Join(repoPath, "refs"), threshold); err != nil {
		return status.Errorf(codes.Internal, "Cleanup: cleanRefsLocks: %v", err)
	}
	if err := cleanPackedRefsLock(repoPath, threshold); err != nil {
		return status.Errorf(codes.Internal, "Cleanup: cleanPackedRefsLock: %v", err)
	}

	worktreeThreshold := time.Now().Add(-6 * time.Hour)
	if err := cleanStaleWorktrees(repoPath, worktreeThreshold); err != nil {
		return status.Errorf(codes.Internal, "Cleanup: cleanStaleWorktrees: %v", err)
	}

	configLockThreshod := time.Now().Add(-15 * time.Minute)
	if err := cleanFileLocks(repoPath, configLockThreshod); err != nil {
		return status.Errorf(codes.Internal, "Cleanup: cleanupConfigLock: %v", err)
	}

	return nil
}

func cleanRefsLocks(rootPath string, threshold time.Time) error {
	return filepath.Walk(rootPath, func(path string, info os.FileInfo, err error) error {
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

		if strings.HasSuffix(info.Name(), ".lock") && info.ModTime().Before(threshold) {
			if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
				return err
			}
		}

		return nil
	})
}

func cleanPackedRefsLock(repoPath string, threshold time.Time) error {
	path := filepath.Join(repoPath, "packed-refs.lock")
	fileInfo, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	if fileInfo.ModTime().Before(threshold) {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return err
		}
	}

	return nil
}

func cleanStaleWorktrees(repoPath string, threshold time.Time) error {
	worktreePath := filepath.Join(repoPath, "worktrees")

	dirInfo, err := os.Stat(worktreePath)
	if err != nil {
		if os.IsNotExist(err) || !dirInfo.IsDir() {
			return nil
		}
		return err
	}

	worktreeEntries, err := ioutil.ReadDir(worktreePath)
	if err != nil {
		return err
	}

	for _, info := range worktreeEntries {
		if !info.IsDir() || (info.Mode()&os.ModeSymlink != 0) {
			continue
		}

		path := filepath.Join(worktreePath, info.Name())

		if info.ModTime().Before(threshold) {
			if err := os.RemoveAll(path); err != nil && !os.IsNotExist(err) {
				return err
			}
		}
	}

	return nil
}

func cleanFileLocks(repoPath string, threshold time.Time) error {
	for _, fileName := range lockFiles {
		lockPath := filepath.Join(repoPath, fileName)

		fi, err := os.Stat(lockPath)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return err
		}

		if fi.ModTime().Before(threshold) {
			if err := os.Remove(lockPath); err != nil && !os.IsNotExist(err) {
				return err
			}
		}
	}

	return nil
}
