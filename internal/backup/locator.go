package backup

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strconv"
	"strings"

	"gitlab.com/gitlab-org/gitaly/v14/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

// LegacyLocator locates backup paths for historic backups. This is the
// structure that gitlab used before incremental backups were introduced.
//
// Existing backup files are expected to be overwritten by the latest backup
// files.
//
// Structure:
//   <repo relative path>.bundle
//   <repo relative path>.refs
//   <repo relative path>/custom_hooks.tar
type LegacyLocator struct{}

// BeginFull returns the static paths for a legacy repository backup
func (l LegacyLocator) BeginFull(ctx context.Context, repo *gitalypb.Repository, backupID string) *Step {
	return l.newFull(repo)
}

// BeginIncremental is not supported for legacy backups
func (l LegacyLocator) BeginIncremental(ctx context.Context, repo *gitalypb.Repository, backupID string) (*Step, error) {
	return nil, errors.New("legacy layout: begin incremental: not supported")
}

// Commit is unused as the locations are static
func (l LegacyLocator) Commit(ctx context.Context, full *Step) error {
	return nil
}

// FindLatest returns the static paths for a legacy repository backup
func (l LegacyLocator) FindLatest(ctx context.Context, repo *gitalypb.Repository) (*Backup, error) {
	return &Backup{
		Steps: []Step{
			*l.newFull(repo),
		},
	}, nil
}

func (l LegacyLocator) newFull(repo *gitalypb.Repository) *Step {
	backupPath := strings.TrimSuffix(repo.RelativePath, ".git")

	return &Step{
		SkippableOnNotFound: true,
		BundlePath:          backupPath + ".bundle",
		RefPath:             backupPath + ".refs",
		CustomHooksPath:     filepath.Join(backupPath, "custom_hooks.tar"),
	}
}

// PointerLocator locates backup paths where each full backup is put into a
// unique timestamp directory and the latest backup taken is pointed to by a
// file named LATEST.
//
// Structure:
//   <repo relative path>/LATEST
//   <repo relative path>/<backup id>/LATEST
//   <repo relative path>/<backup id>/<nnn>.bundle
//   <repo relative path>/<backup id>/<nnn>.refs
//   <repo relative path>/<backup id>/<nnn>.custom_hooks.tar
type PointerLocator struct {
	Sink     Sink
	Fallback Locator
}

// BeginFull returns a tentative first step needed to create a new full backup.
func (l PointerLocator) BeginFull(ctx context.Context, repo *gitalypb.Repository, backupID string) *Step {
	repoPath := strings.TrimSuffix(repo.RelativePath, ".git")

	return &Step{
		BundlePath:      filepath.Join(repoPath, backupID, "001.bundle"),
		RefPath:         filepath.Join(repoPath, backupID, "001.refs"),
		CustomHooksPath: filepath.Join(repoPath, backupID, "001.custom_hooks.tar"),
	}
}

// BeginIncremental returns a tentative step needed to create a new incremental
// backup.  The incremental backup is always based off of the latest full
// backup. If there is no latest backup, a new full backup step is returned
// using fallbackBackupID
func (l PointerLocator) BeginIncremental(ctx context.Context, repo *gitalypb.Repository, fallbackBackupID string) (*Step, error) {
	repoPath := strings.TrimSuffix(repo.RelativePath, ".git")
	backupID, err := l.findLatestID(ctx, repoPath)
	if err != nil {
		if errors.Is(err, ErrDoesntExist) {
			return l.BeginFull(ctx, repo, fallbackBackupID), nil
		}
		return nil, fmt.Errorf("pointer locator: begin incremental: %w", err)
	}
	backup, err := l.find(ctx, repo, backupID)
	if err != nil {
		return nil, err
	}
	if len(backup.Steps) < 1 {
		return nil, fmt.Errorf("pointer locator: begin incremental: no full backup")
	}

	previous := backup.Steps[len(backup.Steps)-1]
	backupPath := filepath.Dir(previous.BundlePath)
	bundleName := filepath.Base(previous.BundlePath)
	incrementID := bundleName[:len(bundleName)-len(filepath.Ext(bundleName))]
	id, err := strconv.Atoi(incrementID)
	if err != nil {
		return nil, fmt.Errorf("pointer locator: begin incremental: determine increment ID: %w", err)
	}
	id++

	return &Step{
		BundlePath:      filepath.Join(backupPath, fmt.Sprintf("%03d.bundle", id)),
		RefPath:         filepath.Join(backupPath, fmt.Sprintf("%03d.refs", id)),
		PreviousRefPath: previous.RefPath,
		CustomHooksPath: filepath.Join(backupPath, fmt.Sprintf("%03d.custom_hooks.tar", id)),
	}, nil
}

// Commit persists the step so that it can be looked up by FindLatest
func (l PointerLocator) Commit(ctx context.Context, step *Step) error {
	backupPath := filepath.Dir(step.BundlePath)
	bundleName := filepath.Base(step.BundlePath)
	repoPath := filepath.Dir(backupPath)
	backupID := filepath.Base(backupPath)
	incrementID := bundleName[:len(bundleName)-len(filepath.Ext(bundleName))]

	if err := l.writeLatest(ctx, backupPath, incrementID); err != nil {
		return fmt.Errorf("pointer locator: commit: %w", err)
	}
	if err := l.writeLatest(ctx, repoPath, backupID); err != nil {
		return fmt.Errorf("pointer locator: commit: %w", err)
	}
	return nil
}

// FindLatest returns the paths committed by the latest call to CommitFull.
//
// If there is no `LATEST` file, the result of the `Fallback` is used.
func (l PointerLocator) FindLatest(ctx context.Context, repo *gitalypb.Repository) (*Backup, error) {
	repoPath := strings.TrimSuffix(repo.RelativePath, ".git")

	backupID, err := l.findLatestID(ctx, repoPath)
	if err != nil {
		if l.Fallback != nil && errors.Is(err, ErrDoesntExist) {
			return l.Fallback.FindLatest(ctx, repo)
		}
		return nil, fmt.Errorf("pointer locator: find latest: %w", err)
	}

	backup, err := l.find(ctx, repo, backupID)
	if err != nil {
		return nil, fmt.Errorf("pointer locator: find latest: %w", err)
	}
	return backup, nil
}

// find returns the repository backup at the given backupID. If the backup does
// not exist then the error ErrDoesntExist is returned.
func (l PointerLocator) find(ctx context.Context, repo *gitalypb.Repository, backupID string) (*Backup, error) {
	repoPath := strings.TrimSuffix(repo.RelativePath, ".git")
	backupPath := filepath.Join(repoPath, backupID)

	latestIncrementID, err := l.findLatestID(ctx, backupPath)
	if err != nil {
		return nil, fmt.Errorf("find: %w", err)
	}

	max, err := strconv.Atoi(latestIncrementID)
	if err != nil {
		return nil, fmt.Errorf("find: determine increment ID: %w", err)
	}

	var backup Backup

	for i := 1; i <= max; i++ {
		var previousRefPath string
		if i > 1 {
			previousRefPath = filepath.Join(backupPath, fmt.Sprintf("%03d.refs", i-1))
		}
		backup.Steps = append(backup.Steps, Step{
			BundlePath:      filepath.Join(backupPath, fmt.Sprintf("%03d.bundle", i)),
			RefPath:         filepath.Join(backupPath, fmt.Sprintf("%03d.refs", i)),
			PreviousRefPath: previousRefPath,
			CustomHooksPath: filepath.Join(backupPath, fmt.Sprintf("%03d.custom_hooks.tar", i)),
		})
	}

	return &backup, nil
}

func (l PointerLocator) findLatestID(ctx context.Context, backupPath string) (string, error) {
	r, err := l.Sink.GetReader(ctx, filepath.Join(backupPath, "LATEST"))
	if err != nil {
		return "", fmt.Errorf("find latest ID: %w", err)
	}
	defer r.Close()

	latest, err := io.ReadAll(r)
	if err != nil {
		return "", fmt.Errorf("find latest ID: %w", err)
	}

	return text.ChompBytes(latest), nil
}

func (l PointerLocator) writeLatest(ctx context.Context, path, target string) error {
	latest := strings.NewReader(target)
	if err := l.Sink.Write(ctx, filepath.Join(path, "LATEST"), latest); err != nil {
		return fmt.Errorf("write latest: %w", err)
	}
	return nil
}
