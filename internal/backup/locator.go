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

// BeginFull returns paths for a new full backup
func (l PointerLocator) BeginFull(ctx context.Context, repo *gitalypb.Repository, backupID string) *Step {
	backupPath := strings.TrimSuffix(repo.RelativePath, ".git")

	return &Step{
		BundlePath:      filepath.Join(backupPath, backupID, "001.bundle"),
		RefPath:         filepath.Join(backupPath, backupID, "001.refs"),
		CustomHooksPath: filepath.Join(backupPath, backupID, "001.custom_hooks.tar"),
	}
}

// Commit persists the paths for a new backup so that it can be looked up by FindLatest
func (l PointerLocator) Commit(ctx context.Context, full *Step) error {
	bundleDir := filepath.Dir(full.BundlePath)
	backupID := filepath.Base(bundleDir)
	backupPath := filepath.Dir(bundleDir)
	if err := l.writeLatest(ctx, bundleDir, "001"); err != nil {
		return err
	}
	if err := l.writeLatest(ctx, backupPath, backupID); err != nil {
		return err
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
		return nil, fmt.Errorf("pointer locator: backup: %w", err)
	}

	backupPath := filepath.Join(repoPath, backupID)

	latestIncrementID, err := l.findLatestID(ctx, backupPath)
	if err != nil {
		return nil, fmt.Errorf("pointer locator: latest incremental: %w", err)
	}

	max, err := strconv.Atoi(latestIncrementID)
	if err != nil {
		return nil, fmt.Errorf("pointer locator: latest incremental: %w", err)
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
