package backup

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
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
func (l LegacyLocator) BeginFull(ctx context.Context, repo *gitalypb.Repository, backupID string) *Full {
	return l.newFull(repo)
}

// CommitFull is unused as the locations are static
func (l LegacyLocator) CommitFull(ctx context.Context, full *Full) error {
	return nil
}

// FindLatestFull returns the static paths for a legacy repository backup
func (l LegacyLocator) FindLatestFull(ctx context.Context, repo *gitalypb.Repository) (*Full, error) {
	return l.newFull(repo), nil
}

func (l LegacyLocator) newFull(repo *gitalypb.Repository) *Full {
	backupPath := strings.TrimSuffix(repo.RelativePath, ".git")

	return &Full{
		BundlePath:      backupPath + ".bundle",
		RefPath:         backupPath + ".refs",
		CustomHooksPath: filepath.Join(backupPath, "custom_hooks.tar"),
	}
}

// PointerLocator locates backup paths where each full backup is put into a
// unique timestamp directory and the latest backup taken is pointed to by a
// file named LATEST.
//
// Structure:
//   <repo relative path>/<backup id>/full.bundle
//   <repo relative path>/<backup id>/full.refs
//   <repo relative path>/<backup id>/custom_hooks.tar
//   <repo relative path>/LATEST
type PointerLocator struct {
	Sink     Sink
	Fallback Locator
}

// BeginFull returns paths for a new full backup
func (l PointerLocator) BeginFull(ctx context.Context, repo *gitalypb.Repository, backupID string) *Full {
	backupPath := strings.TrimSuffix(repo.RelativePath, ".git")

	return &Full{
		BundlePath:      filepath.Join(backupPath, backupID, "full.bundle"),
		RefPath:         filepath.Join(backupPath, backupID, "full.refs"),
		CustomHooksPath: filepath.Join(backupPath, backupID, "custom_hooks.tar"),
	}
}

// CommitFull persists the paths for a new backup so that it can be looked up by FindLatestFull
func (l PointerLocator) CommitFull(ctx context.Context, full *Full) error {
	bundleDir := filepath.Dir(full.BundlePath)
	backupID := filepath.Base(bundleDir)
	backupPath := filepath.Dir(bundleDir)
	return l.commitLatestID(ctx, backupPath, backupID)
}

// FindLatestFull returns the paths committed by the latest call to CommitFull.
//
// If there is no `LATEST` file, the result of the `Fallback` is used.
func (l PointerLocator) FindLatestFull(ctx context.Context, repo *gitalypb.Repository) (*Full, error) {
	backupPath := strings.TrimSuffix(repo.RelativePath, ".git")

	latest, err := l.findLatestID(ctx, backupPath)
	if err != nil {
		if l.Fallback != nil && errors.Is(err, ErrDoesntExist) {
			return l.Fallback.FindLatestFull(ctx, repo)
		}
		return nil, fmt.Errorf("pointer locator: %w", err)
	}

	return &Full{
		BundlePath:      filepath.Join(backupPath, latest, "full.bundle"),
		RefPath:         filepath.Join(backupPath, latest, "full.refs"),
		CustomHooksPath: filepath.Join(backupPath, latest, "custom_hooks.tar"),
	}, nil
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

func (l PointerLocator) commitLatestID(ctx context.Context, backupPath, backupID string) error {
	latest := strings.NewReader(backupID)
	if err := l.Sink.Write(ctx, filepath.Join(backupPath, "LATEST"), latest); err != nil {
		return fmt.Errorf("commit latest ID: %w", err)
	}
	return nil
}
