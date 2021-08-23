package backup

import (
	"context"
	"path/filepath"
	"strings"

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
type LegacyLocator struct {
}

// BeginFull returns the static paths for a legacy repository backup
func (l LegacyLocator) BeginFull(ctx context.Context, repo *gitalypb.Repository, backupID string) (*Full, error) {
	return l.newFull(repo), nil
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
