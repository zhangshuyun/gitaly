package quarantine

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/tempdir"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"google.golang.org/protobuf/proto"
)

// Dir is a quarantine directory for Git objects. Instead of writing new commits into the main
// repository, they're instead written into a temporary quarantine directory. This staging area can
// either be migrated into the main repository or, alternatively, will automatically be discarded
// when the context gets cancelled. If the quarantine environment is discarded without being staged,
// then none of the objects which have been created in the quarantine directory will end up in the
// main repository.
type Dir struct {
	repo            *gitalypb.Repository
	quarantinedRepo *gitalypb.Repository
	dir             tempdir.Dir
	locator         storage.Locator
}

// New creates a new quarantine directory and returns both the directory as well as its cleanup
// function.
func New(ctx context.Context, repo *gitalypb.Repository, locator storage.Locator) (*Dir, error) {
	repoPath, err := locator.GetPath(repo)
	if err != nil {
		return nil, helper.ErrInternalf("getting repo path: %w", err)
	}

	quarantineDir, err := tempdir.NewWithPrefix(ctx, repo.GetStorageName(),
		storage.QuarantineDirectoryPrefix(repo), locator)
	if err != nil {
		return nil, fmt.Errorf("creating quarantine: %w", err)
	}

	relativePath, err := filepath.Rel(repoPath, quarantineDir.Path())
	if err != nil {
		return nil, fmt.Errorf("creating quarantine: %w", err)
	}

	// All paths are relative to the repository root.
	alternateObjectDirs := []string{"objects"}
	if repo.GetGitObjectDirectory() != "" {
		alternateObjectDirs = append(alternateObjectDirs, repo.GetGitObjectDirectory())
	}
	alternateObjectDirs = append(alternateObjectDirs, repo.GetGitAlternateObjectDirectories()...)

	quarantinedRepo := proto.Clone(repo).(*gitalypb.Repository)
	quarantinedRepo.GitObjectDirectory = relativePath
	quarantinedRepo.GitAlternateObjectDirectories = alternateObjectDirs

	return &Dir{
		repo:            repo,
		quarantinedRepo: quarantinedRepo,
		locator:         locator,
		dir:             quarantineDir,
	}, nil
}

// QuarantinedRepo returns a Repository protobuf message with adjusted main and alternate object
// directories. If passed e.g. to the `git.ExecCommandFactory`, then all new objects will end up in
// the quarantine directory.
func (d *Dir) QuarantinedRepo() *gitalypb.Repository {
	return d.quarantinedRepo
}

// Migrate migrates all objects part of the quarantine directory into the main repository and thus
// makes them generally available. This implementation follows the git.git's `tmp_objdir_migrate()`.
func (d *Dir) Migrate() error {
	repoPath, err := d.locator.GetPath(d.repo)
	if err != nil {
		return fmt.Errorf("migrating quarantine: %w", err)
	}

	return migrate(d.dir.Path(), filepath.Join(repoPath, "objects"))
}

func migrate(sourcePath, targetPath string) error {
	entries, err := os.ReadDir(sourcePath)
	if err != nil {
		return fmt.Errorf("reading directory: %w", err)
	}
	sortEntries(entries)

	for _, entry := range entries {
		if entry.Name() == "." {
			continue
		}

		nestedTargetPath := filepath.Join(targetPath, entry.Name())
		nestedSourcePath := filepath.Join(sourcePath, entry.Name())

		if entry.IsDir() {
			if err := os.Mkdir(nestedTargetPath, 0o777); err != nil {
				if !errors.Is(err, os.ErrExist) {
					return fmt.Errorf("creating target directory %q: %w", nestedTargetPath, err)
				}
			}

			if err := migrate(nestedSourcePath, nestedTargetPath); err != nil {
				return fmt.Errorf("migrating directory %q: %w", nestedSourcePath, err)
			}

			continue
		}

		if err := finalizeObjectFile(nestedSourcePath, nestedTargetPath); err != nil {
			return fmt.Errorf("migrating object file %q: %w", nestedSourcePath, err)
		}
	}

	if err := os.Remove(sourcePath); err != nil {
		return fmt.Errorf("removing source directory: %w", err)
	}

	return nil
}

// finalizeObjectFile will move the object file (either a packfile, its metadata or or a loose
// object) to the target path. The move is either done with a hard link if supported or with a
// rename. No error is raised in case the target path exists already.
func finalizeObjectFile(sourcePath, targetPath string) error {
	// We first try to link the file via a hardlink. The benefit compared to doing a rename is
	// that in case of a collision, we do not replace the target.
	err := os.Link(sourcePath, targetPath)

	// In case the hardlink failed, we fall back to a rename.
	renamed := false
	if err != nil && !errors.Is(err, os.ErrExist) {
		err = os.Rename(sourcePath, targetPath)
		renamed = err == nil
	}

	if err != nil && !errors.Is(err, os.ErrExist) {
		return fmt.Errorf("finalizing object file: %w", err)
	}

	if !renamed {
		// It's fair to ignore the error here: we'll purge the quarantine directory anyway
		// in case the context gets cancelled.
		_ = os.Remove(sourcePath)
	}

	return nil
}

// sortEntries sorts packfiles and their associated metafiles such that we copy them over in the
// correct order.
func sortEntries(entries []os.DirEntry) {
	sort.SliceStable(entries, func(i, j int) bool {
		return packCopyPriority(entries[i].Name()) < packCopyPriority(entries[j].Name())
	})
}

func packCopyPriority(name string) int {
	switch {
	case !strings.HasPrefix(name, "pack"):
		return 0
	case strings.HasSuffix(name, ".keep"):
		return 1
	case strings.HasSuffix(name, ".pack"):
		return 2
	case strings.HasSuffix(name, ".rev"):
		return 3
	case strings.HasSuffix(name, ".idx"):
		return 4
	default:
		return 5
	}
}
