package gittest

import (
	"crypto/sha256"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

const (
	// GlRepository is the default repository name for newly created test
	// repos.
	GlRepository = "project-1"
	// GlProjectPath is the default project path for newly created test
	// repos.
	GlProjectPath = "gitlab-org/gitlab-test"
)

// InitRepoDir creates a temporary directory for a repo, without initializing it
func InitRepoDir(t testing.TB, storagePath, relativePath string) *gitalypb.Repository {
	repoPath := filepath.Join(storagePath, relativePath, "..")
	require.NoError(t, os.MkdirAll(repoPath, 0755), "making repo parent dir")
	return &gitalypb.Repository{
		StorageName:   "default",
		RelativePath:  relativePath,
		GlRepository:  GlRepository,
		GlProjectPath: GlProjectPath,
	}
}

// NewObjectPoolName returns a random pool repository name in format
// '@pools/[0-9a-z]{2}/[0-9a-z]{2}/[0-9a-z]{64}.git'.
func NewObjectPoolName(t testing.TB) string {
	return filepath.Join("@pools", newDiskHash(t)+".git")
}

// NewRepositoryName returns a random repository hash
// in format '@hashed/[0-9a-f]{2}/[0-9a-f]{2}/[0-9a-f]{64}(.git)?'.
func NewRepositoryName(t testing.TB, bare bool) string {
	suffix := ""
	if bare {
		suffix = ".git"
	}

	return filepath.Join("@hashed", newDiskHash(t)+suffix)
}

// newDiskHash generates a random directory path following the Rails app's
// approach in the hashed storage module, formatted as '[0-9a-f]{2}/[0-9a-f]{2}/[0-9a-f]{64}'.
// https://gitlab.com/gitlab-org/gitlab/-/blob/f5c7d8eb1dd4eee5106123e04dec26d277ff6a83/app/models/storage/hashed.rb#L38-43
func newDiskHash(t testing.TB) string {
	// rails app calculates a sha256 and uses its hex representation
	// as the directory path
	b, err := text.RandomHex(sha256.Size)
	require.NoError(t, err)
	return filepath.Join(b[0:2], b[2:4], b)
}

// InitRepoOpts contains options for InitRepo.
type InitRepoOpts struct {
	// WithWorktree determines whether the resulting Git repository should have a worktree or
	// not.
	WithWorktree bool
}

// InitRepo creates a new empty repository in the given storage. You can either pass no or exactly
// one InitRepoOpts.
func InitRepo(t testing.TB, cfg config.Cfg, storage config.Storage, opts ...InitRepoOpts) (*gitalypb.Repository, string) {
	require.Less(t, len(opts), 2, "you must either pass no or exactly one option")

	opt := InitRepoOpts{}
	if len(opts) == 1 {
		opt = opts[0]
	}

	relativePath := NewRepositoryName(t, !opt.WithWorktree)
	repoPath := filepath.Join(storage.Path, relativePath)

	args := []string{"init"}
	if !opt.WithWorktree {
		args = append(args, "--bare")
	}

	Exec(t, cfg, append(args, repoPath)...)

	repo := InitRepoDir(t, storage.Path, relativePath)
	repo.StorageName = storage.Name
	if opt.WithWorktree {
		repo.RelativePath = filepath.Join(repo.RelativePath, ".git")
	}

	t.Cleanup(func() { require.NoError(t, os.RemoveAll(repoPath)) })

	return repo, repoPath
}

// CloneRepoOpts is an option for CloneRepo.
type CloneRepoOpts struct {
	// RelativePath determines the relative path of newly created Git repository. If unset, the
	// relative path is computed via NewRepositoryName.
	RelativePath string
	// WithWorktree determines whether the resulting Git repository should have a worktree or
	// not.
	WithWorktree bool
	// SourceRepo determines the name of the source repository which shall be cloned. The source
	// repository is assumed to be relative to "_build/testrepos". If unset, defaults to
	// "gitlab-test.git".
	SourceRepo string
}

// CloneRepo clones a new copy of test repository under a subdirectory in the storage root. You can
// either pass no or exactly one CloneRepoOpts.
func CloneRepo(t testing.TB, cfg config.Cfg, storage config.Storage, opts ...CloneRepoOpts) (*gitalypb.Repository, string) {
	require.Less(t, len(opts), 2, "you must either pass no or exactly one option")

	opt := CloneRepoOpts{}
	if len(opts) == 1 {
		opt = opts[0]
	}

	relativePath := opt.RelativePath
	if relativePath == "" {
		relativePath = NewRepositoryName(t, !opt.WithWorktree)
	}

	sourceRepo := opt.SourceRepo
	if sourceRepo == "" {
		sourceRepo = "gitlab-test.git"
	}

	repo := InitRepoDir(t, storage.Path, relativePath)
	repo.StorageName = storage.Name

	args := []string{"clone", "--no-hardlinks", "--dissociate"}
	if !opt.WithWorktree {
		args = append(args, "--bare")
	} else {
		// For non-bare repos the relative path is the .git folder inside the path
		repo.RelativePath = filepath.Join(relativePath, ".git")
	}

	absolutePath := filepath.Join(storage.Path, relativePath)
	Exec(t, cfg, append(args, testRepositoryPath(t, sourceRepo), absolutePath)...)

	t.Cleanup(func() { require.NoError(t, os.RemoveAll(absolutePath)) })

	return repo, absolutePath
}

// testRepositoryPath returns the absolute path of local 'gitlab-org/gitlab-test.git' clone.
// It is cloned under the path by the test preparing step of make.
func testRepositoryPath(t testing.TB, repo string) string {
	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		require.Fail(t, "could not get caller info")
	}

	path := filepath.Join(filepath.Dir(currentFile), "..", "..", "..", "_build", "testrepos", repo)
	if !isValidRepoPath(path) {
		makePath := filepath.Join(filepath.Dir(currentFile), "..", "..", "..")
		makeTarget := "prepare-test-repos"
		log.Printf("local clone of test repository %q not found in %q, running `make %v`", repo, path, makeTarget)
		testhelper.MustRunCommand(t, nil, "make", "-C", makePath, makeTarget)
	}

	return path
}

// isValidRepoPath checks whether a valid git repository exists at the given path.
func isValidRepoPath(absolutePath string) bool {
	if _, err := os.Stat(filepath.Join(absolutePath, "objects")); err != nil {
		return false
	}

	return true
}

// AddWorktreeArgs returns git command arguments for adding a worktree at the
// specified repo
func AddWorktreeArgs(repoPath, worktreeName string) []string {
	return []string{"-C", repoPath, "worktree", "add", "--detach", worktreeName}
}

// AddWorktree creates a worktree in the repository path for tests
func AddWorktree(t testing.TB, cfg config.Cfg, repoPath string, worktreeName string) {
	Exec(t, cfg, AddWorktreeArgs(repoPath, worktreeName)...)
}
