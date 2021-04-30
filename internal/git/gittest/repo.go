package gittest

import (
	"crypto/sha256"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

const (
	// GlRepository is the default repository name for newly created test
	// repos.
	GlRepository = "project-1"
	// GlProjectPath is the default project path for newly created test
	// repos.
	GlProjectPath = "gitlab-org/gitlab-test"

	testRepo = "gitlab-test.git"
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

// InitBareRepoAt creates a new bare repository in the storage
func InitBareRepoAt(t testing.TB, cfg config.Cfg, storage config.Storage) (*gitalypb.Repository, string, func()) {
	return initRepoAt(t, cfg, true, storage)
}

// InitRepoWithWorktreeAtStorage creates a new repository with a worktree in the storage
func InitRepoWithWorktreeAtStorage(t testing.TB, cfg config.Cfg, storage config.Storage) (*gitalypb.Repository, string, func()) {
	return initRepoAt(t, cfg, false, storage)
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

func initRepoAt(t testing.TB, cfg config.Cfg, bare bool, storage config.Storage) (*gitalypb.Repository, string, func()) {
	relativePath := NewRepositoryName(t, bare)
	repoPath := filepath.Join(storage.Path, relativePath)

	args := []string{"init"}
	if bare {
		args = append(args, "--bare")
	}

	Exec(t, cfg, append(args, repoPath)...)

	repo := InitRepoDir(t, storage.Path, relativePath)
	repo.StorageName = storage.Name
	if !bare {
		repo.RelativePath = filepath.Join(repo.RelativePath, ".git")
	}

	return repo, repoPath, func() { require.NoError(t, os.RemoveAll(repoPath)) }
}

// CloneRepoAtStorageRoot clones a new copy of test repository under a subdirectory in the storage root.
func CloneRepoAtStorageRoot(t testing.TB, cfg config.Cfg, storageRoot, relativePath string) *gitalypb.Repository {
	repo, _, _ := cloneRepo(t, cfg, storageRoot, relativePath, testRepo, true)
	return repo
}

// CloneRepoAtStorage clones a new copy of test repository under a subdirectory in the storage root.
func CloneRepoAtStorage(t testing.TB, storage config.Storage, relativePath string) (*gitalypb.Repository, string, testhelper.Cleanup) {
	repo, repoPath, cleanup := cloneRepo(t, config.Config, storage.Path, relativePath, testRepo, true)
	repo.StorageName = storage.Name
	return repo, repoPath, cleanup
}

// CloneRepo creates a bare copy of the test repository.
func CloneRepo(t testing.TB) (repo *gitalypb.Repository, repoPath string, cleanup func()) {
	return cloneRepo(t, config.Config, testhelper.GitlabTestStoragePath(), NewRepositoryName(t, true), testRepo, true)
}

// CloneRepoWithWorktree creates a copy of the test repository with a worktree. This is allows you
// to run normal 'non-bare' Git commands.
func CloneRepoWithWorktree(t testing.TB) (repo *gitalypb.Repository, repoPath string, cleanup func()) {
	return cloneRepo(t, config.Config, testhelper.GitlabTestStoragePath(), NewRepositoryName(t, false), testRepo, false)
}

// CloneRepoWithWorktreeAtStorage creates a copy of the test repository with a worktree at the storage you want.
// This is allows you to run normal 'non-bare' Git commands.
func CloneRepoWithWorktreeAtStorage(t testing.TB, cfg config.Cfg, storage config.Storage) (*gitalypb.Repository, string, testhelper.Cleanup) {
	repo, repoPath, cleanup := cloneRepo(t, cfg, storage.Path, NewRepositoryName(t, false), testRepo, false)
	repo.StorageName = storage.Name
	return repo, repoPath, cleanup
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

func cloneRepo(t testing.TB, cfg config.Cfg, storageRoot, relativePath, repoName string, bare bool) (repo *gitalypb.Repository, repoPath string, cleanup func()) {
	repoPath = filepath.Join(storageRoot, relativePath)

	repo = InitRepoDir(t, storageRoot, relativePath)
	args := []string{"clone", "--no-hardlinks", "--dissociate"}
	if bare {
		args = append(args, "--bare")
	} else {
		// For non-bare repos the relative path is the .git folder inside the path
		repo.RelativePath = filepath.Join(relativePath, ".git")
	}

	Exec(t, cfg, append(args, testRepositoryPath(t, repoName), repoPath)...)

	return repo, repoPath, func() { require.NoError(t, os.RemoveAll(repoPath)) }
}

// CloneBenchRepo creates a bare copy of the benchmarking test repository.
func CloneBenchRepo(t testing.TB, cfg config.Cfg) (repo *gitalypb.Repository, repoPath string, cleanup func()) {
	return cloneRepo(t, cfg, testhelper.GitlabTestStoragePath(), NewRepositoryName(t, true),
		"benchmark.git", true)
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
