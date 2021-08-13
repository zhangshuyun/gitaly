package testhelper

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/version"
)

var (
	buildGitalyGit2GoOnce    sync.Once
	buildGitalyLFSSmudgeOnce sync.Once
	buildGitalyHooksOnce     sync.Once
	buildGitalySSHOnce       sync.Once
)

// BuildGitalyGit2Go builds the gitaly-git2go command and installs it into the binary directory.
func BuildGitalyGit2Go(t testing.TB, cfg config.Cfg) {
	buildBinary(t, cfg.BinDir, "gitaly-git2go", &buildGitalyGit2GoOnce)
	// The link is needed because gitaly uses version-named binary.
	// Please check out https://gitlab.com/gitlab-org/gitaly/-/issues/3647 for more info.
	if err := os.Link(filepath.Join(cfg.BinDir, "gitaly-git2go"), filepath.Join(cfg.BinDir, "gitaly-git2go-"+version.GetModuleVersion())); err != nil {
		if errors.Is(err, os.ErrExist) {
			return
		}
		require.NoError(t, err)
	}
}

// BuildGitalyLFSSmudge builds the gitaly-lfs-smudge command and installs it into the binary
// directory.
func BuildGitalyLFSSmudge(t *testing.T, cfg config.Cfg) {
	buildBinary(t, cfg.BinDir, "gitaly-lfs-smudge", &buildGitalyLFSSmudgeOnce)
}

// BuildGitalyHooks builds the gitaly-hooks command and installs it into the binary directory.
func BuildGitalyHooks(t testing.TB, cfg config.Cfg) {
	buildBinary(t, cfg.BinDir, "gitaly-hooks", &buildGitalyHooksOnce)
}

// BuildGitalySSH builds the gitaly-ssh command and installs it into the binary directory.
func BuildGitalySSH(t testing.TB, cfg config.Cfg) {
	buildBinary(t, cfg.BinDir, "gitaly-ssh", &buildGitalySSHOnce)
}

func buildBinary(t testing.TB, dstDir, name string, buildOnce *sync.Once) {
	require.NotEmpty(t, testDirectory, "you must call testhelper.Configure() first")

	// binsPath is a shared between all tests location where all compiled binaries should be placed
	binsPath := filepath.Join(testDirectory, "bins")
	// binPath is a path to a specific binary file
	binPath := filepath.Join(binsPath, name)

	defer func() {
		if !t.Failed() {
			// copy compiled binary to the destination folder
			require.NoError(t, os.MkdirAll(dstDir, os.ModePerm))
			targetPath := filepath.Join(dstDir, name)
			CopyFile(t, binPath, targetPath)
			require.NoError(t, os.Chmod(targetPath, 0777))
		}
	}()

	buildOnce.Do(func() {
		require.NoError(t, os.MkdirAll(binsPath, os.ModePerm))
		require.NoFileExists(t, binPath, "binary has already been built")

		MustRunCommand(t, nil,
			"go",
			"build",
			"-tags", "static,system_libgit2",
			"-o", binPath,
			fmt.Sprintf("gitlab.com/gitlab-org/gitaly/v14/cmd/%s", name),
		)
	})

	require.FileExists(t, binPath, "%s does not exist", name)
}
