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

var buildOnceByName sync.Map

// BuildGitalyGit2Go builds the gitaly-git2go command and installs it into the binary directory.
func BuildGitalyGit2Go(t testing.TB, cfg config.Cfg) {
	buildBinary(t, cfg.BinDir, "gitaly-git2go")
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
	buildBinary(t, cfg.BinDir, "gitaly-lfs-smudge")
}

// BuildGitalyHooks builds the gitaly-hooks command and installs it into the binary directory.
func BuildGitalyHooks(t testing.TB, cfg config.Cfg) {
	buildBinary(t, cfg.BinDir, "gitaly-hooks")
}

// BuildGitalySSH builds the gitaly-ssh command and installs it into the binary directory.
func BuildGitalySSH(t testing.TB, cfg config.Cfg) {
	buildBinary(t, cfg.BinDir, "gitaly-ssh")
}

// BuildPraefect builds the praefect command and installs it into the binary directory.
func BuildPraefect(t testing.TB, cfg config.Cfg) {
	buildBinary(t, cfg.BinDir, "praefect")
}

func buildBinary(t testing.TB, dstDir, name string) {
	require.NotEmpty(t, testDirectory, "you must call testhelper.Configure() first")

	// binsPath is a shared between all tests location where all compiled binaries should be placed
	binsPath := filepath.Join(testDirectory, "bins")
	// binPath is a path to a specific binary file
	binPath := filepath.Join(binsPath, name)

	defer func() {
		if t.Failed() {
			return
		}

		targetPath := filepath.Join(dstDir, name)

		// Exit early if the file exists.
		if _, err := os.Stat(targetPath); err == nil {
			return
		}

		// copy compiled binary to the destination folder
		require.NoError(t, os.MkdirAll(dstDir, os.ModePerm))
		CopyFile(t, binPath, targetPath)
		require.NoError(t, os.Chmod(targetPath, 0o777))
	}()

	buildOnceInterface, _ := buildOnceByName.LoadOrStore(name, &sync.Once{})
	buildOnce, ok := buildOnceInterface.(*sync.Once)
	require.True(t, ok)

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
