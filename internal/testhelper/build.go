package testhelper

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/version"
)

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

// ConfigureGitalySSHBin builds gitaly-ssh command for tests for the cfg.
func ConfigureGitalySSHBin(t testing.TB, cfg config.Cfg) {
	buildBinary(t, cfg.BinDir, "gitaly-ssh")
}

func buildBinary(t testing.TB, dstDir, name string) {
	// binsPath is a shared between all tests location where all compiled binaries should be placed
	binsPath := filepath.Join(testDirectory, "bins")
	// binPath is a path to a specific binary file
	binPath := filepath.Join(binsPath, name)
	// lockPath is a path to the special lock file used to prevent parallel build runs
	lockPath := binPath + ".lock"

	defer func() {
		if !t.Failed() {
			// copy compiled binary to the destination folder
			require.NoError(t, os.MkdirAll(dstDir, os.ModePerm))
			MustRunCommand(t, nil, "cp", binPath, dstDir)
		}
	}()

	require.NoError(t, os.MkdirAll(binsPath, os.ModePerm))

	lockFile, err := os.OpenFile(lockPath, os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		if !errors.Is(err, os.ErrExist) {
			require.FailNow(t, err.Error())
		}
		// another process is creating the binary at the moment, wait for it to complete (5s)
		for i := 0; i < 50; i++ {
			if _, err := os.Stat(binPath); err != nil {
				if !errors.Is(err, os.ErrNotExist) {
					require.NoError(t, err)
				}
				time.Sleep(100 * time.Millisecond)
				continue
			}
			// binary was created
			return
		}
		require.FailNow(t, "another process is creating binary for too long")
	}
	defer func() { require.NoError(t, os.Remove(lockPath)) }()
	require.NoError(t, lockFile.Close())

	if _, err := os.Stat(binPath); err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			// something went wrong and for some reason the binary already exists
			require.FailNow(t, err.Error())
		}
		buildCommand(t, binsPath, name)
	}
}

func buildCommand(t testing.TB, outputDir, cmd string) {
	if outputDir == "" {
		log.Fatal("BinDir must be set")
	}

	goBuildArgs := []string{
		"build",
		"-tags", "static,system_libgit2",
		"-o", filepath.Join(outputDir, cmd),
		fmt.Sprintf("gitlab.com/gitlab-org/gitaly/v14/cmd/%s", cmd),
	}
	MustRunCommand(t, nil, "go", goBuildArgs...)
}
