package testhelper

import (
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v13/internal/git/hooks"
	"gitlab.com/gitlab-org/gitaly/v13/internal/gitaly/config"
)

// CaptureHookEnv creates a bogus 'update' Git hook to sniff out what
// environment variables get set for hooks.
func CaptureHookEnv(t testing.TB) (hookPath string, cleanup func()) {
	var err error
	oldOverride := hooks.Override
	hooks.Override, err = filepath.Abs("testdata/scratch/hooks")
	require.NoError(t, err)

	hookOutputFile, err := filepath.Abs("testdata/scratch/hook.env")
	require.NoError(t, err)

	require.NoError(t, os.RemoveAll(hookOutputFile))

	require.NoError(t, os.MkdirAll(hooks.Override, 0755))
	require.NoError(t, ioutil.WriteFile(filepath.Join(hooks.Override, "update"), []byte(`
#!/bin/sh
env | grep -e ^GIT -e ^GL_ > `+hookOutputFile+"\n"), 0755))

	return hookOutputFile, func() {
		hooks.Override = oldOverride
	}
}

// ConfigureGitalyHooksBinary builds gitaly-hooks command for tests
func ConfigureGitalyHooksBinary() {
	if config.Config.BinDir == "" {
		log.Fatal("config.Config.BinDir must be set")
	}

	goBuildArgs := []string{
		"build",
		"-o",
		filepath.Join(config.Config.BinDir, "gitaly-hooks"),
		"gitlab.com/gitlab-org/gitaly/v13/cmd/gitaly-hooks",
	}
	MustRunCommand(nil, nil, "go", goBuildArgs...)
}
