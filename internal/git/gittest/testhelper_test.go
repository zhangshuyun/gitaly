package gittest

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

func TestMain(m *testing.M) {
	os.Exit(testMain(m))
}

func testMain(m *testing.M) int {
	defer testhelper.MustHaveNoChildProcess()
	cleanup := testhelper.Configure()
	defer cleanup()
	return m.Run()
}

// setup sets up a test configuration and repository. Ideally we'd use our central test helpers to
// do this, but because of an import cycle we can't.
func setup(t testing.TB) (config.Cfg, *gitalypb.Repository, string) {
	t.Helper()

	rootDir := testhelper.TempDir(t)

	var cfg config.Cfg

	cfg.SocketPath = "it is a stub to bypass Validate method"

	cfg.Storages = []config.Storage{
		{
			Name: "default",
			Path: filepath.Join(rootDir, "storage.d"),
		},
	}
	require.NoError(t, os.Mkdir(cfg.Storages[0].Path, 0755))

	cfg.GitlabShell.Dir = filepath.Join(rootDir, "shell.d")
	require.NoError(t, os.Mkdir(cfg.GitlabShell.Dir, 0755))

	cfg.BinDir = filepath.Join(rootDir, "bin.d")
	require.NoError(t, os.Mkdir(cfg.BinDir, 0755))

	require.NoError(t, testhelper.ConfigureRuby(&cfg))
	require.NoError(t, cfg.Validate())

	repo, repoPath, cleanup := CloneRepoAtStorage(t, cfg.Storages[0], t.Name())
	t.Cleanup(cleanup)

	return cfg, repo, repoPath
}
