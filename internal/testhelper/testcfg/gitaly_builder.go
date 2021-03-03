package testcfg

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

// Option is a configuration option for the builder.
type Option func(*GitalyCfgBuilder)

// WithBase allows use cfg as a template for start building on top of.
// override parameter signals if settings of the cfg can be overridden or not
// (if setting has a default value it is considered "not configured" and can be
// set despite flag value).
func WithBase(cfg config.Cfg) Option {
	return func(builder *GitalyCfgBuilder) {
		builder.cfg = cfg
	}
}

// WithStorages allows to configure list of storages under this gitaly instance.
// All storages will have a test repository by default.
func WithStorages(name string, names ...string) Option {
	return func(builder *GitalyCfgBuilder) {
		builder.storages = append([]string{name}, names...)
	}
}

// NewGitalyCfgBuilder returns gitaly configuration builder with configured set of options.
func NewGitalyCfgBuilder(opts ...Option) GitalyCfgBuilder {
	cfgBuilder := GitalyCfgBuilder{}

	for _, opt := range opts {
		opt(&cfgBuilder)
	}

	return cfgBuilder
}

// GitalyCfgBuilder automates creation of the gitaly configuration and filesystem structure required.
type GitalyCfgBuilder struct {
	cfg      config.Cfg
	cleanups []testhelper.Cleanup

	storages []string
}

func (gc *GitalyCfgBuilder) addCleanup(f testhelper.Cleanup) {
	gc.cleanups = append(gc.cleanups, f)
}

// Cleanup releases all resources allocated fot the created config.
// It should be called once the test is done.
func (gc *GitalyCfgBuilder) Cleanup() {
	for i := len(gc.cleanups) - 1; i >= 0; i-- {
		gc.cleanups[i]()
	}
}

func (gc *GitalyCfgBuilder) tempDir(t testing.TB) string {
	tempDir, cleanupTempDir := testhelper.TempDir(t)
	gc.addCleanup(cleanupTempDir)
	return tempDir
}

// Build setups required filesystem structure, creates and returns configuration of the gitaly service.
func (gc *GitalyCfgBuilder) Build(t testing.TB) config.Cfg {
	t.Helper()

	cfg := gc.cfg
	cfg.SocketPath = "it is a stub to bypass Validate method"

	root := gc.tempDir(t)

	cfg.BinDir = filepath.Join(root, "bin.d")
	require.NoError(t, os.Mkdir(cfg.BinDir, 0755))

	cfg.Logging.Dir = filepath.Join(root, "log.d")
	require.NoError(t, os.Mkdir(cfg.Logging.Dir, 0755))

	cfg.GitlabShell.Dir = filepath.Join(root, "shell.d")
	require.NoError(t, os.Mkdir(cfg.GitlabShell.Dir, 0755))

	cfg.InternalSocketDir = filepath.Join(root, "internal_socks.d")
	require.NoError(t, os.Mkdir(cfg.InternalSocketDir, 0755))

	storagesDir := filepath.Join(root, "storages.d")
	require.NoError(t, os.Mkdir(storagesDir, 0755))

	if len(gc.storages) == 0 {
		gc.storages = []string{"default"}
	}

	// creation of the required storages (empty storage directories)
	cfg.Storages = make([]config.Storage, len(gc.storages))
	for i, storageName := range gc.storages {
		storagePath := filepath.Join(storagesDir, storageName)
		require.NoError(t, os.MkdirAll(storagePath, 0755))
		cfg.Storages[i].Name = storageName
		cfg.Storages[i].Path = storagePath
	}

	require.NoError(t, testhelper.ConfigureRuby(&cfg))
	require.NoError(t, cfg.Validate())

	return cfg
}

// BuildWithRepoAt setups required filesystem structure, creates and returns configuration of the gitaly service,
// clones test repository into each configured storage the provided relative path.
func (gc *GitalyCfgBuilder) BuildWithRepoAt(t testing.TB, relativePath string) (config.Cfg, []*gitalypb.Repository) {
	t.Helper()

	cfg := gc.Build(t)

	// clone the test repo to the each storage
	repos := make([]*gitalypb.Repository, len(cfg.Storages))
	for i, gitalyStorage := range cfg.Storages {
		repos[i], _, _ = gittest.CloneRepoAtStorage(t, gitalyStorage, relativePath)
		repos[i].StorageName = gitalyStorage.Name
	}

	return cfg, repos
}
