package testcfg

import (
	"io/ioutil"
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

// WithRealLinguist suppress stubbing of the linguist language detection.
func WithRealLinguist() Option {
	return func(builder *GitalyCfgBuilder) {
		builder.realLinguist = true
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
	cfg config.Cfg

	storages     []string
	realLinguist bool
}

// Build setups required filesystem structure, creates and returns configuration of the gitaly service.
func (gc *GitalyCfgBuilder) Build(t testing.TB) config.Cfg {
	t.Helper()

	cfg := gc.cfg
	if cfg.SocketPath == "" {
		cfg.SocketPath = "it is a stub to bypass Validate method"
	}

	root := testhelper.TempDir(t)

	if cfg.BinDir == "" {
		cfg.BinDir = filepath.Join(root, "bin.d")
		require.NoError(t, os.Mkdir(cfg.BinDir, 0755))
	}

	if cfg.Logging.Dir == "" {
		cfg.Logging.Dir = filepath.Join(root, "log.d")
		require.NoError(t, os.Mkdir(cfg.Logging.Dir, 0755))
	}

	if cfg.GitlabShell.Dir == "" {
		cfg.GitlabShell.Dir = filepath.Join(root, "shell.d")
		require.NoError(t, os.Mkdir(cfg.GitlabShell.Dir, 0755))
	}

	if cfg.InternalSocketDir == "" {
		cfg.InternalSocketDir = filepath.Join(root, "internal_socks.d")
		require.NoError(t, os.Mkdir(cfg.InternalSocketDir, 0755))
	}

	if len(cfg.Storages) != 0 && len(gc.storages) != 0 {
		require.FailNow(t, "invalid configuration build setup: fix storages configured")
	}

	if len(cfg.Storages) == 0 {
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
	}

	if !gc.realLinguist {
		if cfg.Ruby.LinguistLanguagesPath == "" {
			// set a stub to prevent a long ruby process to run where it is not needed
			cfg.Ruby.LinguistLanguagesPath = filepath.Join(root, "linguist_languages.json")
			require.NoError(t, ioutil.WriteFile(cfg.Ruby.LinguistLanguagesPath, []byte(`{}`), 0655))
		}
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
		repos[i] = gittest.CloneRepoAtStorageRoot(t, cfg, gitalyStorage.Path, relativePath)
		repos[i].StorageName = gitalyStorage.Name
	}

	return cfg, repos
}

// Build creates a minimal configuration setup with no options and returns it with cleanup function.
func Build(t testing.TB, opts ...Option) config.Cfg {
	cfgBuilder := NewGitalyCfgBuilder(opts...)

	return cfgBuilder.Build(t)
}

// BuildWithRepo creates a minimal configuration setup with no options.
// It also clones test repository at the storage and returns it with the full path to the repository.
func BuildWithRepo(t testing.TB, opts ...Option) (config.Cfg, *gitalypb.Repository, string) {
	cfgBuilder := NewGitalyCfgBuilder(opts...)

	cfg, repos := cfgBuilder.BuildWithRepoAt(t, t.Name())
	repoPath := filepath.Join(cfg.Storages[0].Path, repos[0].RelativePath)
	return cfg, repos[0], repoPath
}
