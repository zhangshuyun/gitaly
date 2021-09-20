package config

import (
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config/auth"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config/cgroups"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config/prometheus"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config/sentry"
)

func TestLoadBrokenConfig(t *testing.T) {
	tmpFile := strings.NewReader(`path = "/tmp"\nname="foo"`)
	_, err := Load(tmpFile)
	assert.Error(t, err)
}

func TestLoadEmptyConfig(t *testing.T) {
	cfg, err := Load(strings.NewReader(``))
	require.NoError(t, err)

	defaultConf := Cfg{
		Prometheus:        prometheus.DefaultConfig(),
		InternalSocketDir: cfg.InternalSocketDir,
	}
	require.NoError(t, defaultConf.setDefaults())

	assert.Equal(t, defaultConf, cfg)
}

func TestLoadURLs(t *testing.T) {
	tmpFile := strings.NewReader(`
[gitlab]
url = "unix:///tmp/test.socket"
relative_url_root = "/gitlab"`)

	cfg, err := Load(tmpFile)
	require.NoError(t, err)

	defaultConf := Cfg{
		Gitlab: Gitlab{
			URL:             "unix:///tmp/test.socket",
			RelativeURLRoot: "/gitlab",
		},
	}
	require.NoError(t, defaultConf.setDefaults())

	assert.Equal(t, defaultConf.Gitlab, cfg.Gitlab)
}

func TestLoadStorage(t *testing.T) {
	tmpFile := strings.NewReader(`[[storage]]
name = "default"
path = "/tmp/"`)

	cfg, err := Load(tmpFile)
	require.NoError(t, err)

	if assert.Equal(t, 1, len(cfg.Storages), "Expected one (1) storage") {
		expectedConf := Cfg{
			Storages: []Storage{
				{Name: "default", Path: "/tmp"},
			},
		}
		require.NoError(t, expectedConf.setDefaults())

		assert.Equal(t, expectedConf.Storages, cfg.Storages)
	}
}

func TestUncleanStoragePaths(t *testing.T) {
	cfg, err := Load(strings.NewReader(`[[storage]]
name="unclean-path-1"
path="/tmp/repos1//"

[[storage]]
name="unclean-path-2"
path="/tmp/repos2/subfolder/.."
`))
	require.NoError(t, err)

	require.Equal(t, []Storage{
		{Name: "unclean-path-1", Path: "/tmp/repos1"},
		{Name: "unclean-path-2", Path: "/tmp/repos2"},
	}, cfg.Storages)
}

func TestLoadMultiStorage(t *testing.T) {
	tmpFile := strings.NewReader(`[[storage]]
name="default"
path="/tmp/repos1"

[[storage]]
name="other"
path="/tmp/repos2/"`)

	cfg, err := Load(tmpFile)
	require.NoError(t, err)

	if assert.Equal(t, 2, len(cfg.Storages), "Expected one (1) storage") {
		expectedConf := Cfg{
			Storages: []Storage{
				{Name: "default", Path: "/tmp/repos1"},
				{Name: "other", Path: "/tmp/repos2"},
			},
		}
		require.NoError(t, expectedConf.setDefaults())

		assert.Equal(t, expectedConf.Storages, cfg.Storages)
	}
}

func TestLoadSentry(t *testing.T) {
	tmpFile := strings.NewReader(`[logging]
sentry_environment = "production"
sentry_dsn = "abc123"
ruby_sentry_dsn = "xyz456"`)

	cfg, err := Load(tmpFile)
	require.NoError(t, err)

	expectedConf := Cfg{
		Logging: Logging{
			Sentry: Sentry(sentry.Config{
				Environment: "production",
				DSN:         "abc123",
			}),
			RubySentryDSN: "xyz456",
		},
	}
	require.NoError(t, expectedConf.setDefaults())

	assert.Equal(t, expectedConf.Logging, cfg.Logging)
}

func TestLoadPrometheus(t *testing.T) {
	tmpFile := strings.NewReader(`
		prometheus_listen_addr=":9236"
		[prometheus]
		scrape_timeout       = "1s"
		grpc_latency_buckets = [0.0, 1.0, 2.0]
	`)

	cfg, err := Load(tmpFile)
	require.NoError(t, err)

	assert.Equal(t, ":9236", cfg.PrometheusListenAddr)
	assert.Equal(t, prometheus.Config{
		ScrapeTimeout:      time.Second,
		GRPCLatencyBuckets: []float64{0, 1, 2},
	}, cfg.Prometheus)
}

func TestLoadSocketPath(t *testing.T) {
	tmpFile := strings.NewReader(`socket_path="/tmp/gitaly.sock"`)

	cfg, err := Load(tmpFile)
	require.NoError(t, err)

	assert.Equal(t, "/tmp/gitaly.sock", cfg.SocketPath)
}

func TestLoadListenAddr(t *testing.T) {
	tmpFile := strings.NewReader(`listen_addr=":8080"`)

	cfg, err := Load(tmpFile)
	require.NoError(t, err)

	assert.Equal(t, ":8080", cfg.ListenAddr)
}

func TestValidateStorages(t *testing.T) {
	repositories := tempDir(t)
	repositories2 := tempDir(t)
	nestedRepositories := filepath.Join(repositories, "nested")
	require.NoError(t, os.MkdirAll(nestedRepositories, os.ModePerm))
	f, err := os.CreateTemp("", "")
	require.NoError(t, err)
	require.NoError(t, f.Close())
	filePath := f.Name()

	invalidDir := filepath.Join(repositories, t.Name())

	testCases := []struct {
		desc      string
		storages  []Storage
		expErrMsg string
	}{
		{
			desc:      "no storages",
			expErrMsg: "no storage configurations found. Are you using the right format? https://gitlab.com/gitlab-org/gitaly/issues/397",
		},
		{
			desc: "just 1 storage",
			storages: []Storage{
				{Name: "default", Path: repositories},
			},
		},
		{
			desc: "multiple storages",
			storages: []Storage{
				{Name: "default", Path: repositories},
				{Name: "other", Path: repositories2},
			},
		},
		{
			desc: "multiple storages pointing to same directory",
			storages: []Storage{
				{Name: "default", Path: repositories},
				{Name: "other", Path: repositories},
				{Name: "third", Path: repositories},
			},
		},
		{
			desc: "nested paths 1",
			storages: []Storage{
				{Name: "default", Path: repositories},
				{Name: "other", Path: repositories},
				{Name: "third", Path: nestedRepositories},
			},
			expErrMsg: `storage paths may not nest: "third" and "default"`,
		},
		{
			desc: "nested paths 2",
			storages: []Storage{
				{Name: "default", Path: nestedRepositories},
				{Name: "other", Path: repositories},
				{Name: "third", Path: repositories},
			},
			expErrMsg: `storage paths may not nest: "other" and "default"`,
		},
		{
			desc: "duplicate definition",
			storages: []Storage{
				{Name: "default", Path: repositories},
				{Name: "default", Path: repositories},
			},
			expErrMsg: `storage "default" is defined more than once`,
		},
		{
			desc: "re-definition",
			storages: []Storage{
				{Name: "default", Path: repositories},
				{Name: "default", Path: repositories2},
			},
			expErrMsg: `storage "default" is defined more than once`,
		},
		{
			desc: "empty name",
			storages: []Storage{
				{Name: "some", Path: repositories},
				{Name: "", Path: repositories},
			},
			expErrMsg: `empty storage name at declaration 2`,
		},
		{
			desc: "empty path",
			storages: []Storage{
				{Name: "some", Path: repositories},
				{Name: "default", Path: ""},
			},
			expErrMsg: `empty storage path for storage "default"`,
		},
		{
			desc: "non existing directory",
			storages: []Storage{
				{Name: "default", Path: repositories},
				{Name: "nope", Path: invalidDir},
			},
			expErrMsg: fmt.Sprintf(`storage path %q for storage "nope" doesn't exist`, invalidDir),
		},
		{
			desc: "path points to the regular file",
			storages: []Storage{
				{Name: "default", Path: repositories},
				{Name: "is_file", Path: filePath},
			},
			expErrMsg: fmt.Sprintf(`storage path %q for storage "is_file" is not a dir`, filePath),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			cfg := Cfg{Storages: tc.storages}

			err := cfg.validateStorages()
			if tc.expErrMsg != "" {
				assert.EqualError(t, err, tc.expErrMsg, "%+v", tc.storages)
				return
			}

			assert.NoError(t, err, "%+v", tc.storages)
		})
	}
}

func TestStoragePath(t *testing.T) {
	cfg := Cfg{Storages: []Storage{
		{Name: "default", Path: "/home/git/repositories1"},
		{Name: "other", Path: "/home/git/repositories2"},
		{Name: "third", Path: "/home/git/repositories3"},
	}}

	testCases := []struct {
		in, out string
		ok      bool
	}{
		{in: "default", out: "/home/git/repositories1", ok: true},
		{in: "third", out: "/home/git/repositories3", ok: true},
		{in: "", ok: false},
		{in: "foobar", ok: false},
	}

	for _, tc := range testCases {
		out, ok := cfg.StoragePath(tc.in)
		if !assert.Equal(t, tc.ok, ok, "%+v", tc) {
			continue
		}
		assert.Equal(t, tc.out, out, "%+v", tc)
	}
}

type hookFileMode int

const (
	hookFileExists hookFileMode = 1 << (4 - 1 - iota)
	hookFileExecutable
)

func setupTempHookDirs(t *testing.T, m map[string]hookFileMode) (string, func()) {
	tempDir, err := os.MkdirTemp("", "hooks")
	require.NoError(t, err)

	for hookName, mode := range m {
		if mode&hookFileExists > 0 {
			path := filepath.Join(tempDir, hookName)
			require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))

			require.NoError(t, ioutil.WriteFile(filepath.Join(tempDir, hookName), nil, 0o644))

			if mode&hookFileExecutable > 0 {
				require.NoError(t, os.Chmod(filepath.Join(tempDir, hookName), 0o755))
			}
		}
	}

	return tempDir, func() { require.NoError(t, os.RemoveAll(tempDir)) }
}

var (
	fileNotExistsErrRegexSnippit  = "no such file or directory"
	fileNotExecutableRegexSnippit = "not executable: .*"
)

func TestValidateHooks(t *testing.T) {
	testCases := []struct {
		desc             string
		expectedErrRegex string
		hookFiles        map[string]hookFileMode
	}{
		{
			desc: "everything is ✅",
			hookFiles: map[string]hookFileMode{
				"ruby/git-hooks/update":       hookFileExists | hookFileExecutable,
				"ruby/git-hooks/pre-receive":  hookFileExists | hookFileExecutable,
				"ruby/git-hooks/post-receive": hookFileExists | hookFileExecutable,
			},
			expectedErrRegex: "",
		},
		{
			desc: "missing git-hooks",
			hookFiles: map[string]hookFileMode{
				"ruby/git-hooks/update":       0,
				"ruby/git-hooks/pre-receive":  0,
				"ruby/git-hooks/post-receive": 0,
			},
			expectedErrRegex: fmt.Sprintf("%s, %s, %s", fileNotExistsErrRegexSnippit, fileNotExistsErrRegexSnippit, fileNotExistsErrRegexSnippit),
		},
		{
			desc: "git-hooks are not executable",
			hookFiles: map[string]hookFileMode{
				"ruby/git-hooks/update":       hookFileExists,
				"ruby/git-hooks/pre-receive":  hookFileExists,
				"ruby/git-hooks/post-receive": hookFileExists,
			},
			expectedErrRegex: fmt.Sprintf("%s, %s, %s", fileNotExecutableRegexSnippit, fileNotExecutableRegexSnippit, fileNotExecutableRegexSnippit),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			tempHookDir, cleanup := setupTempHookDirs(t, tc.hookFiles)
			defer cleanup()

			cfg := Cfg{
				Ruby: Ruby{
					Dir: filepath.Join(tempHookDir, "ruby"),
				},
				GitlabShell: GitlabShell{
					Dir: filepath.Join(tempHookDir, "/gitlab-shell"),
				},
				BinDir: filepath.Join(tempHookDir, "/bin"),
			}

			err := cfg.validateHooks()
			if tc.expectedErrRegex != "" {
				require.Error(t, err)
				require.Regexp(t, tc.expectedErrRegex, err.Error(), "error should match regexp")
			}
		})
	}
}

func TestLoadGit(t *testing.T) {
	tmpFile := strings.NewReader(`[git]
bin_path = "/my/git/path"
catfile_cache_size = 50

[[git.config]]
key = "first.key"
value = "first-value"

[[git.config]]
key = "second.key"
value = "second-value"
`)

	cfg, err := Load(tmpFile)
	require.NoError(t, err)

	require.Equal(t, Git{
		BinPath:          "/my/git/path",
		CatfileCacheSize: 50,
		Config: []GitConfig{
			{Key: "first.key", Value: "first-value"},
			{Key: "second.key", Value: "second-value"},
		},
	}, cfg.Git)
}

func TestSetGitPath(t *testing.T) {
	// Clean up env so each test case can set it individually
	val, set := os.LookupEnv("GITALY_TESTING_GIT_BINARY")
	require.NoError(t, os.Unsetenv("GITALY_TESTING_GIT_BINARY"))
	if set {
		defer func() { require.NoError(t, os.Setenv("GITALY_TESTING_GIT_BINARY", val)) }()
	} else {
		defer func() { require.NoError(t, os.Unsetenv("GITALY_TESTING_GIT_BINARY")) }()
	}

	t.Run("set in config", func(t *testing.T) {
		cfg := Cfg{Git: Git{BinPath: "/path/to/myGit"}}
		require.NoError(t, cfg.SetGitPath())
		assert.Equal(t, "/path/to/myGit", cfg.Git.BinPath)
	})

	t.Run("set using env var", func(t *testing.T) {
		require.NoError(t, os.Setenv("GITALY_TESTING_GIT_BINARY", "/path/to/env_git"))
		defer func() { require.NoError(t, os.Unsetenv("GITALY_TESTING_GIT_BINARY")) }()
		cfg := Cfg{Git: Git{}}
		require.NoError(t, cfg.SetGitPath())
		assert.Equal(t, "/path/to/env_git", cfg.Git.BinPath)
	})

	t.Run("not set, get from system", func(t *testing.T) {
		resolvedPath, err := exec.LookPath("git")
		require.NoError(t, err)
		cfg := Cfg{Git: Git{}}
		require.NoError(t, cfg.SetGitPath())
		assert.Equal(t, resolvedPath, cfg.Git.BinPath)
	})

	t.Run("doesn't exist in the system", func(t *testing.T) {
		val, set := os.LookupEnv("PATH")
		require.NoError(t, os.Unsetenv("PATH"))
		if set {
			defer func() { require.NoError(t, os.Setenv("PATH", val)) }()
		}
		cfg := Cfg{Git: Git{}}
		assert.EqualError(t, cfg.SetGitPath(), `"git" executable not found, set path to it in the configuration file or add it to the PATH`)
	})
}

func TestValidateGitConfig(t *testing.T) {
	testCases := []struct {
		desc        string
		configPairs []GitConfig
		expectedErr error
	}{
		{
			desc: "empty config is valid",
		},
		{
			desc: "valid config entry",
			configPairs: []GitConfig{
				{Key: "foo.bar", Value: "value"},
			},
		},
		{
			desc: "missing key",
			configPairs: []GitConfig{
				{Value: "value"},
			},
			expectedErr: fmt.Errorf("invalid configuration key \"\": %w", errors.New("key cannot be empty")),
		},
		{
			desc: "key has no section",
			configPairs: []GitConfig{
				{Key: "foo", Value: "value"},
			},
			expectedErr: fmt.Errorf("invalid configuration key \"foo\": %w", errors.New("key must contain at least one section")),
		},
		{
			desc: "key with leading dot",
			configPairs: []GitConfig{
				{Key: ".foo.bar", Value: "value"},
			},
			expectedErr: fmt.Errorf("invalid configuration key \".foo.bar\": %w", errors.New("key must not start or end with a dot")),
		},
		{
			desc: "key with trailing dot",
			configPairs: []GitConfig{
				{Key: "foo.bar.", Value: "value"},
			},
			expectedErr: fmt.Errorf("invalid configuration key \"foo.bar.\": %w", errors.New("key must not start or end with a dot")),
		},
		{
			desc: "key has assignment",
			configPairs: []GitConfig{
				{Key: "foo.bar=value", Value: "value"},
			},
			expectedErr: fmt.Errorf("invalid configuration key \"foo.bar=value\": %w",
				errors.New("key cannot contain assignment")),
		},
		{
			desc: "missing value",
			configPairs: []GitConfig{
				{Key: "foo.bar"},
			},
			expectedErr: fmt.Errorf("invalid configuration value: \"\""),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			cfg := Cfg{Git: Git{Config: tc.configPairs}}
			require.Equal(t, tc.expectedErr, cfg.validateGit())
		})
	}
}

func TestValidateShellPath(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "gitaly-tests-")
	require.NoError(t, err)
	require.NoError(t, os.MkdirAll(filepath.Join(tmpDir, "bin"), 0o755))
	tmpFile := filepath.Join(tmpDir, "my-file")
	defer func() { require.NoError(t, os.RemoveAll(tmpDir)) }()
	fp, err := os.Create(tmpFile)
	require.NoError(t, err)
	require.NoError(t, fp.Close())

	testCases := []struct {
		desc      string
		path      string
		expErrMsg string
	}{
		{
			desc:      "When no Shell Path set",
			path:      "",
			expErrMsg: "gitlab-shell.dir: is not set",
		},
		{
			desc:      "When Shell Path set to non-existing path",
			path:      "/non/existing/path",
			expErrMsg: `gitlab-shell.dir: path doesn't exist: "/non/existing/path"`,
		},
		{
			desc:      "When Shell Path set to non-dir path",
			path:      tmpFile,
			expErrMsg: fmt.Sprintf(`gitlab-shell.dir: not a directory: %q`, tmpFile),
		},
		{
			desc:      "When Shell Path set to a valid directory",
			path:      tmpDir,
			expErrMsg: "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			cfg := Cfg{GitlabShell: GitlabShell{Dir: tc.path}}
			err := cfg.validateShell()
			if tc.expErrMsg != "" {
				assert.EqualError(t, err, tc.expErrMsg)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestConfigureRuby(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "gitaly-test")
	require.NoError(t, err)
	defer func() { require.NoError(t, os.RemoveAll(tmpDir)) }()

	tmpFile := filepath.Join(tmpDir, "file")
	require.NoError(t, ioutil.WriteFile(tmpFile, nil, 0o644))

	testCases := []struct {
		desc      string
		dir       string
		expErrMsg string
	}{
		{
			desc: "relative path",
			dir:  ".",
		},
		{
			desc: "ok",
			dir:  tmpDir,
		},
		{
			desc:      "empty",
			dir:       "",
			expErrMsg: "gitaly-ruby.dir: is not set",
		},
		{
			desc:      "does not exist",
			dir:       "/does/not/exist",
			expErrMsg: `gitaly-ruby.dir: path doesn't exist: "/does/not/exist"`,
		},
		{
			desc:      "exists but is not a directory",
			dir:       tmpFile,
			expErrMsg: fmt.Sprintf(`gitaly-ruby.dir: not a directory: %q`, tmpFile),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			cfg := Cfg{Ruby: Ruby{Dir: tc.dir}}

			err := cfg.ConfigureRuby()
			if tc.expErrMsg != "" {
				require.EqualError(t, err, tc.expErrMsg)
				return
			}

			require.NoError(t, err)

			dir := cfg.Ruby.Dir
			require.True(t, filepath.IsAbs(dir), "expected %q to be absolute path", dir)
		})
	}
}

func TestConfigureRubyNumWorkers(t *testing.T) {
	testCases := []struct {
		in, out int
	}{
		{in: -1, out: 2},
		{in: 0, out: 2},
		{in: 1, out: 2},
		{in: 2, out: 2},
		{in: 3, out: 3},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%+v", tc), func(t *testing.T) {
			cfg := Cfg{Ruby: Ruby{Dir: "/", NumWorkers: tc.in}}
			require.NoError(t, cfg.ConfigureRuby())
			require.Equal(t, tc.out, cfg.Ruby.NumWorkers)
		})
	}
}

func TestValidateListeners(t *testing.T) {
	testCases := []struct {
		desc string
		Cfg
		expErrMsg string
	}{
		{desc: "empty", expErrMsg: `at least one of socket_path, listen_addr or tls_listen_addr must be set`},
		{desc: "socket only", Cfg: Cfg{SocketPath: "/foo/bar"}},
		{desc: "tcp only", Cfg: Cfg{ListenAddr: "a.b.c.d:1234"}},
		{desc: "tls only", Cfg: Cfg{TLSListenAddr: "a.b.c.d:1234"}},
		{desc: "both socket and tcp", Cfg: Cfg{SocketPath: "/foo/bar", ListenAddr: "a.b.c.d:1234"}},
		{desc: "all addresses", Cfg: Cfg{SocketPath: "/foo/bar", ListenAddr: "a.b.c.d:1234", TLSListenAddr: "a.b.c.d:1234"}},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			err := tc.Cfg.validateListeners()
			if tc.expErrMsg != "" {
				require.EqualError(t, err, tc.expErrMsg)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestLoadGracefulRestartTimeout(t *testing.T) {
	tests := []struct {
		name     string
		config   string
		expected time.Duration
	}{
		{
			name:     "default value",
			expected: 1 * time.Minute,
		},
		{
			name:     "8m03s",
			config:   `graceful_restart_timeout = "8m03s"`,
			expected: 8*time.Minute + 3*time.Second,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tmpFile := strings.NewReader(test.config)

			cfg, err := Load(tmpFile)
			assert.NoError(t, err)

			assert.Equal(t, test.expected, cfg.GracefulRestartTimeout.Duration())
		})
	}
}

func TestGitlabShellDefaults(t *testing.T) {
	gitlabShellDir := "/dir"
	expectedGitlab := Gitlab{
		SecretFile: filepath.Join(gitlabShellDir, ".gitlab_shell_secret"),
	}

	expectedHooks := Hooks{
		CustomHooksDir: filepath.Join(gitlabShellDir, "hooks"),
	}

	tmpFile := strings.NewReader(fmt.Sprintf(`[gitlab-shell]
dir = '%s'`, gitlabShellDir))
	cfg, err := Load(tmpFile)
	require.NoError(t, err)

	require.Equal(t, expectedGitlab, cfg.Gitlab)
	require.Equal(t, expectedHooks, cfg.Hooks)
}

func TestValidateInternalSocketDir(t *testing.T) {
	// create a valid socket directory
	tmpDir := tempDir(t)
	// create a symlinked socket directory
	dirName := "internal_socket_dir"
	validSocketDirSymlink := filepath.Join(tmpDir, dirName)
	tmpSocketDir, err := os.MkdirTemp(tmpDir, "")
	require.NoError(t, err)
	tmpSocketDir, err = filepath.Abs(tmpSocketDir)
	require.NoError(t, err)
	require.NoError(t, os.Symlink(tmpSocketDir, validSocketDirSymlink))

	// create a broken symlink
	dirName = "internal_socket_dir_broken"
	brokenSocketDirSymlink := filepath.Join(tmpDir, dirName)
	require.NoError(t, os.Symlink("/does/not/exist", brokenSocketDirSymlink))

	pathTooLongForSocket := filepath.Join(tmpDir, strings.Repeat("/nested_directory", 10))
	require.NoError(t, os.MkdirAll(pathTooLongForSocket, os.ModePerm))

	testCases := []struct {
		desc              string
		internalSocketDir string
		expErrMsgRegexp   string
	}{
		{
			desc:              "empty socket dir",
			internalSocketDir: "",
		},
		{
			desc:              "non existing directory",
			internalSocketDir: "/tmp/relative/path/to/nowhere",
			expErrMsgRegexp:   `internal_socket_dir: path doesn't exist: "/tmp/relative/path/to/nowhere"`,
		},
		{
			desc:              "valid socket directory",
			internalSocketDir: tmpDir,
		},
		{
			desc:              "valid symlinked directory",
			internalSocketDir: validSocketDirSymlink,
		},
		{
			desc:              "broken symlinked directory",
			internalSocketDir: brokenSocketDirSymlink,
			expErrMsgRegexp:   fmt.Sprintf(`internal_socket_dir: path doesn't exist: %q`, brokenSocketDirSymlink),
		},
		{
			desc:              "socket can't be created",
			internalSocketDir: pathTooLongForSocket,
			expErrMsgRegexp:   `internal_socket_dir: try create socket: socket could not be created in .*\/test-.{8}\.sock: bind: invalid argument`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			err := (&Cfg{InternalSocketDir: tc.internalSocketDir}).validateInternalSocketDir()
			if tc.expErrMsgRegexp != "" {
				assert.Regexp(t, tc.expErrMsgRegexp, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestInternalSocketDir(t *testing.T) {
	cfg, err := Load(bytes.NewReader(nil))
	require.NoError(t, err)
	socketDir := cfg.InternalSocketDir

	require.NoError(t, trySocketCreation(socketDir))
	require.NoError(t, os.RemoveAll(socketDir))
}

func TestLoadDailyMaintenance(t *testing.T) {
	for _, tt := range []struct {
		name        string
		rawCfg      string
		expect      DailyJob
		loadErr     error
		validateErr error
	}{
		{
			name: "success",
			rawCfg: `[[storage]]
			name = "default"
			path = "/"

			[daily_maintenance]
			start_hour = 11
			start_minute = 23
			duration = "45m"
			storages = ["default"]
			`,
			expect: DailyJob{
				Hour:     11,
				Minute:   23,
				Duration: Duration(45 * time.Minute),
				Storages: []string{"default"},
			},
		},
		{
			rawCfg: `[daily_maintenance]
			start_hour = 24`,
			expect: DailyJob{
				Hour: 24,
			},
			validateErr: errors.New("daily maintenance specified hour '24' outside range (0-23)"),
		},
		{
			rawCfg: `[daily_maintenance]
			start_hour = 60`,
			expect: DailyJob{
				Hour: 60,
			},
			validateErr: errors.New("daily maintenance specified hour '60' outside range (0-23)"),
		},
		{
			rawCfg: `[daily_maintenance]
			start_hour = 0
			start_minute = 61`,
			expect: DailyJob{
				Hour:   0,
				Minute: 61,
			},
			validateErr: errors.New("daily maintenance specified minute '61' outside range (0-59)"),
		},
		{
			rawCfg: `[daily_maintenance]
			start_hour = 0
			start_minute = 59
			duration = "86401s"`,
			expect: DailyJob{
				Hour:     0,
				Minute:   59,
				Duration: Duration(24*time.Hour + time.Second),
			},
			validateErr: errors.New("daily maintenance specified duration 24h0m1s must be less than 24 hours"),
		},
		{
			rawCfg: `[daily_maintenance]
			duration = "meow"`,
			expect:  DailyJob{},
			loadErr: errors.New("load toml: (2, 4): unmarshal text: time: invalid duration"),
		},
		{
			rawCfg: `[daily_maintenance]
			storages = ["default"]`,
			expect: DailyJob{
				Storages: []string{"default"},
			},
			validateErr: errors.New(`daily maintenance specified storage "default" does not exist in configuration`),
		},
		{
			name: "default window",
			rawCfg: `[[storage]]
			name = "default"
			path = "/"
			`,
			expect: DailyJob{
				Hour:     12,
				Minute:   0,
				Duration: Duration(10 * time.Minute),
				Storages: []string{"default"},
			},
		},
		{
			name: "override default window",
			rawCfg: `[[storage]]
			name = "default"
			path = "/"
			[daily_maintenance]
			disabled = true
			`,
			expect: DailyJob{
				Disabled: true,
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			tmpFile := strings.NewReader(tt.rawCfg)
			cfg, err := Load(tmpFile)
			if err != nil {
				require.Contains(t, err.Error(), tt.loadErr.Error())
			}
			require.Equal(t, tt.expect, cfg.DailyMaintenance)
			require.Equal(t, tt.validateErr, cfg.validateMaintenance())
		})
	}
}

func TestValidateCgroups(t *testing.T) {
	for _, tt := range []struct {
		name        string
		rawCfg      string
		expect      cgroups.Config
		validateErr error
	}{
		{
			name: "enabled success",
			rawCfg: `[cgroups]
			count = 10
			mountpoint = "/sys/fs/cgroup"
			hierarchy_root = "gitaly"
			[cgroups.memory]
			enabled = true
			limit = 1024
			[cgroups.cpu]
			enabled = true
			shares = 512`,
			expect: cgroups.Config{
				Count:         10,
				Mountpoint:    "/sys/fs/cgroup",
				HierarchyRoot: "gitaly",
				Memory: cgroups.Memory{
					Enabled: true,
					Limit:   1024,
				},
				CPU: cgroups.CPU{
					Enabled: true,
					Shares:  512,
				},
			},
		},
		{
			name: "disabled success",
			rawCfg: `[cgroups]
			count = 0`,
			expect: cgroups.Config{
				Count: 0,
			},
		},
		{
			name: "empty mount point",
			rawCfg: `[cgroups]
			count = 10
			mountpoint = ""`,
			expect: cgroups.Config{
				Count:      10,
				Mountpoint: "",
			},
			validateErr: errors.New("cgroups.mountpoint: cannot be empty"),
		},
		{
			name: "empty hierarchy_root",
			rawCfg: `[cgroups]
			count = 10
			mountpoint = "/sys/fs/cgroup"
			hierarchy_root = ""`,
			expect: cgroups.Config{
				Count:         10,
				Mountpoint:    "/sys/fs/cgroup",
				HierarchyRoot: "",
			},
			validateErr: errors.New("cgroups.hierarchy_root: cannot be empty"),
		},
		{
			name: "invalid cpu shares",
			rawCfg: `[cgroups]
			count = 10
			mountpoint = "/sys/fs/cgroup"
			hierarchy_root = "gitaly"
			[cgroups.cpu]
			enabled = true
			shares = 0`,
			expect: cgroups.Config{
				Count:         10,
				Mountpoint:    "/sys/fs/cgroup",
				HierarchyRoot: "gitaly",
				CPU: cgroups.CPU{
					Enabled: true,
					Shares:  0,
				},
			},
			validateErr: errors.New("cgroups.cpu.shares: has to be greater than zero"),
		},
		{
			name: "invalid memory limit - zero",
			rawCfg: `[cgroups]
			count = 10
			mountpoint = "/sys/fs/cgroup"
			hierarchy_root = "gitaly"
			[cgroups.memory]
			enabled = true
			limit = 0`,
			expect: cgroups.Config{
				Count:         10,
				Mountpoint:    "/sys/fs/cgroup",
				HierarchyRoot: "gitaly",
				Memory: cgroups.Memory{
					Enabled: true,
					Limit:   0,
				},
			},
			validateErr: errors.New("cgroups.memory.limit: has to be greater than zero or equal to -1"),
		},
		{
			name: "invalid memory limit - negative",
			rawCfg: `[cgroups]
			count = 10
			mountpoint = "/sys/fs/cgroup"
			hierarchy_root = "gitaly"
			[cgroups.memory]
			enabled = true
			limit = -5`,
			expect: cgroups.Config{
				Count:         10,
				Mountpoint:    "/sys/fs/cgroup",
				HierarchyRoot: "gitaly",
				Memory: cgroups.Memory{
					Enabled: true,
					Limit:   -5,
				},
			},
			validateErr: errors.New("cgroups.memory.limit: has to be greater than zero or equal to -1"),
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			tmpFile := strings.NewReader(tt.rawCfg)
			cfg, err := Load(tmpFile)
			require.NoError(t, err)
			require.Equal(t, tt.expect, cfg.Cgroups)
			require.Equal(t, tt.validateErr, cfg.validateCgroups())
		})
	}
}

func TestConfigurePackObjectsCache(t *testing.T) {
	storageConfig := `[[storage]]
name="default"
path="/foobar"
`

	testCases := []struct {
		desc string
		in   string
		out  StreamCacheConfig
		err  error
	}{
		{desc: "empty"},
		{
			desc: "enabled",
			in: storageConfig + `[pack_objects_cache]
enabled = true
`,
			out: StreamCacheConfig{Enabled: true, MaxAge: Duration(5 * time.Minute), Dir: "/foobar/+gitaly/PackObjectsCache"},
		},
		{
			desc: "enabled with custom values",
			in: storageConfig + `[pack_objects_cache]
enabled = true
dir = "/bazqux"
max_age = "10m"
`,
			out: StreamCacheConfig{Enabled: true, MaxAge: Duration(10 * time.Minute), Dir: "/bazqux"},
		},
		{
			desc: "enabled with 0 storages",
			in: `[pack_objects_cache]
enabled = true
`,
			err: errPackObjectsCacheNoStorages,
		},
		{
			desc: "enabled with negative max age",
			in: `[pack_objects_cache]
enabled = true
max_age = "-5m"
`,
			err: errPackObjectsCacheNegativeMaxAge,
		},
		{
			desc: "enabled with relative path",
			in: `[pack_objects_cache]
enabled = true
dir = "foobar"
`,
			err: errPackObjectsCacheRelativePath,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			cfg, err := Load(strings.NewReader(tc.in))
			require.NoError(t, err)

			err = cfg.configurePackObjectsCache()
			if tc.err != nil {
				require.Equal(t, tc.err, err)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tc.out, cfg.PackObjectsCache)
		})
	}
}

func TestValidateToken(t *testing.T) {
	require.NoError(t, (&Cfg{Auth: auth.Config{}}).validateToken())
	require.NoError(t, (&Cfg{Auth: auth.Config{Token: ""}}).validateToken())
	require.NoError(t, (&Cfg{Auth: auth.Config{Token: "secret"}}).validateToken())
	require.NoError(t, (&Cfg{Auth: auth.Config{Transitioning: true, Token: "secret"}}).validateToken())
}

func TestValidateBinDir(t *testing.T) {
	tmpDir := tempDir(t)
	tmpFile := filepath.Join(tmpDir, "file")
	fp, err := os.Create(tmpFile)
	require.NoError(t, err)
	require.NoError(t, fp.Close())

	for _, tc := range []struct {
		desc      string
		binDir    string
		expErrMsg string
	}{
		{
			desc:   "ok",
			binDir: tmpDir,
		},
		{
			desc:      "empty",
			binDir:    "",
			expErrMsg: "bin_dir: is not set",
		},
		{
			desc:      "path doesn't exist",
			binDir:    "/not/exists",
			expErrMsg: `bin_dir: path doesn't exist: "/not/exists"`,
		},
		{
			desc:      "is not a directory",
			binDir:    tmpFile,
			expErrMsg: fmt.Sprintf(`bin_dir: not a directory: %q`, tmpFile),
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			err := (&Cfg{BinDir: tc.binDir}).validateBinDir()
			if tc.expErrMsg == "" {
				require.NoError(t, err)
			} else {
				require.EqualError(t, err, tc.expErrMsg)
			}
		})
	}
}

func tempDir(t *testing.T) string {
	tmpdir, err := os.MkdirTemp("", "")
	require.NoError(t, err)
	t.Cleanup(func() {
		if err := os.RemoveAll(tmpdir); err != nil && !errors.Is(err, os.ErrNotExist) {
			require.NoError(t, err)
		}
	})
	return tmpdir
}
