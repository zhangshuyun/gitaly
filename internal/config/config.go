package config

import (
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/kelseyhightower/envconfig"
	log "github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/internal/config/auth"
	internallog "gitlab.com/gitlab-org/gitaly/internal/config/log"
	"gitlab.com/gitlab-org/gitaly/internal/config/prometheus"
	"gitlab.com/gitlab-org/gitaly/internal/config/sentry"
	"gitlab.com/gitlab-org/gitaly/internal/helper/text"
)

var (
	// Config stores the global configuration
	Config Cfg

	hooks []func(Cfg) error
)

// Cfg is a container for all config derived from config.toml.
type Cfg struct {
	SocketPath             string            `toml:"socket_path" split_words:"true"`
	ListenAddr             string            `toml:"listen_addr" split_words:"true"`
	TLSListenAddr          string            `toml:"tls_listen_addr" split_words:"true"`
	PrometheusListenAddr   string            `toml:"prometheus_listen_addr" split_words:"true"`
	BinDir                 string            `toml:"bin_dir"`
	Git                    Git               `toml:"git" envconfig:"git"`
	Storages               []Storage         `toml:"storage" envconfig:"storage"`
	Logging                Logging           `toml:"logging" envconfig:"logging"`
	Prometheus             prometheus.Config `toml:"prometheus"`
	Auth                   auth.Config       `toml:"auth"`
	TLS                    TLS               `toml:"tls"`
	Ruby                   Ruby              `toml:"gitaly-ruby"`
	Gitlab                 Gitlab            `toml:"gitlab"`
	GitlabShell            GitlabShell       `toml:"gitlab-shell"`
	Hooks                  Hooks             `toml:"hooks"`
	Concurrency            []Concurrency     `toml:"concurrency"`
	GracefulRestartTimeout Duration          `toml:"graceful_restart_timeout"`
	InternalSocketDir      string            `toml:"internal_socket_dir"`
}

// TLS configuration
type TLS struct {
	CertPath string `toml:"certificate_path"`
	KeyPath  string `toml:"key_path"`
}

// GitlabShell contains the settings required for executing `gitlab-shell`
type GitlabShell struct {
	Dir string `toml:"dir" json:"dir"`
}

// Gitlab contains settings required to connect to the Gitlab api
type Gitlab struct {
	URL          string       `toml:"url" json:"url"`
	HTTPSettings HTTPSettings `toml:"http-settings" json:"http_settings"`
	SecretFile   string       `toml:"secret_file" json:"secret_file"`
}

// Hooks contains the settings required for hooks
type Hooks struct {
	CustomHooksDir string `toml:"custom_hooks_dir" json:"custom_hooks_dir"`
}

type HTTPSettings struct {
	ReadTimeout int    `toml:"read_timeout" json:"read_timeout"`
	User        string `toml:"user" json:"user"`
	Password    string `toml:"password" json:"password"`
	CAFile      string `toml:"ca_file" json:"ca_file"`
	CAPath      string `toml:"ca_path" json:"ca_path"`
	SelfSigned  bool   `toml:"self_signed_cert" json:"self_signed_cert"`
}

// Git contains the settings for the Git executable
type Git struct {
	BinPath          string `toml:"bin_path"`
	CatfileCacheSize int    `toml:"catfile_cache_size"`
}

// Storage contains a single storage-shard
type Storage struct {
	Name string
	Path string
}

// Sentry is a sentry.Config. We redefine this type to a different name so
// we can embed both structs into Logging
type Sentry sentry.Config

// Logging contains the logging configuration for Gitaly
type Logging struct {
	internallog.Config
	Sentry

	RubySentryDSN string `toml:"ruby_sentry_dsn"`
}

// Concurrency allows endpoints to be limited to a maximum concurrency per repo
type Concurrency struct {
	RPC        string `toml:"rpc"`
	MaxPerRepo int    `toml:"max_per_repo"`
}

// Load initializes the Config variable from file and the environment.
//  Environment variables take precedence over the file.
func Load(file io.Reader) error {
	Config = Cfg{}

	if _, err := toml.DecodeReader(file, &Config); err != nil {
		return fmt.Errorf("load toml: %v", err)
	}

	if err := envconfig.Process("gitaly", &Config); err != nil {
		return fmt.Errorf("envconfig: %v", err)
	}

	Config.setDefaults()

	for i := range Config.Storages {
		Config.Storages[i].Path = filepath.Clean(Config.Storages[i].Path)
	}

	return nil
}

// RegisterHook adds a post-validation callback. Your hook should only
// access config via the Cfg instance it gets passed. This avoids race
// conditions during testing, when the global config.Config instance gets
// updated after these hooks have run.
func RegisterHook(f func(c Cfg) error) {
	hooks = append(hooks, f)
}

// Validate checks the current Config for sanity. It also runs all hooks
// registered with RegisterHook.
func Validate() error {
	for _, err := range []error{
		validateListeners(),
		validateStorages(),
		validateToken(),
		SetGitPath(),
		validateShell(),
		ConfigureRuby(),
		validateBinDir(),
		validateInternalSocketDir(),
		validateHooks(),
	} {
		if err != nil {
			return err
		}
	}

	for _, f := range hooks {
		if err := f(Config); err != nil {
			return err
		}
	}

	return nil
}

func (c *Cfg) setDefaults() {
	if c.GracefulRestartTimeout.Duration() == 0 {
		c.GracefulRestartTimeout = Duration(time.Minute)
	}

	if c.Gitlab.SecretFile == "" {
		c.Gitlab.SecretFile = filepath.Join(c.GitlabShell.Dir, ".gitlab_shell_secret")
	}

	if c.Hooks.CustomHooksDir == "" {
		c.Hooks.CustomHooksDir = filepath.Join(c.GitlabShell.Dir, "hooks")
	}
}

func validateListeners() error {
	if len(Config.SocketPath) == 0 && len(Config.ListenAddr) == 0 {
		return fmt.Errorf("invalid listener config: at least one of socket_path and listen_addr must be set")
	}
	return nil
}

func validateShell() error {
	if len(Config.GitlabShell.Dir) == 0 {
		return fmt.Errorf("gitlab-shell.dir is not set")
	}

	return validateIsDirectory(Config.GitlabShell.Dir, "gitlab-shell.dir")
}

func checkExecutable(path string) error {
	fi, err := os.Stat(path)
	if err != nil {
		return err
	}

	if fi.Mode()&0755 < 0755 {
		return fmt.Errorf("not executable: %v", path)
	}

	return nil
}

type hookErrs struct {
	errors []error
}

func (h *hookErrs) Error() string {
	var errStrings []string
	for _, err := range h.errors {
		errStrings = append(errStrings, err.Error())
	}

	return strings.Join(errStrings, ", ")
}

func (h *hookErrs) Add(err error) {
	h.errors = append(h.errors, err)
}

func validateHooks() error {
	if SkipHooks() {
		return nil
	}

	errs := &hookErrs{}

	for _, hookName := range []string{"pre-receive", "post-receive", "update"} {
		if err := checkExecutable(filepath.Join(Config.Ruby.Dir, "git-hooks", hookName)); err != nil {
			errs.Add(err)
			continue
		}
	}

	if len(errs.errors) > 0 {
		return errs
	}

	return nil
}

func validateIsDirectory(path, name string) error {
	s, err := os.Stat(path)
	if err != nil {
		return err
	}
	if !s.IsDir() {
		return fmt.Errorf("not a directory: %q", path)
	}

	log.WithField("dir", path).
		Debugf("%s set", name)

	return nil
}

func validateStorages() error {
	if len(Config.Storages) == 0 {
		return fmt.Errorf("no storage configurations found. Are you using the right format? https://gitlab.com/gitlab-org/gitaly/issues/397")
	}

	for i, storage := range Config.Storages {
		if storage.Name == "" {
			return fmt.Errorf("empty storage name in %+v", storage)
		}

		if storage.Path == "" {
			return fmt.Errorf("empty storage path in %+v", storage)
		}

		fs, err := os.Stat(storage.Path)
		if err != nil {
			return fmt.Errorf("storage %+v path must exist: %w", storage, err)
		}

		if !fs.IsDir() {
			return fmt.Errorf("storage %+v path must be a dir", storage)
		}

		for _, other := range Config.Storages[:i] {
			if other.Name == storage.Name {
				return fmt.Errorf("storage %q is defined more than once", storage.Name)
			}

			if storage.Path == other.Path {
				// This is weird but we allow it for legacy gitlab.com reasons.
				continue
			}

			if strings.HasPrefix(storage.Path, other.Path) || strings.HasPrefix(other.Path, storage.Path) {
				// If storages have the same sub directory, that is allowed
				if filepath.Dir(storage.Path) == filepath.Dir(other.Path) {
					continue
				}
				return fmt.Errorf("storage paths may not nest: %q and %q", storage.Name, other.Name)
			}
		}
	}

	return nil
}

func SkipHooks() bool {
	return os.Getenv("GITALY_TESTING_NO_GIT_HOOKS") == "1"
}

// SetGitPath populates the variable GitPath with the path to the `git`
// executable. It warns if no path was specified in the configuration.
func SetGitPath() error {
	if Config.Git.BinPath != "" {
		return nil
	}

	resolvedPath, err := exec.LookPath("git")
	if err != nil {
		return err
	}

	log.WithFields(log.Fields{
		"resolvedPath": resolvedPath,
	}).Warn("git path not configured. Using default path resolution")

	Config.Git.BinPath = resolvedPath

	return nil
}

// StoragePath looks up the base path for storageName. The second boolean
// return value indicates if anything was found.
func (c Cfg) StoragePath(storageName string) (string, bool) {
	storage, ok := c.Storage(storageName)
	return storage.Path, ok
}

// Storage looks up storageName.
func (c Cfg) Storage(storageName string) (Storage, bool) {
	for _, storage := range c.Storages {
		if storage.Name == storageName {
			return storage, true
		}
	}
	return Storage{}, false
}

func validateBinDir() error {
	if err := validateIsDirectory(Config.BinDir, "bin_dir"); err != nil {
		log.WithError(err).Warn("Gitaly bin directory is not configured")
		return err
	}

	var err error
	Config.BinDir, err = filepath.Abs(Config.BinDir)
	return err
}

func validateToken() error {
	if !Config.Auth.Transitioning || len(Config.Auth.Token) == 0 {
		return nil
	}

	log.Warn("Authentication is enabled but not enforced because transitioning=true. Gitaly will accept unauthenticated requests.")
	return nil
}

var (
	lazyInit                   sync.Once
	generatedInternalSocketDir string
)

func generateSocketPath() {
	// The socket path must be short-ish because listen(2) fails on long
	// socket paths. We hope/expect that ioutil.TempDir creates a directory
	// that is not too deep. We need a directory, not a tempfile, because we
	// will later want to set its permissions to 0700

	var err error
	generatedInternalSocketDir, err = ioutil.TempDir("", "gitaly-internal")
	if err != nil {
		log.Fatalf("create ruby server socket directory: %v", err)
	}
}

// InternalSocketDir will generate a temp dir for internal sockets if one is not provided in the config
func InternalSocketDir() string {
	if Config.InternalSocketDir != "" {
		return Config.InternalSocketDir
	}

	if generatedInternalSocketDir == "" {
		lazyInit.Do(generateSocketPath)
	}

	return generatedInternalSocketDir
}

// GitalyInternalSocketPath is the path to the internal gitaly socket
func GitalyInternalSocketPath() string {
	socketDir := InternalSocketDir()
	if socketDir == "" {
		panic("internal socket directory is missing")
	}

	return filepath.Join(socketDir, "internal.sock")
}

// GeneratedInternalSocketDir returns the path to the generated internal socket directory
func GeneratedInternalSocketDir() string {
	return generatedInternalSocketDir
}

func validateInternalSocketDir() error {
	if Config.InternalSocketDir == "" {
		return nil
	}

	dir := Config.InternalSocketDir

	f, err := os.Stat(dir)
	switch {
	case err != nil:
		return fmt.Errorf("InternalSocketDir: %s", err)
	case !f.IsDir():
		return fmt.Errorf("InternalSocketDir %s is not a directory", dir)
	}

	return trySocketCreation(dir)
}

func trySocketCreation(dir string) error {
	// To validate the socket can actually be created, we open and close a socket.
	// Any error will be assumed persistent for when the gitaly-ruby sockets are created
	// and thus fatal at boot time
	b, err := text.RandomHex(4)
	if err != nil {
		return err
	}

	socketPath := filepath.Join(dir, fmt.Sprintf("test-%s.sock", b))
	defer os.Remove(socketPath)

	// Attempt to create an actual socket and not just a file to catch socket path length problems
	l, err := net.Listen("unix", socketPath)
	if err != nil {
		return fmt.Errorf("socket could not be created in %s: %s", dir, err)
	}

	return l.Close()
}
