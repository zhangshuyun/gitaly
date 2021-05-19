package config

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"time"

	"github.com/kelseyhightower/envconfig"
	"github.com/pelletier/go-toml"
	log "github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config/auth"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config/cgroups"
	internallog "gitlab.com/gitlab-org/gitaly/internal/gitaly/config/log"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config/prometheus"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config/sentry"
	"gitlab.com/gitlab-org/gitaly/internal/helper/text"
	"golang.org/x/sys/unix"
)

const (
	// GitalyDataPrefix is the top-level directory we use to store system
	// (non-user) data. We need to be careful that this path does not clash
	// with any directory name that could be provided by a user. The '+'
	// character is not allowed in GitLab namespaces or repositories.
	GitalyDataPrefix = "+gitaly"
)

// DailyJob enables a daily task to be scheduled for specific storages
type DailyJob struct {
	Hour     uint     `toml:"start_hour"`
	Minute   uint     `toml:"start_minute"`
	Duration Duration `toml:"duration"`
	Storages []string `toml:"storages"`

	// Disabled will completely disable a daily job, even in cases where a
	// default schedule is implied
	Disabled bool `toml:"disabled"`
}

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
	DailyMaintenance       DailyJob          `toml:"daily_maintenance"`
	Cgroups                cgroups.Config    `toml:"cgroups"`
	PackObjectsCache       PackObjectsCache  `toml:"pack_objects_cache"`
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
	URL             string       `toml:"url" json:"url"`
	RelativeURLRoot string       `toml:"relative_url_root" json:"relative_url_root"` // For UNIX sockets only
	HTTPSettings    HTTPSettings `toml:"http-settings" json:"http_settings"`
	SecretFile      string       `toml:"secret_file" json:"secret_file"`
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
	BinPath          string      `toml:"bin_path"`
	CatfileCacheSize int         `toml:"catfile_cache_size"`
	Config           []GitConfig `toml:"config"`
}

// GitConfig contains a key-value pair which is to be passed to git as configuration.
type GitConfig struct {
	Key   string `toml:"key"`
	Value string `toml:"value"`
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

// PackObjectsCache contains settings for the pack-objects cache.
type PackObjectsCache struct {
	Enabled bool     `toml:"enabled"` // Default: false
	Dir     string   `toml:"dir"`     // Default: <FIRST STORAGE PATH>/+gitaly/PackObjectsCache
	MaxAge  Duration `toml:"max_age"` // Default: 5m
}

// Load initializes the Config variable from file and the environment.
//  Environment variables take precedence over the file.
func Load(file io.Reader) (Cfg, error) {
	var cfg Cfg

	if err := toml.NewDecoder(file).Decode(&cfg); err != nil {
		return Cfg{}, fmt.Errorf("load toml: %v", err)
	}

	if err := envconfig.Process("gitaly", &cfg); err != nil {
		return Cfg{}, fmt.Errorf("envconfig: %v", err)
	}

	if err := cfg.setDefaults(); err != nil {
		return Cfg{}, err
	}

	for i := range cfg.Storages {
		cfg.Storages[i].Path = filepath.Clean(cfg.Storages[i].Path)
	}

	return cfg, nil
}

// Validate checks the current Config for sanity.
func (cfg *Cfg) Validate() error {
	for _, run := range []func() error{
		cfg.validateListeners,
		cfg.validateStorages,
		cfg.validateToken,
		cfg.validateGit,
		cfg.validateShell,
		cfg.ConfigureRuby,
		cfg.validateBinDir,
		cfg.validateInternalSocketDir,
		cfg.validateHooks,
		cfg.validateMaintenance,
		cfg.validateCgroups,
		cfg.configurePackObjectsCache,
	} {
		if err := run(); err != nil {
			return err
		}
	}

	return nil
}

func (cfg *Cfg) setDefaults() error {
	if cfg.GracefulRestartTimeout.Duration() == 0 {
		cfg.GracefulRestartTimeout = Duration(time.Minute)
	}

	if cfg.Gitlab.SecretFile == "" {
		cfg.Gitlab.SecretFile = filepath.Join(cfg.GitlabShell.Dir, ".gitlab_shell_secret")
	}

	if cfg.Hooks.CustomHooksDir == "" {
		cfg.Hooks.CustomHooksDir = filepath.Join(cfg.GitlabShell.Dir, "hooks")
	}

	if cfg.InternalSocketDir == "" {
		// The socket path must be short-ish because listen(2) fails on long
		// socket paths. We hope/expect that ioutil.TempDir creates a directory
		// that is not too deep. We need a directory, not a tempfile, because we
		// will later want to set its permissions to 0700

		tmpDir, err := ioutil.TempDir("", "gitaly-internal")
		if err != nil {
			return fmt.Errorf("create internal socket directory: %w", err)
		}
		cfg.InternalSocketDir = tmpDir
	}

	if reflect.DeepEqual(cfg.DailyMaintenance, DailyJob{}) {
		cfg.DailyMaintenance = defaultMaintenanceWindow(cfg.Storages)
	}

	return nil
}

func (cfg *Cfg) validateListeners() error {
	if len(cfg.SocketPath) == 0 && len(cfg.ListenAddr) == 0 {
		return fmt.Errorf("invalid listener config: at least one of socket_path and listen_addr must be set")
	}
	return nil
}

func (cfg *Cfg) validateShell() error {
	if len(cfg.GitlabShell.Dir) == 0 {
		return fmt.Errorf("gitlab-shell.dir is not set")
	}

	return validateIsDirectory(cfg.GitlabShell.Dir, "gitlab-shell.dir")
}

func checkExecutable(path string) error {
	if err := unix.Access(path, unix.X_OK); err != nil {
		if errors.Is(err, os.ErrPermission) {
			return fmt.Errorf("not executable: %v", path)
		}
		return err
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

func (cfg *Cfg) validateHooks() error {
	if SkipHooks() {
		return nil
	}

	errs := &hookErrs{}

	for _, hookName := range []string{"pre-receive", "post-receive", "update"} {
		if err := checkExecutable(filepath.Join(cfg.Ruby.Dir, "git-hooks", hookName)); err != nil {
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

func (cfg *Cfg) validateStorages() error {
	if len(cfg.Storages) == 0 {
		return fmt.Errorf("no storage configurations found. Are you using the right format? https://gitlab.com/gitlab-org/gitaly/issues/397")
	}

	for i, storage := range cfg.Storages {
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

		for _, other := range cfg.Storages[:i] {
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
func (cfg *Cfg) SetGitPath() error {
	if cfg.Git.BinPath != "" {
		return nil
	}

	if path, ok := os.LookupEnv("GITALY_TESTING_GIT_BINARY"); ok {
		cfg.Git.BinPath = path
		return nil
	}

	resolvedPath, err := exec.LookPath("git")
	if err != nil {
		return err
	}

	log.WithFields(log.Fields{
		"resolvedPath": resolvedPath,
	}).Warn("git path not configured. Using default path resolution")

	cfg.Git.BinPath = resolvedPath

	return nil
}

// StoragePath looks up the base path for storageName. The second boolean
// return value indicates if anything was found.
func (cfg *Cfg) StoragePath(storageName string) (string, bool) {
	storage, ok := cfg.Storage(storageName)
	return storage.Path, ok
}

// Storage looks up storageName.
func (cfg *Cfg) Storage(storageName string) (Storage, bool) {
	for _, storage := range cfg.Storages {
		if storage.Name == storageName {
			return storage, true
		}
	}
	return Storage{}, false
}

// GitalyInternalSocketPath is the path to the internal gitaly socket
func (cfg *Cfg) GitalyInternalSocketPath() string {
	return filepath.Join(cfg.InternalSocketDir, fmt.Sprintf("internal_%d.sock", os.Getpid()))
}

func (cfg *Cfg) validateBinDir() error {
	if err := validateIsDirectory(cfg.BinDir, "bin_dir"); err != nil {
		log.WithError(err).Warn("Gitaly bin directory is not configured")
		return err
	}

	var err error
	cfg.BinDir, err = filepath.Abs(cfg.BinDir)
	return err
}

// validateGitConfigKey does a best-effort check whether or not a given git config key is valid. It
// does not allow for assignments in keys, which is overly strict and does not allow some valid
// keys. It does avoid misinterpretation of keys though and should catch many cases of
// misconfiguration.
func validateGitConfigKey(key string) error {
	if key == "" {
		return errors.New("key cannot be empty")
	}
	if strings.Contains(key, "=") {
		return errors.New("key cannot contain assignment")
	}
	if !strings.Contains(key, ".") {
		return errors.New("key must contain at least one section")
	}
	if strings.HasPrefix(key, ".") || strings.HasSuffix(key, ".") {
		return errors.New("key must not start or end with a dot")
	}
	return nil
}

func (cfg *Cfg) validateGit() error {
	if err := cfg.SetGitPath(); err != nil {
		return err
	}

	for _, configPair := range cfg.Git.Config {
		if err := validateGitConfigKey(configPair.Key); err != nil {
			return fmt.Errorf("invalid configuration key %q: %w", configPair.Key, err)
		}
		if configPair.Value == "" {
			return fmt.Errorf("invalid configuration value: %q", configPair.Value)
		}
	}

	return nil
}

func (cfg *Cfg) validateToken() error {
	if !cfg.Auth.Transitioning || len(cfg.Auth.Token) == 0 {
		return nil
	}

	log.Warn("Authentication is enabled but not enforced because transitioning=true. Gitaly will accept unauthenticated requests.")
	return nil
}

func (cfg *Cfg) validateInternalSocketDir() error {
	if cfg.InternalSocketDir == "" {
		return nil
	}

	dir := cfg.InternalSocketDir

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

// defaultMaintenanceWindow specifies a 10 minute job that runs daily at +1200
// GMT time
func defaultMaintenanceWindow(storages []Storage) DailyJob {
	storageNames := make([]string, len(storages))
	for i, s := range storages {
		storageNames[i] = s.Name
	}

	return DailyJob{
		Hour:     12,
		Minute:   0,
		Duration: Duration(10 * time.Minute),
		Storages: storageNames,
	}
}

func (cfg *Cfg) validateMaintenance() error {
	dm := cfg.DailyMaintenance

	sNames := map[string]struct{}{}
	for _, s := range cfg.Storages {
		sNames[s.Name] = struct{}{}
	}
	for _, sName := range dm.Storages {
		if _, ok := sNames[sName]; !ok {
			return fmt.Errorf("daily maintenance specified storage %q does not exist in configuration", sName)
		}
	}

	if dm.Hour > 23 {
		return fmt.Errorf("daily maintenance specified hour '%d' outside range (0-23)", dm.Hour)
	}
	if dm.Minute > 59 {
		return fmt.Errorf("daily maintenance specified minute '%d' outside range (0-59)", dm.Minute)
	}
	if dm.Duration.Duration() > 24*time.Hour {
		return fmt.Errorf("daily maintenance specified duration %s must be less than 24 hours", dm.Duration.Duration())
	}

	return nil
}

func (cfg *Cfg) validateCgroups() error {
	cg := cfg.Cgroups

	if cg.Count == 0 {
		return nil
	}

	if cg.Mountpoint == "" {
		return fmt.Errorf("cgroups mountpoint cannot be empty")
	}

	if cg.HierarchyRoot == "" {
		return fmt.Errorf("cgroups hierarchy root cannot be empty")
	}

	if cg.CPU.Enabled && cg.CPU.Shares == 0 {
		return fmt.Errorf("cgroups CPU shares has to be greater than zero")
	}

	if cg.Memory.Enabled && (cg.Memory.Limit == 0 || cg.Memory.Limit < -1) {
		return fmt.Errorf("cgroups memory limit has to be greater than zero or equal to -1")
	}

	return nil
}

var (
	errPackObjectsCacheNegativeMaxAge = errors.New("pack_objects_cache.max_age cannot be negative")
	errPackObjectsCacheNoStorages     = errors.New("pack_objects_cache: cannot pick default cache directory: no storages")
	errPackObjectsCacheRelativePath   = errors.New("pack_objects_cache: storage directory must be absolute path")
)

func (cfg *Cfg) configurePackObjectsCache() error {
	poc := &cfg.PackObjectsCache
	if !poc.Enabled {
		return nil
	}

	if poc.MaxAge < 0 {
		return errPackObjectsCacheNegativeMaxAge
	}

	if poc.MaxAge == 0 {
		poc.MaxAge = Duration(5 * time.Minute)
	}

	if poc.Dir == "" {
		if len(cfg.Storages) == 0 {
			return errPackObjectsCacheNoStorages
		}

		poc.Dir = filepath.Join(cfg.Storages[0].Path, GitalyDataPrefix, "PackObjectsCache")
	}

	if !filepath.IsAbs(poc.Dir) {
		return errPackObjectsCacheRelativePath
	}

	return nil
}
