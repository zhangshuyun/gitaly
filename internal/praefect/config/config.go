package config

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
	"gitlab.com/gitlab-org/gitaly/internal/config"
	"gitlab.com/gitlab-org/gitaly/internal/config/auth"
	"gitlab.com/gitlab-org/gitaly/internal/config/log"
	"gitlab.com/gitlab-org/gitaly/internal/config/prometheus"
	"gitlab.com/gitlab-org/gitaly/internal/config/sentry"
)

type Failover struct {
	Enabled                  bool            `toml:"enabled"`
	ElectionStrategy         string          `toml:"election_strategy"`
	ReadOnlyAfterFailover    bool            `toml:"read_only_after_failover"`
	ErrorThresholdWindow     config.Duration `toml:"error_threshold_window"`
	WriteErrorThresholdCount uint32          `toml:"write_error_threshold_count"`
	ReadErrorThresholdCount  uint32          `toml:"read_error_threshold_count"`
}

const sqlFailoverValue = "sql"

// Config is a container for everything found in the TOML config file
type Config struct {
	ListenAddr           string            `toml:"listen_addr"`
	TLSListenAddr        string            `toml:"tls_listen_addr"`
	SocketPath           string            `toml:"socket_path"`
	VirtualStorages      []*VirtualStorage `toml:"virtual_storage"`
	Logging              log.Config        `toml:"logging"`
	Sentry               sentry.Config     `toml:"sentry"`
	PrometheusListenAddr string            `toml:"prometheus_listen_addr"`
	Prometheus           prometheus.Config `toml:"prometheus"`
	Auth                 auth.Config       `toml:"auth"`
	TLS                  config.TLS        `toml:"tls"`
	DB                   `toml:"database"`
	Failover             Failover `toml:"failover"`
	// Keep for legacy reasons: remove after Omnibus has switched
	FailoverEnabled     bool            `toml:"failover_enabled"`
	MemoryQueueEnabled  bool            `toml:"memory_queue_enabled"`
	GracefulStopTimeout config.Duration `toml:"graceful_stop_timeout"`
}

// VirtualStorage represents a set of nodes for a storage
type VirtualStorage struct {
	Name  string  `toml:"name"`
	Nodes []*Node `toml:"node"`
}

// FromFile loads the config for the passed file path
func FromFile(filePath string) (Config, error) {
	conf := &Config{
		// Sets the default Failover, to be overwritten when deserializing the TOML
		Failover: Failover{Enabled: true, ElectionStrategy: sqlFailoverValue, ReadOnlyAfterFailover: true},
	}
	if _, err := toml.DecodeFile(filePath, conf); err != nil {
		return Config{}, err
	}

	conf.setDefaults()

	// TODO: Remove this after failover_enabled has moved under a separate failover section. This is for
	// backwards compatibility only
	if conf.FailoverEnabled {
		conf.Failover.Enabled = true
	}

	return *conf, nil
}

var (
	errDuplicateStorage         = errors.New("internal gitaly storages are not unique")
	errGitalyWithoutAddr        = errors.New("all gitaly nodes must have an address")
	errGitalyWithoutStorage     = errors.New("all gitaly nodes must have a storage")
	errMoreThanOnePrimary       = errors.New("only 1 node can be designated as a primary")
	errNoGitalyServers          = errors.New("no primary gitaly backends configured")
	errNoListener               = errors.New("no listen address or socket path configured")
	errNoPrimaries              = errors.New("no primaries designated")
	errNoVirtualStorages        = errors.New("no virtual storages configured")
	errStorageAddressDuplicate  = errors.New("multiple storages have the same address")
	errVirtualStoragesNotUnique = errors.New("virtual storages must have unique names")
	errVirtualStorageUnnamed    = errors.New("virtual storages must have a name")
)

// Validate establishes if the config is valid
func (c *Config) Validate() error {
	if c.ListenAddr == "" && c.SocketPath == "" && c.TLSListenAddr == "" {
		return errNoListener
	}

	if len(c.VirtualStorages) == 0 {
		return errNoVirtualStorages
	}

	allAddresses := make(map[string]struct{})
	virtualStorages := make(map[string]struct{}, len(c.VirtualStorages))

	for _, virtualStorage := range c.VirtualStorages {
		if virtualStorage.Name == "" {
			return errVirtualStorageUnnamed
		}

		if len(virtualStorage.Nodes) == 0 {
			return fmt.Errorf("virtual storage %q: %w", virtualStorage.Name, errNoGitalyServers)
		}

		if _, ok := virtualStorages[virtualStorage.Name]; ok {
			return fmt.Errorf("virtual storage %q: %w", virtualStorage.Name, errVirtualStoragesNotUnique)
		}
		virtualStorages[virtualStorage.Name] = struct{}{}

		storages := make(map[string]struct{}, len(virtualStorage.Nodes))
		primaries := 0
		for _, node := range virtualStorage.Nodes {
			if node.DefaultPrimary {
				primaries++
			}

			if primaries > 1 {
				return fmt.Errorf("virtual storage %q: %w", virtualStorage.Name, errMoreThanOnePrimary)
			}

			if node.Storage == "" {
				return fmt.Errorf("virtual storage %q: %w", virtualStorage.Name, errGitalyWithoutStorage)
			}

			if node.Address == "" {
				return fmt.Errorf("virtual storage %q: %w", virtualStorage.Name, errGitalyWithoutAddr)
			}

			if _, found := storages[node.Storage]; found {
				return fmt.Errorf("virtual storage %q: %w", virtualStorage.Name, errDuplicateStorage)
			}
			storages[node.Storage] = struct{}{}

			if _, found := allAddresses[node.Address]; found {
				return fmt.Errorf("virtual storage %q: address %q : %w", virtualStorage.Name, node.Address, errStorageAddressDuplicate)
			}
			allAddresses[node.Address] = struct{}{}
		}

		if primaries == 0 {
			return fmt.Errorf("virtual storage %q: %w", virtualStorage.Name, errNoPrimaries)
		}
	}

	return nil
}

// NeedsSQL returns true if the driver for SQL needs to be initialized
func (c *Config) NeedsSQL() bool {
	return !c.MemoryQueueEnabled ||
		(c.Failover.Enabled && c.Failover.ElectionStrategy == sqlFailoverValue)
}

func (c *Config) setDefaults() {
	if c.GracefulStopTimeout.Duration() == 0 {
		c.GracefulStopTimeout = config.Duration(time.Minute)
	}
}

// VirtualStorageNames returns names of all virtual storages configured.
func (c *Config) VirtualStorageNames() []string {
	names := make([]string, len(c.VirtualStorages))
	for i, virtual := range c.VirtualStorages {
		names[i] = virtual.Name
	}
	return names
}

// StorageNames returns storage names by virtual storage.
func (c *Config) StorageNames() map[string][]string {
	storages := make(map[string][]string, len(c.VirtualStorages))
	for _, vs := range c.VirtualStorages {
		nodes := make([]string, len(vs.Nodes))
		for i, n := range vs.Nodes {
			nodes[i] = n.Storage
		}

		storages[vs.Name] = nodes
	}

	return storages
}

// DB holds Postgres client configuration data.
type DB struct {
	Host                         string `toml:"host"`
	Port                         int    `toml:"port"`
	User                         string `toml:"user"`
	Password                     string `toml:"password"`
	DBName                       string `toml:"dbname"`
	SSLMode                      string `toml:"sslmode"`
	SSLCert                      string `toml:"sslcert"`
	SSLKey                       string `toml:"sslkey"`
	SSLRootCert                  string `toml:"sslrootcert"`
	StatementTimeoutMilliseconds int    `toml:"default_timeout_ms"`
}

// ToPQString returns a connection string that can be passed to github.com/lib/pq.
func (db DB) ToPQString() string {
	fields := []string{fmt.Sprintf("statement_timeout=%d", db.StatementTimeoutMilliseconds)}
	if db.Port > 0 {
		fields = append(fields, fmt.Sprintf("port=%d", db.Port))
	}

	for _, kv := range []struct{ key, value string }{
		{"host", db.Host},
		{"user", db.User},
		{"password", db.Password},
		{"dbname", db.DBName},
		{"sslmode", db.SSLMode},
		{"sslcert", db.SSLCert},
		{"sslkey", db.SSLKey},
		{"sslrootcert", db.SSLRootCert},
		{"binary_parameters", "yes"},
	} {
		if len(kv.value) == 0 {
			continue
		}

		kv.value = strings.ReplaceAll(kv.value, "'", `\'`)
		kv.value = strings.ReplaceAll(kv.value, " ", `\ `)

		fields = append(fields, kv.key+"="+kv.value)
	}

	return strings.Join(fields, " ")
}
