package config

import (
	"errors"
	"fmt"
	"io/ioutil"
	"strings"
	"time"

	"github.com/pelletier/go-toml"
	promclient "github.com/prometheus/client_golang/prometheus"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config/auth"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config/log"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config/prometheus"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config/sentry"
)

// ElectionStrategy is a Praefect primary election strategy.
type ElectionStrategy string

// validate validates the election strategy is a valid one.
func (es ElectionStrategy) validate() error {
	switch es {
	case ElectionStrategyLocal, ElectionStrategySQL, ElectionStrategyPerRepository:
		return nil
	default:
		return fmt.Errorf("invalid election strategy: %q", es)
	}
}

const (
	// ElectionStrategyLocal configures a single node, in-memory election strategy.
	ElectionStrategyLocal ElectionStrategy = "local"
	// ElectionStrategySQL configures an SQL based strategy that elects a primary for a virtual storage.
	ElectionStrategySQL ElectionStrategy = "sql"
	// ElectionStrategyPerRepository configures an SQL based strategy that elects different primaries per repository.
	ElectionStrategyPerRepository ElectionStrategy = "per_repository"
)

type Failover struct {
	Enabled bool `toml:"enabled"`
	// ElectionStrategy is the strategy to use for electing primaries nodes.
	ElectionStrategy         ElectionStrategy `toml:"election_strategy"`
	ErrorThresholdWindow     config.Duration  `toml:"error_threshold_window"`
	WriteErrorThresholdCount uint32           `toml:"write_error_threshold_count"`
	ReadErrorThresholdCount  uint32           `toml:"read_error_threshold_count"`
	// BootstrapInterval allows set a time duration that would be used on startup to make initial health check.
	// The default value is 1s.
	BootstrapInterval config.Duration `toml:"bootstrap_interval"`
	// MonitorInterval allows set a time duration that would be used after bootstrap is completed to execute health checks.
	// The default value is 3s.
	MonitorInterval config.Duration `toml:"monitor_interval"`
}

// ErrorThresholdsConfigured checks whether returns whether the errors thresholds are configured. If they
// are configured but in an invalid way, an error is returned.
func (f Failover) ErrorThresholdsConfigured() (bool, error) {
	if f.ErrorThresholdWindow == 0 && f.WriteErrorThresholdCount == 0 && f.ReadErrorThresholdCount == 0 {
		return false, nil
	}

	if f.ErrorThresholdWindow == 0 {
		return false, errors.New("threshold window not set")
	}

	if f.WriteErrorThresholdCount == 0 {
		return false, errors.New("write error threshold not set")
	}

	if f.ReadErrorThresholdCount == 0 {
		return false, errors.New("read error threshold not set")
	}

	return true, nil
}

// Reconciliation contains reconciliation specific configuration options.
type Reconciliation struct {
	// SchedulingInterval the interval between each automatic reconciliation run. If set to 0,
	// automatic reconciliation is disabled.
	SchedulingInterval config.Duration `toml:"scheduling_interval"`
	// HistogramBuckets configures the reconciliation scheduling duration histogram's buckets.
	HistogramBuckets []float64 `toml:"histogram_buckets"`
}

// DefaultReconciliationConfig returns the default values for reconciliation configuration.
func DefaultReconciliationConfig() Reconciliation {
	return Reconciliation{
		SchedulingInterval: 5 * config.Duration(time.Minute),
		HistogramBuckets:   promclient.DefBuckets,
	}
}

// Replication contains replication specific configuration options.
type Replication struct {
	// BatchSize controls how many replication jobs to dequeue and lock
	// in a single call to the database.
	BatchSize uint `toml:"batch_size"`
}

// DefaultReplicationConfig returns the default values for replication configuration.
func DefaultReplicationConfig() Replication {
	return Replication{BatchSize: 10}
}

// Config is a container for everything found in the TOML config file
type Config struct {
	AllowLegacyElectors  bool              `toml:"i_understand_my_election_strategy_is_unsupported_and_will_be_removed_without_warning"`
	Reconciliation       Reconciliation    `toml:"reconciliation"`
	Replication          Replication       `toml:"replication"`
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
	// DefaultReplicationFactor is the replication factor set for new repositories.
	// A valid value is inclusive between 1 and the number of configured storages in the
	// virtual storage. Setting the value to 0 or below causes Praefect to not store any
	// host assignments, falling back to the behavior of replicating to every configured
	// storage
	DefaultReplicationFactor int `toml:"default_replication_factor"`
}

// FromFile loads the config for the passed file path
func FromFile(filePath string) (Config, error) {
	b, err := ioutil.ReadFile(filePath)
	if err != nil {
		return Config{}, err
	}

	conf := &Config{
		Reconciliation: DefaultReconciliationConfig(),
		Replication:    DefaultReplicationConfig(),
		// Sets the default Failover, to be overwritten when deserializing the TOML
		Failover: Failover{Enabled: true, ElectionStrategy: ElectionStrategyPerRepository},
	}
	if err := toml.Unmarshal(b, conf); err != nil {
		return Config{}, err
	}

	// TODO: Remove this after failover_enabled has moved under a separate failover section. This is for
	// backwards compatibility only
	if conf.FailoverEnabled {
		conf.Failover.Enabled = true
	}

	conf.setDefaults()

	return *conf, nil
}

var (
	errDuplicateStorage         = errors.New("internal gitaly storages are not unique")
	errGitalyWithoutAddr        = errors.New("all gitaly nodes must have an address")
	errGitalyWithoutStorage     = errors.New("all gitaly nodes must have a storage")
	errNoGitalyServers          = errors.New("no primary gitaly backends configured")
	errNoListener               = errors.New("no listen address or socket path configured")
	errNoVirtualStorages        = errors.New("no virtual storages configured")
	errStorageAddressDuplicate  = errors.New("multiple storages have the same address")
	errVirtualStoragesNotUnique = errors.New("virtual storages must have unique names")
	errVirtualStorageUnnamed    = errors.New("virtual storages must have a name")
)

// Validate establishes if the config is valid
func (c *Config) Validate() error {
	if err := c.Failover.ElectionStrategy.validate(); err != nil {
		return err
	}

	if c.ListenAddr == "" && c.SocketPath == "" && c.TLSListenAddr == "" {
		return errNoListener
	}

	if len(c.VirtualStorages) == 0 {
		return errNoVirtualStorages
	}

	if c.Replication.BatchSize < 1 {
		return fmt.Errorf("replication batch size was %d but must be >=1", c.Replication.BatchSize)
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
		for _, node := range virtualStorage.Nodes {
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

		if virtualStorage.DefaultReplicationFactor > len(virtualStorage.Nodes) {
			return fmt.Errorf(
				"virtual storage %q has a default replication factor (%d) which is higher than the number of storages (%d)",
				virtualStorage.Name, virtualStorage.DefaultReplicationFactor, len(virtualStorage.Nodes),
			)
		}
	}

	return nil
}

// NeedsSQL returns true if the driver for SQL needs to be initialized
func (c *Config) NeedsSQL() bool {
	return !c.MemoryQueueEnabled || (c.Failover.Enabled && c.Failover.ElectionStrategy != ElectionStrategyLocal)
}

func (c *Config) setDefaults() {
	if c.GracefulStopTimeout.Duration() == 0 {
		c.GracefulStopTimeout = config.Duration(time.Minute)
	}

	if c.Failover.Enabled {
		if c.Failover.BootstrapInterval.Duration() == 0 {
			c.Failover.BootstrapInterval = config.Duration(time.Second)
		}

		if c.Failover.MonitorInterval.Duration() == 0 {
			c.Failover.MonitorInterval = config.Duration(3 * time.Second)
		}
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

// DefaultReplicationFactors returns a map with the default replication factors of
// the virtual storages.
func (c Config) DefaultReplicationFactors() map[string]int {
	replicationFactors := make(map[string]int, len(c.VirtualStorages))
	for _, vs := range c.VirtualStorages {
		replicationFactors[vs.Name] = vs.DefaultReplicationFactor
	}

	return replicationFactors
}

// DB holds Postgres client configuration data.
type DB struct {
	Host        string `toml:"host"`
	Port        int    `toml:"port"`
	User        string `toml:"user"`
	Password    string `toml:"password"`
	DBName      string `toml:"dbname"`
	SSLMode     string `toml:"sslmode"`
	SSLCert     string `toml:"sslcert"`
	SSLKey      string `toml:"sslkey"`
	SSLRootCert string `toml:"sslrootcert"`
	HostNoProxy string `toml:"host_no_proxy"`
	PortNoProxy int    `toml:"port_no_proxy"`
}

// ToPQString returns a connection string that can be passed to github.com/lib/pq.
func (db DB) ToPQString(direct bool) string {
	var hostVal string
	var portVal int

	if direct {
		if db.HostNoProxy == "" || db.PortNoProxy == 0 {
			return ""
		}
		hostVal = db.HostNoProxy
		portVal = db.PortNoProxy
	} else {
		hostVal = db.Host
		portVal = db.Port
	}

	var fields []string
	if portVal > 0 {
		fields = append(fields, fmt.Sprintf("port=%d", portVal))
	}

	for _, kv := range []struct{ key, value string }{
		{"host", hostVal},
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
