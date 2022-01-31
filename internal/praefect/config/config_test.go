package config

import (
	"bytes"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/pelletier/go-toml"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config/log"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config/prometheus"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config/sentry"
)

func TestConfigValidation(t *testing.T) {
	vs1Nodes := []*Node{
		{Storage: "internal-1.0", Address: "localhost:23456", Token: "secret-token-1"},
		{Storage: "internal-2.0", Address: "localhost:23457", Token: "secret-token-1"},
		{Storage: "internal-3.0", Address: "localhost:23458", Token: "secret-token-1"},
	}

	vs2Nodes := []*Node{
		// storage can have same name as storage in another virtual storage, but all addresses must be unique
		{Storage: "internal-1.0", Address: "localhost:33456", Token: "secret-token-2"},
		{Storage: "internal-2.1", Address: "localhost:33457", Token: "secret-token-2"},
		{Storage: "internal-3.1", Address: "localhost:33458", Token: "secret-token-2"},
	}

	testCases := []struct {
		desc         string
		changeConfig func(*Config)
		errMsg       string
	}{
		{
			desc:         "Valid config with ListenAddr",
			changeConfig: func(*Config) {},
		},
		{
			desc: "Valid config with local elector",
			changeConfig: func(cfg *Config) {
				cfg.Failover.ElectionStrategy = ElectionStrategyLocal
			},
		},
		{
			desc: "Valid config with per repository elector",
			changeConfig: func(cfg *Config) {
				cfg.Failover.ElectionStrategy = ElectionStrategyPerRepository
			},
		},
		{
			desc: "Invalid election strategy",
			changeConfig: func(cfg *Config) {
				cfg.Failover.ElectionStrategy = "invalid-strategy"
			},
			errMsg: `invalid election strategy: "invalid-strategy"`,
		},
		{
			desc: "Valid config with TLSListenAddr",
			changeConfig: func(cfg *Config) {
				cfg.ListenAddr = ""
				cfg.TLSListenAddr = "tls://localhost:4321"
			},
		},
		{
			desc: "Valid config with SocketPath",
			changeConfig: func(cfg *Config) {
				cfg.ListenAddr = ""
				cfg.SocketPath = "/tmp/praefect.socket"
			},
		},
		{
			desc: "Invalid replication batch size",
			changeConfig: func(cfg *Config) {
				cfg.Replication = Replication{BatchSize: 0}
			},
			errMsg: "replication batch size was 0 but must be >=1",
		},
		{
			desc: "No ListenAddr or SocketPath or TLSListenAddr",
			changeConfig: func(cfg *Config) {
				cfg.ListenAddr = ""
			},
			errMsg: "no listen address or socket path configured",
		},
		{
			desc: "No virtual storages",
			changeConfig: func(cfg *Config) {
				cfg.VirtualStorages = nil
			},
			errMsg: "no virtual storages configured",
		},
		{
			desc: "duplicate storage",
			changeConfig: func(cfg *Config) {
				cfg.VirtualStorages = []*VirtualStorage{
					{
						Name: "default",
						Nodes: append(vs1Nodes, &Node{
							Storage: vs1Nodes[0].Storage,
							Address: vs1Nodes[1].Address,
						}),
					},
				}
			},
			errMsg: `virtual storage "default": internal gitaly storages are not unique`,
		},
		{
			desc: "Node storage has no name",
			changeConfig: func(cfg *Config) {
				cfg.VirtualStorages = []*VirtualStorage{
					{
						Name: "default",
						Nodes: []*Node{
							{
								Storage: "",
								Address: "localhost:23456",
								Token:   "secret-token-1",
							},
						},
					},
				}
			},
			errMsg: `virtual storage "default": all gitaly nodes must have a storage`,
		},
		{
			desc: "Node storage has no address",
			changeConfig: func(cfg *Config) {
				cfg.VirtualStorages = []*VirtualStorage{
					{
						Name: "default",
						Nodes: []*Node{
							{
								Storage: "internal",
								Address: "",
								Token:   "secret-token-1",
							},
						},
					},
				}
			},
			errMsg: `virtual storage "default": all gitaly nodes must have an address`,
		},
		{
			desc: "Virtual storage has no name",
			changeConfig: func(cfg *Config) {
				cfg.VirtualStorages = []*VirtualStorage{
					{Name: "", Nodes: vs1Nodes},
				}
			},
			errMsg: `virtual storages must have a name`,
		},
		{
			desc: "Virtual storage not unique",
			changeConfig: func(cfg *Config) {
				cfg.VirtualStorages = []*VirtualStorage{
					{Name: "default", Nodes: vs1Nodes},
					{Name: "default", Nodes: vs2Nodes},
				}
			},
			errMsg: `virtual storage "default": virtual storages must have unique names`,
		},
		{
			desc: "Virtual storage has no nodes",
			changeConfig: func(cfg *Config) {
				cfg.VirtualStorages = []*VirtualStorage{
					{Name: "default", Nodes: vs1Nodes},
					{Name: "secondary", Nodes: nil},
				}
			},
			errMsg: `virtual storage "secondary": no primary gitaly backends configured`,
		},
		{
			desc: "Node storage has address duplicate",
			changeConfig: func(cfg *Config) {
				cfg.VirtualStorages = []*VirtualStorage{
					{Name: "default", Nodes: vs1Nodes},
					{Name: "secondary", Nodes: append(vs2Nodes, vs1Nodes[1])},
				}
			},
			errMsg: `multiple storages have the same address`,
		},
		{
			desc: "default replication factor too high",
			changeConfig: func(cfg *Config) {
				cfg.VirtualStorages = []*VirtualStorage{
					{
						Name:                     "default",
						DefaultReplicationFactor: 2,
						Nodes: []*Node{
							{
								Storage: "storage-1",
								Address: "localhost:23456",
							},
						},
					},
				}
			},
			errMsg: `virtual storage "default" has a default replication factor (2) which is higher than the number of storages (1)`,
		},
		{
			desc: "repositories_cleanup minimal duration is too low",
			changeConfig: func(cfg *Config) {
				cfg.RepositoriesCleanup.CheckInterval = config.Duration(minimalSyncCheckInterval - time.Nanosecond)
			},
			errMsg: `repositories_cleanup.check_interval is less then 1m0s, which could lead to a database performance problem`,
		},
		{
			desc: "repositories_cleanup minimal duration is too low",
			changeConfig: func(cfg *Config) {
				cfg.RepositoriesCleanup.RunInterval = config.Duration(minimalSyncRunInterval - time.Nanosecond)
			},
			errMsg: `repositories_cleanup.run_interval is less then 1m0s, which could lead to a database performance problem`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			config := Config{
				ListenAddr:  "localhost:1234",
				Replication: DefaultReplicationConfig(),
				VirtualStorages: []*VirtualStorage{
					{Name: "default", Nodes: vs1Nodes},
					{Name: "secondary", Nodes: vs2Nodes},
				},
				Failover:            Failover{ElectionStrategy: ElectionStrategySQL},
				RepositoriesCleanup: DefaultRepositoriesCleanup(),
			}

			tc.changeConfig(&config)

			err := config.Validate()
			if tc.errMsg == "" {
				require.NoError(t, err)
				return
			}

			require.Error(t, err)
			require.Contains(t, err.Error(), tc.errMsg)
		})
	}
}

func TestConfigParsing(t *testing.T) {
	testCases := []struct {
		desc        string
		filePath    string
		expected    Config
		expectedErr error
	}{
		{
			desc:     "check all configuration values",
			filePath: "testdata/config.toml",
			expected: Config{
				TLSListenAddr: "0.0.0.0:2306",
				TLS: config.TLS{
					CertPath: "/home/git/cert.cert",
					KeyPath:  "/home/git/key.pem",
				},
				Logging: log.Config{
					Level:  "info",
					Format: "json",
				},
				Sentry: sentry.Config{
					DSN:         "abcd123",
					Environment: "production",
				},
				VirtualStorages: []*VirtualStorage{
					{
						Name:                     "praefect",
						DefaultReplicationFactor: 2,
						Nodes: []*Node{
							{
								Address: "tcp://gitaly-internal-1.example.com",
								Storage: "praefect-internal-1",
							},
							{
								Address: "tcp://gitaly-internal-2.example.com",
								Storage: "praefect-internal-2",
							},
							{
								Address: "tcp://gitaly-internal-3.example.com",
								Storage: "praefect-internal-3",
							},
						},
					},
				},
				Prometheus: prometheus.Config{
					ScrapeTimeout:      time.Second,
					GRPCLatencyBuckets: []float64{0.1, 0.2, 0.3},
				},
				PrometheusExcludeDatabaseFromDefaultMetrics: true,
				DB: DB{
					Host:        "1.2.3.4",
					Port:        5432,
					User:        "praefect",
					Password:    "db-secret",
					DBName:      "praefect_production",
					SSLMode:     "require",
					SSLCert:     "/path/to/cert",
					SSLKey:      "/path/to/key",
					SSLRootCert: "/path/to/root-cert",
					SessionPooled: DBConnection{
						Host:        "2.3.4.5",
						Port:        6432,
						User:        "praefect_sp",
						Password:    "db-secret-sp",
						DBName:      "praefect_production_sp",
						SSLMode:     "prefer",
						SSLCert:     "/path/to/sp/cert",
						SSLKey:      "/path/to/sp/key",
						SSLRootCert: "/path/to/sp/root-cert",
					},
				},
				MemoryQueueEnabled:  true,
				GracefulStopTimeout: config.Duration(30 * time.Second),
				Reconciliation: Reconciliation{
					SchedulingInterval: config.Duration(time.Minute),
					HistogramBuckets:   []float64{1, 2, 3, 4, 5},
				},
				Replication: Replication{BatchSize: 1, ParallelStorageProcessingWorkers: 2},
				Failover: Failover{
					Enabled:                  true,
					ElectionStrategy:         ElectionStrategyPerRepository,
					ErrorThresholdWindow:     config.Duration(20 * time.Second),
					WriteErrorThresholdCount: 1500,
					ReadErrorThresholdCount:  100,
					BootstrapInterval:        config.Duration(1 * time.Second),
					MonitorInterval:          config.Duration(3 * time.Second),
				},
				RepositoriesCleanup: RepositoriesCleanup{
					CheckInterval:       config.Duration(time.Second),
					RunInterval:         config.Duration(3 * time.Second),
					RepositoriesInBatch: 10,
				},
			},
		},
		{
			desc:     "overwriting default values in the config",
			filePath: "testdata/config.overwritedefaults.toml",
			expected: Config{
				GracefulStopTimeout: config.Duration(time.Minute),
				Reconciliation: Reconciliation{
					SchedulingInterval: 0,
					HistogramBuckets:   []float64{1, 2, 3, 4, 5},
				},
				Prometheus: prometheus.DefaultConfig(),
				PrometheusExcludeDatabaseFromDefaultMetrics: true,
				Replication: Replication{BatchSize: 1, ParallelStorageProcessingWorkers: 2},
				Failover: Failover{
					Enabled:           false,
					ElectionStrategy:  "local",
					BootstrapInterval: config.Duration(5 * time.Second),
					MonitorInterval:   config.Duration(10 * time.Second),
				},
				RepositoriesCleanup: RepositoriesCleanup{
					CheckInterval:       config.Duration(time.Second),
					RunInterval:         config.Duration(4 * time.Second),
					RepositoriesInBatch: 11,
				},
			},
		},
		{
			desc:     "empty config yields default values",
			filePath: "testdata/config.empty.toml",
			expected: Config{
				GracefulStopTimeout: config.Duration(time.Minute),
				Prometheus:          prometheus.DefaultConfig(),
				PrometheusExcludeDatabaseFromDefaultMetrics: true,
				Reconciliation: DefaultReconciliationConfig(),
				Replication:    DefaultReplicationConfig(),
				Failover: Failover{
					Enabled:           true,
					ElectionStrategy:  ElectionStrategyPerRepository,
					BootstrapInterval: config.Duration(time.Second),
					MonitorInterval:   config.Duration(3 * time.Second),
				},
				RepositoriesCleanup: RepositoriesCleanup{
					CheckInterval:       config.Duration(30 * time.Minute),
					RunInterval:         config.Duration(24 * time.Hour),
					RepositoriesInBatch: 16,
				},
			},
		},
		{
			desc:        "config file does not exist",
			filePath:    "testdata/config.invalid-path.toml",
			expectedErr: os.ErrNotExist,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			cfg, err := FromFile(tc.filePath)
			require.True(t, errors.Is(err, tc.expectedErr), "actual error: %v", err)
			require.Equal(t, tc.expected, cfg)
		})
	}
}

func TestVirtualStorageNames(t *testing.T) {
	conf := Config{VirtualStorages: []*VirtualStorage{{Name: "praefect-1"}, {Name: "praefect-2"}}}
	require.Equal(t, []string{"praefect-1", "praefect-2"}, conf.VirtualStorageNames())
}

func TestStorageNames(t *testing.T) {
	conf := Config{
		VirtualStorages: []*VirtualStorage{
			{Name: "virtual-storage-1", Nodes: []*Node{{Storage: "gitaly-1"}, {Storage: "gitaly-2"}}},
			{Name: "virtual-storage-2", Nodes: []*Node{{Storage: "gitaly-3"}, {Storage: "gitaly-4"}}},
		},
	}
	require.Equal(t, map[string][]string{
		"virtual-storage-1": {"gitaly-1", "gitaly-2"},
		"virtual-storage-2": {"gitaly-3", "gitaly-4"},
	}, conf.StorageNames())
}

func TestDefaultReplicationFactors(t *testing.T) {
	for _, tc := range []struct {
		desc                      string
		virtualStorages           []*VirtualStorage
		defaultReplicationFactors map[string]int
	}{
		{
			desc: "replication factors set on some",
			virtualStorages: []*VirtualStorage{
				{Name: "virtual-storage-1", DefaultReplicationFactor: 0},
				{Name: "virtual-storage-2", DefaultReplicationFactor: 1},
			},
			defaultReplicationFactors: map[string]int{
				"virtual-storage-1": 0,
				"virtual-storage-2": 1,
			},
		},
		{
			desc:                      "returns always initialized map",
			virtualStorages:           []*VirtualStorage{},
			defaultReplicationFactors: map[string]int{},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			require.Equal(t,
				tc.defaultReplicationFactors,
				Config{VirtualStorages: tc.virtualStorages}.DefaultReplicationFactors(),
			)
		})
	}
}

func TestNeedsSQL(t *testing.T) {
	testCases := []struct {
		desc     string
		config   Config
		expected bool
	}{
		{
			desc:     "default",
			config:   Config{},
			expected: true,
		},
		{
			desc:     "Memory queue enabled",
			config:   Config{MemoryQueueEnabled: true},
			expected: false,
		},
		{
			desc:     "Failover enabled with default election strategy",
			config:   Config{Failover: Failover{Enabled: true}},
			expected: true,
		},
		{
			desc:     "Failover enabled with SQL election strategy",
			config:   Config{Failover: Failover{Enabled: true, ElectionStrategy: ElectionStrategyPerRepository}},
			expected: true,
		},
		{
			desc:     "Both PostgresQL and SQL election strategy enabled",
			config:   Config{Failover: Failover{Enabled: true, ElectionStrategy: ElectionStrategyPerRepository}},
			expected: true,
		},
		{
			desc:     "Both PostgresQL and SQL election strategy enabled but failover disabled",
			config:   Config{Failover: Failover{Enabled: false, ElectionStrategy: ElectionStrategyPerRepository}},
			expected: true,
		},
		{
			desc:     "Both PostgresQL and per_repository election strategy enabled but failover disabled",
			config:   Config{Failover: Failover{Enabled: false, ElectionStrategy: ElectionStrategyPerRepository}},
			expected: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			require.Equal(t, tc.expected, tc.config.NeedsSQL())
		})
	}
}

func TestSerialization(t *testing.T) {
	out := &bytes.Buffer{}
	encoder := toml.NewEncoder(out)

	t.Run("completely empty", func(t *testing.T) {
		out.Reset()
		require.NoError(t, encoder.Encode(Config{}))
		require.Empty(t, out.Bytes())
	})

	t.Run("partially set", func(t *testing.T) {
		out.Reset()
		require.NoError(t, encoder.Encode(Config{ListenAddr: "localhost:5640"}))
		require.Equal(t, "listen_addr = \"localhost:5640\"\n", out.String())
	})
}
