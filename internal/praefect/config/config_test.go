package config

import (
	"errors"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config/log"
	gitaly_prometheus "gitlab.com/gitlab-org/gitaly/internal/gitaly/config/prometheus"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config/sentry"
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

	fromValidConfig := func(changeFunc func(*Config)) Config {
		cfg := Config{
			ListenAddr:  "localhost:1234",
			Replication: DefaultReplicationConfig(),
			VirtualStorages: []*VirtualStorage{
				{Name: "default", Nodes: vs1Nodes},
				{Name: "secondary", Nodes: vs2Nodes},
			},
			Failover: Failover{ElectionStrategy: ElectionStrategySQL},
		}

		changeFunc(&cfg)

		return cfg
	}

	testCases := []struct {
		desc   string
		config Config
		errMsg string
	}{
		{
			desc:   "Valid config with ListenAddr",
			config: fromValidConfig(func(*Config) {}),
		},
		{
			desc: "Valid config with local elector",
			config: fromValidConfig(func(cfg *Config) {
				cfg.Failover.ElectionStrategy = ElectionStrategyLocal
			}),
		},
		{
			desc: "Valid config with per repository elector",
			config: fromValidConfig(func(cfg *Config) {
				cfg.Failover.ElectionStrategy = ElectionStrategyPerRepository
			}),
		},
		{
			desc: "Invalid election strategy",
			config: fromValidConfig(func(cfg *Config) {
				cfg.Failover.ElectionStrategy = "invalid-strategy"
			}),
			errMsg: `invalid election strategy: "invalid-strategy"`,
		},
		{
			desc: "Valid config with TLSListenAddr",
			config: Config{
				TLSListenAddr: "tls://localhost:4321",
				Replication:   DefaultReplicationConfig(),
				VirtualStorages: []*VirtualStorage{
					{Name: "default", Nodes: vs1Nodes},
				},
				Failover: Failover{ElectionStrategy: ElectionStrategySQL},
			},
		},
		{
			desc: "Valid config with SocketPath",
			config: Config{
				SocketPath:  "/tmp/praefect.socket",
				Replication: DefaultReplicationConfig(),
				VirtualStorages: []*VirtualStorage{
					{Name: "default", Nodes: vs1Nodes},
				},
				Failover: Failover{ElectionStrategy: ElectionStrategySQL},
			},
		},
		{
			desc: "Invalid replication batch size",
			config: Config{
				SocketPath:  "/tmp/praefect.socket",
				Replication: Replication{BatchSize: 0},
				VirtualStorages: []*VirtualStorage{
					{Name: "default", Nodes: vs1Nodes},
				},
				Failover: Failover{ElectionStrategy: ElectionStrategySQL},
			},
			errMsg: "replication batch size was 0 but must be >=1",
		},
		{
			desc: "No ListenAddr or SocketPath or TLSListenAddr",
			config: Config{
				ListenAddr:    "",
				TLSListenAddr: "",
				SocketPath:    "",
				VirtualStorages: []*VirtualStorage{
					{Name: "default", Nodes: vs1Nodes},
				},
				Replication: DefaultReplicationConfig(),
				Failover:    Failover{ElectionStrategy: ElectionStrategySQL},
			},
			errMsg: "no listen address or socket path configured",
		},
		{
			desc: "No virtual storages",
			config: Config{
				ListenAddr:  "localhost:1234",
				Replication: DefaultReplicationConfig(),
				Failover:    Failover{ElectionStrategy: ElectionStrategySQL},
			},
			errMsg: "no virtual storages configured",
		},
		{
			desc: "duplicate storage",
			config: Config{
				ListenAddr:  "localhost:1234",
				Replication: DefaultReplicationConfig(),
				VirtualStorages: []*VirtualStorage{
					{
						Name: "default",
						Nodes: append(vs1Nodes, &Node{
							Storage: vs1Nodes[0].Storage,
							Address: vs1Nodes[1].Address,
						}),
					},
				},
				Failover: Failover{ElectionStrategy: ElectionStrategySQL},
			},
			errMsg: `virtual storage "default": internal gitaly storages are not unique`,
		},
		{
			desc: "Node storage has no name",
			config: Config{
				ListenAddr:  "localhost:1234",
				Replication: DefaultReplicationConfig(),
				VirtualStorages: []*VirtualStorage{
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
				},
				Failover: Failover{ElectionStrategy: ElectionStrategySQL},
			},
			errMsg: `virtual storage "default": all gitaly nodes must have a storage`,
		},
		{
			desc: "Node storage has no address",
			config: Config{
				ListenAddr:  "localhost:1234",
				Replication: DefaultReplicationConfig(),
				VirtualStorages: []*VirtualStorage{
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
				},
				Failover: Failover{ElectionStrategy: ElectionStrategySQL},
			},
			errMsg: `virtual storage "default": all gitaly nodes must have an address`,
		},
		{
			desc: "Virtual storage has no name",
			config: Config{
				ListenAddr:  "localhost:1234",
				Replication: DefaultReplicationConfig(),
				VirtualStorages: []*VirtualStorage{
					{Name: "", Nodes: vs1Nodes},
				},
				Failover: Failover{ElectionStrategy: ElectionStrategySQL},
			},
			errMsg: `virtual storages must have a name`,
		},
		{
			desc: "Virtual storage not unique",
			config: Config{
				ListenAddr:  "localhost:1234",
				Replication: DefaultReplicationConfig(),
				VirtualStorages: []*VirtualStorage{
					{Name: "default", Nodes: vs1Nodes},
					{Name: "default", Nodes: vs2Nodes},
				},
				Failover: Failover{ElectionStrategy: ElectionStrategySQL},
			},
			errMsg: `virtual storage "default": virtual storages must have unique names`,
		},
		{
			desc: "Virtual storage has no nodes",
			config: Config{
				ListenAddr:  "localhost:1234",
				Replication: DefaultReplicationConfig(),
				VirtualStorages: []*VirtualStorage{
					{Name: "default", Nodes: vs1Nodes},
					{Name: "secondary", Nodes: nil},
				},
				Failover: Failover{ElectionStrategy: ElectionStrategySQL},
			},
			errMsg: `virtual storage "secondary": no primary gitaly backends configured`,
		},
		{
			desc: "Node storage has address duplicate",
			config: Config{
				ListenAddr:  "localhost:1234",
				Replication: DefaultReplicationConfig(),
				VirtualStorages: []*VirtualStorage{
					{Name: "default", Nodes: vs1Nodes},
					{Name: "secondary", Nodes: append(vs2Nodes, vs1Nodes[1])},
				},
				Failover: Failover{ElectionStrategy: ElectionStrategySQL},
			},
			errMsg: `multiple storages have the same address`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			err := tc.config.Validate()
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
					&VirtualStorage{
						Name: "praefect",
						Nodes: []*Node{
							&Node{
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
				Prometheus: gitaly_prometheus.Config{
					GRPCLatencyBuckets: []float64{0.1, 0.2, 0.3},
				},
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
					HostNoProxy: "2.3.4.5",
					PortNoProxy: 6432,
				},
				MemoryQueueEnabled:  true,
				GracefulStopTimeout: config.Duration(30 * time.Second),
				Reconciliation: Reconciliation{
					SchedulingInterval: config.Duration(time.Minute),
					HistogramBuckets:   []float64{1, 2, 3, 4, 5},
				},
				Replication: Replication{BatchSize: 1},
				Failover: Failover{
					Enabled:                  true,
					ElectionStrategy:         ElectionStrategySQL,
					ErrorThresholdWindow:     config.Duration(20 * time.Second),
					WriteErrorThresholdCount: 1500,
					ReadErrorThresholdCount:  100,
					BootstrapInterval:        config.Duration(1 * time.Second),
					MonitorInterval:          config.Duration(3 * time.Second),
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
				Replication: Replication{BatchSize: 1},
				Failover: Failover{
					Enabled:           false,
					ElectionStrategy:  "local",
					BootstrapInterval: config.Duration(5 * time.Second),
					MonitorInterval:   config.Duration(10 * time.Second),
				},
			},
		},
		{
			desc:     "empty config yields default values",
			filePath: "testdata/config.empty.toml",
			expected: Config{
				GracefulStopTimeout: config.Duration(time.Minute),
				Reconciliation:      DefaultReconciliationConfig(),
				Replication:         DefaultReplicationConfig(),
				Failover: Failover{
					Enabled:           true,
					ElectionStrategy:  ElectionStrategySQL,
					BootstrapInterval: config.Duration(time.Second),
					MonitorInterval:   config.Duration(3 * time.Second),
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
		}}
	require.Equal(t, map[string][]string{
		"virtual-storage-1": {"gitaly-1", "gitaly-2"},
		"virtual-storage-2": {"gitaly-3", "gitaly-4"},
	}, conf.StorageNames())
}

func TestToPQString(t *testing.T) {
	testCases := []struct {
		desc   string
		in     DB
		direct bool
		out    string
	}{
		{desc: "empty", in: DB{}, out: "binary_parameters=yes"},
		{
			desc: "proxy connection",
			in: DB{
				Host:        "1.2.3.4",
				Port:        2345,
				User:        "praefect-user",
				Password:    "secret",
				DBName:      "praefect_production",
				SSLMode:     "require",
				SSLCert:     "/path/to/cert",
				SSLKey:      "/path/to/key",
				SSLRootCert: "/path/to/root-cert",
			},
			direct: false,
			out:    `port=2345 host=1.2.3.4 user=praefect-user password=secret dbname=praefect_production sslmode=require sslcert=/path/to/cert sslkey=/path/to/key sslrootcert=/path/to/root-cert binary_parameters=yes`,
		},
		{
			desc: "direct connection",
			in: DB{
				HostNoProxy: "1.2.3.4",
				PortNoProxy: 2345,
				User:        "praefect-user",
				Password:    "secret",
				DBName:      "praefect_production",
				SSLMode:     "require",
				SSLCert:     "/path/to/cert",
				SSLKey:      "/path/to/key",
				SSLRootCert: "/path/to/root-cert",
			},
			direct: true,
			out:    `port=2345 host=1.2.3.4 user=praefect-user password=secret dbname=praefect_production sslmode=require sslcert=/path/to/cert sslkey=/path/to/key sslrootcert=/path/to/root-cert binary_parameters=yes`,
		},
		{
			desc: "direct connection host not set",
			in: DB{
				HostNoProxy: "",
				PortNoProxy: 2345,
			},
			direct: true,
			out:    "",
		},
		{
			desc: "direct connection port not set",
			in: DB{
				HostNoProxy: "localhost",
				PortNoProxy: 0,
			},
			direct: true,
			out:    "",
		},
		{
			desc: "with spaces and quotes",
			in: DB{
				Password: "secret foo'bar",
			},
			out: `password=secret\ foo\'bar binary_parameters=yes`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			require.Equal(t, tc.out, tc.in.ToPQString(tc.direct))
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
