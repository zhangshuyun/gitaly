package config

import (
	"errors"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/config"
	"gitlab.com/gitlab-org/gitaly/internal/config/log"
	gitaly_prometheus "gitlab.com/gitlab-org/gitaly/internal/config/prometheus"
	"gitlab.com/gitlab-org/gitaly/internal/config/sentry"
)

func TestConfigValidation(t *testing.T) {
	vs1Nodes := []*Node{
		{Storage: "internal-1.0", Address: "localhost:23456", Token: "secret-token-1", DefaultPrimary: true},
		{Storage: "internal-2.0", Address: "localhost:23457", Token: "secret-token-1"},
		{Storage: "internal-3.0", Address: "localhost:23458", Token: "secret-token-1"},
	}

	vs2Nodes := []*Node{
		// storage can have same name as storage in another virtual storage, but all addresses must be unique
		{Storage: "internal-1.0", Address: "localhost:33456", Token: "secret-token-2", DefaultPrimary: true},
		{Storage: "internal-2.1", Address: "localhost:33457", Token: "secret-token-2"},
		{Storage: "internal-3.1", Address: "localhost:33458", Token: "secret-token-2"},
	}

	testCases := []struct {
		desc   string
		config Config
		errMsg string
	}{
		{
			desc: "Valid config with ListenAddr",
			config: Config{
				ListenAddr: "localhost:1234",
				VirtualStorages: []*VirtualStorage{
					{Name: "default", Nodes: vs1Nodes},
					{Name: "secondary", Nodes: vs2Nodes},
				},
			},
		},
		{
			desc: "Valid config with TLSListenAddr",
			config: Config{
				TLSListenAddr: "tls://localhost:4321",
				VirtualStorages: []*VirtualStorage{
					{Name: "default", Nodes: vs1Nodes},
				},
			},
		},
		{
			desc: "Valid config with SocketPath",
			config: Config{
				SocketPath: "/tmp/praefect.socket",
				VirtualStorages: []*VirtualStorage{
					{Name: "default", Nodes: vs1Nodes},
				},
			},
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
			},
			errMsg: "no listen address or socket path configured",
		},
		{
			desc:   "No virtual storages",
			config: Config{ListenAddr: "localhost:1234"},
			errMsg: "no virtual storages configured",
		},
		{
			desc: "duplicate storage",
			config: Config{
				ListenAddr: "localhost:1234",
				VirtualStorages: []*VirtualStorage{
					{
						Name: "default",
						Nodes: append(vs1Nodes, &Node{
							Storage: vs1Nodes[0].Storage,
							Address: vs1Nodes[1].Address,
						}),
					},
				},
			},
			errMsg: `virtual storage "default": internal gitaly storages are not unique`,
		},
		{
			desc: "No designated primaries",
			config: Config{
				ListenAddr: "localhost:1234",
				VirtualStorages: []*VirtualStorage{
					{Name: "default", Nodes: vs1Nodes[1:]},
				},
			},
			errMsg: `virtual storage "default": no primaries designated`,
		},
		{
			desc: "More than 1 primary",
			config: Config{
				ListenAddr: "localhost:1234",
				VirtualStorages: []*VirtualStorage{
					{
						Name: "default",
						Nodes: append(vs1Nodes,
							&Node{
								Storage:        "internal-4",
								Address:        "localhost:23459",
								Token:          "secret-token",
								DefaultPrimary: true,
							}),
					},
				},
			},
			errMsg: `virtual storage "default": only 1 node can be designated as a primary`,
		},
		{
			desc: "Node storage has no name",
			config: Config{
				ListenAddr: "localhost:1234",
				VirtualStorages: []*VirtualStorage{
					{
						Name: "default",
						Nodes: []*Node{
							{
								Storage:        "",
								Address:        "localhost:23456",
								Token:          "secret-token-1",
								DefaultPrimary: true,
							},
						},
					},
				},
			},
			errMsg: `virtual storage "default": all gitaly nodes must have a storage`,
		},
		{
			desc: "Node storage has no address",
			config: Config{
				ListenAddr: "localhost:1234",
				VirtualStorages: []*VirtualStorage{
					{
						Name: "default",
						Nodes: []*Node{
							{
								Storage:        "internal",
								Address:        "",
								Token:          "secret-token-1",
								DefaultPrimary: true,
							},
						},
					},
				},
			},
			errMsg: `virtual storage "default": all gitaly nodes must have an address`,
		},
		{
			desc: "Virtual storage has no name",
			config: Config{
				ListenAddr: "localhost:1234",
				VirtualStorages: []*VirtualStorage{
					{Name: "", Nodes: vs1Nodes},
				},
			},
			errMsg: `virtual storages must have a name`,
		},
		{
			desc: "Virtual storage not unique",
			config: Config{
				ListenAddr: "localhost:1234",
				VirtualStorages: []*VirtualStorage{
					{Name: "default", Nodes: vs1Nodes},
					{Name: "default", Nodes: vs2Nodes},
				},
			},
			errMsg: `virtual storage "default": virtual storages must have unique names`,
		},
		{
			desc: "Virtual storage has no nodes",
			config: Config{
				ListenAddr: "localhost:1234",
				VirtualStorages: []*VirtualStorage{
					{Name: "default", Nodes: vs1Nodes},
					{Name: "secondary", Nodes: nil},
				},
			},
			errMsg: `virtual storage "secondary": no primary gitaly backends configured`,
		},
		{
			desc: "Node storage has address duplicate",
			config: Config{
				ListenAddr: "localhost:1234",
				VirtualStorages: []*VirtualStorage{
					{Name: "default", Nodes: vs1Nodes},
					{Name: "secondary", Nodes: append(vs2Nodes, vs1Nodes[1])},
				},
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
								Address:        "tcp://gitaly-internal-1.example.com",
								Storage:        "praefect-internal-1",
								DefaultPrimary: true,
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
				},
				MemoryQueueEnabled:  true,
				GracefulStopTimeout: config.Duration(30 * time.Second),
				Failover: Failover{
					Enabled:                  true,
					ElectionStrategy:         sqlFailoverValue,
					ReadOnlyAfterFailover:    true,
					ErrorThresholdWindow:     config.Duration(20 * time.Second),
					WriteErrorThresholdCount: 1500,
					ReadErrorThresholdCount:  100,
				},
			},
		},
		{
			desc:     "overwriting default values in the config",
			filePath: "testdata/config.overwritedefaults.toml",
			expected: Config{
				GracefulStopTimeout: config.Duration(time.Minute),
				Failover: Failover{
					Enabled:               false,
					ElectionStrategy:      "local",
					ReadOnlyAfterFailover: false,
				},
			},
		},
		{
			desc:     "empty config yields default values",
			filePath: "testdata/config.empty.toml",
			expected: Config{
				GracefulStopTimeout: config.Duration(time.Minute),
				Failover: Failover{
					Enabled:               true,
					ElectionStrategy:      sqlFailoverValue,
					ReadOnlyAfterFailover: true,
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
			require.Equal(t, tc.expected, cfg)
			require.True(t, errors.Is(err, tc.expectedErr), "actual error: %v", err)
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
		desc string
		in   DB
		out  string
	}{
		{desc: "empty", in: DB{}, out: "statement_timeout=0 binary_parameters=yes"},
		{
			desc: "basic example",
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
			out: `statement_timeout=0 port=2345 host=1.2.3.4 user=praefect-user password=secret dbname=praefect_production sslmode=require sslcert=/path/to/cert sslkey=/path/to/key sslrootcert=/path/to/root-cert binary_parameters=yes`,
		},
		{
			desc: "with spaces and quotes",
			in: DB{
				Password: "secret foo'bar",
			},
			out: `statement_timeout=0 password=secret\ foo\'bar binary_parameters=yes`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			require.Equal(t, tc.out, tc.in.ToPQString())
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
			config:   Config{Failover: Failover{Enabled: true, ElectionStrategy: "sql"}},
			expected: true,
		},
		{
			desc:     "Both PostgresQL and SQL election strategy enabled",
			config:   Config{Failover: Failover{Enabled: true, ElectionStrategy: "sql"}},
			expected: true,
		},
		{
			desc:     "Both PostgresQL and SQL election strategy enabled but failover disabled",
			config:   Config{Failover: Failover{Enabled: false, ElectionStrategy: "sql"}},
			expected: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			require.Equal(t, tc.expected, tc.config.NeedsSQL())
		})
	}
}
