package config

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/config/log"
	gitaly_prometheus "gitlab.com/gitlab-org/gitaly/internal/config/prometheus"
	"gitlab.com/gitlab-org/gitaly/internal/config/sentry"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/models"
)

func TestConfigValidation(t *testing.T) {
	nodes := []*models.Node{
		{Storage: "internal-1", Address: "localhost:23456", Token: "secret-token", DefaultPrimary: true},
		{Storage: "internal-2", Address: "localhost:23457", Token: "secret-token"},
		{Storage: "internal-3", Address: "localhost:23458", Token: "secret-token"},
	}

	testCases := []struct {
		desc   string
		config Config
		err    error
	}{
		{
			desc:   "No ListenAddr or SocketPath",
			config: Config{ListenAddr: "", VirtualStorages: []*VirtualStorage{&VirtualStorage{Nodes: nodes}}},
			err:    errNoListener,
		},
		{
			desc:   "Only a SocketPath",
			config: Config{SocketPath: "/tmp/praefect.socket", VirtualStorages: []*VirtualStorage{&VirtualStorage{Nodes: nodes}}},
			err:    nil,
		},
		{
			desc:   "No servers",
			config: Config{ListenAddr: "localhost:1234"},
			err:    errNoVirtualStorages,
		},
		{
			desc: "duplicate storage",
			config: Config{
				ListenAddr: "localhost:1234",
				VirtualStorages: []*VirtualStorage{
					&VirtualStorage{Nodes: append(nodes, &models.Node{Storage: nodes[0].Storage, Address: nodes[1].Address})},
				},
			},
			err: errDuplicateStorage,
		},
		{
			desc:   "Valid config",
			config: Config{ListenAddr: "localhost:1234", VirtualStorages: []*VirtualStorage{&VirtualStorage{Nodes: nodes}}},
			err:    nil,
		},
		{
			desc:   "No designated primaries",
			config: Config{ListenAddr: "localhost:1234", VirtualStorages: []*VirtualStorage{&VirtualStorage{Nodes: nodes[1:]}}},
			err:    errNoPrimaries,
		},
		{
			desc: "More than 1 primary",
			config: Config{
				ListenAddr: "localhost:1234",
				VirtualStorages: []*VirtualStorage{
					&VirtualStorage{
						Nodes: append(nodes,
							&models.Node{
								Storage:        "internal-4",
								Address:        "localhost:23459",
								Token:          "secret-token",
								DefaultPrimary: true,
							}),
					},
				},
			},
			err: errMoreThanOnePrimary,
		},
		{
			desc: "Node storage not unique",
			config: Config{
				ListenAddr: "localhost:1234",
				VirtualStorages: []*VirtualStorage{
					&VirtualStorage{Name: "default", Nodes: nodes},
					&VirtualStorage{
						Name: "backup",
						Nodes: []*models.Node{
							&models.Node{
								Storage:        nodes[0].Storage,
								Address:        "some.other.address",
								DefaultPrimary: true},
						},
					},
				},
			},
			err: errStorageAddressMismatch,
		},
		{
			desc: "Node storage not unique",
			config: Config{
				ListenAddr: "localhost:1234",
				VirtualStorages: []*VirtualStorage{
					&VirtualStorage{Name: "default", Nodes: nodes},
					&VirtualStorage{
						Name: "default",
						Nodes: []*models.Node{
							&models.Node{
								Storage:        nodes[0].Storage,
								Address:        "some.other.address",
								DefaultPrimary: true}},
					},
				},
			},
			err: errVirtualStoragesNotUnique,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			err := tc.config.Validate()
			if tc.err == nil {
				assert.NoError(t, err)
				return
			}

			assert.True(t, strings.Contains(err.Error(), tc.err.Error()))
		})
	}
}

func TestConfigParsing(t *testing.T) {
	testCases := []struct {
		filePath string
		expected Config
	}{
		{
			filePath: "testdata/config.toml",
			expected: Config{
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
						Nodes: []*models.Node{
							&models.Node{
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
				PostgresQueueEnabled: true,
			},
		},
		//TODO: Remove this test, as well as the fixture in testdata/single-virtual-storage.config.toml
		// once omnibus and gdk are updated with support for VirtualStorages
		{
			filePath: "testdata/single-virtual-storage.config.toml",
			expected: Config{
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
						Nodes: []*models.Node{
							&models.Node{
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
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.filePath, func(t *testing.T) {
			cfg, err := FromFile(tc.filePath)
			require.NoError(t, err)
			require.Equal(t, tc.expected, cfg)
		})
	}
}

func TestToPQString(t *testing.T) {
	testCases := []struct {
		desc string
		in   DB
		out  string
	}{
		{desc: "empty", in: DB{}, out: "binary_parameters=yes"},
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
			out: `port=2345 host=1.2.3.4 user=praefect-user password=secret dbname=praefect_production sslmode=require sslcert=/path/to/cert sslkey=/path/to/key sslrootcert=/path/to/root-cert binary_parameters=yes`,
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
			expected: false,
		},
		{
			desc:     "PostgreSQL queue enabled",
			config:   Config{PostgresQueueEnabled: true},
			expected: true,
		},
		{
			desc:     "Failover enabled with default election strategy",
			config:   Config{Failover: Failover{Enabled: true}},
			expected: false,
		},
		{
			desc:     "Failover enabled with SQL election strategy",
			config:   Config{Failover: Failover{Enabled: true, ElectionStrategy: "sql"}},
			expected: true,
		},
		{
			desc:     "Both PostgresQL and SQL election strategy enabled",
			config:   Config{PostgresQueueEnabled: true, Failover: Failover{Enabled: true, ElectionStrategy: "sql"}},
			expected: true,
		},
		{
			desc:     "Both PostgresQL and SQL election strategy enabled but failover disabled",
			config:   Config{PostgresQueueEnabled: true, Failover: Failover{Enabled: false, ElectionStrategy: "sql"}},
			expected: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			require.Equal(t, tc.expected, tc.config.NeedsSQL())
		})
	}
}
