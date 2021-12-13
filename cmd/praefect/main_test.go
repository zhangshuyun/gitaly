package main

import (
	"errors"
	"fmt"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/bootstrap"
	"gitlab.com/gitlab-org/gitaly/v14/internal/bootstrap/starter"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/datastore"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testdb"
)

func TestMain(m *testing.M) {
	testhelper.Run(m)
}

func TestNoConfigFlag(t *testing.T) {
	_, err := initConfig()

	assert.Equal(t, err, errNoConfigFile)
}

func TestGetStarterConfigs(t *testing.T) {
	for _, tc := range []struct {
		desc   string
		conf   config.Config
		exp    []starter.Config
		expErr error
	}{
		{
			desc:   "no addresses",
			expErr: errors.New("no listening addresses were provided, unable to start"),
		},
		{
			desc: "addresses without schema",
			conf: config.Config{
				ListenAddr:    "127.0.0.1:2306",
				TLSListenAddr: "127.0.0.1:2307",
				SocketPath:    "/socket/path",
			},
			exp: []starter.Config{
				{
					Name:              starter.TCP,
					Addr:              "127.0.0.1:2306",
					HandoverOnUpgrade: true,
				},
				{
					Name:              starter.TLS,
					Addr:              "127.0.0.1:2307",
					HandoverOnUpgrade: true,
				},
				{
					Name:              starter.Unix,
					Addr:              "/socket/path",
					HandoverOnUpgrade: true,
				},
			},
		},
		{
			desc: "addresses with schema",
			conf: config.Config{
				ListenAddr:    "tcp://127.0.0.1:2306",
				TLSListenAddr: "tls://127.0.0.1:2307",
				SocketPath:    "unix:///socket/path",
			},
			exp: []starter.Config{
				{
					Name:              starter.TCP,
					Addr:              "127.0.0.1:2306",
					HandoverOnUpgrade: true,
				},
				{
					Name:              starter.TLS,
					Addr:              "127.0.0.1:2307",
					HandoverOnUpgrade: true,
				},
				{
					Name:              starter.Unix,
					Addr:              "/socket/path",
					HandoverOnUpgrade: true,
				},
			},
		},
		{
			desc: "addresses without schema",
			conf: config.Config{
				ListenAddr:    "127.0.0.1:2306",
				TLSListenAddr: "127.0.0.1:2307",
				SocketPath:    "/socket/path",
			},
			exp: []starter.Config{
				{
					Name:              starter.TCP,
					Addr:              "127.0.0.1:2306",
					HandoverOnUpgrade: true,
				},
				{
					Name:              starter.TLS,
					Addr:              "127.0.0.1:2307",
					HandoverOnUpgrade: true,
				},
				{
					Name:              starter.Unix,
					Addr:              "/socket/path",
					HandoverOnUpgrade: true,
				},
			},
		},
		{
			desc: "addresses with/without schema",
			conf: config.Config{
				ListenAddr:    "127.0.0.1:2306",
				TLSListenAddr: "tls://127.0.0.1:2307",
				SocketPath:    "unix:///socket/path",
			},
			exp: []starter.Config{
				{
					Name:              starter.TCP,
					Addr:              "127.0.0.1:2306",
					HandoverOnUpgrade: true,
				},
				{
					Name:              starter.TLS,
					Addr:              "127.0.0.1:2307",
					HandoverOnUpgrade: true,
				},
				{
					Name:              starter.Unix,
					Addr:              "/socket/path",
					HandoverOnUpgrade: true,
				},
			},
		},
		{
			desc: "secure and insecure can't be the same",
			conf: config.Config{
				ListenAddr:    "127.0.0.1:2306",
				TLSListenAddr: "127.0.0.1:2306",
			},
			expErr: errors.New(`same address can't be used for different schemas "127.0.0.1:2306"`),
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			actual, err := getStarterConfigs(tc.conf)
			require.Equal(t, tc.expErr, err)
			require.ElementsMatch(t, tc.exp, actual)
		})
	}
}

func newMockRegisterer() *mockRegisterer {
	return &mockRegisterer{}
}

type mockRegisterer struct {
	repositoryCollectorRegistered bool
}

func (m *mockRegisterer) Register(c prometheus.Collector) error {
	if _, ok := c.(*datastore.RepositoryStoreCollector); ok {
		m.repositoryCollectorRegistered = true
	}

	return nil
}

func (m *mockRegisterer) MustRegister(collectors ...prometheus.Collector) {
	for _, c := range collectors {
		if _, ok := c.(*datastore.RepositoryStoreCollector); ok {
			m.repositoryCollectorRegistered = true
		}
	}
}

func (m *mockRegisterer) Unregister(c prometheus.Collector) bool {
	return false
}

func (m *mockRegisterer) Gather() ([]*dto.MetricFamily, error) {
	return nil, nil
}

func TestExcludeDatabaseMetricsFromDefaultMetrics(t *testing.T) {
	t.Parallel()

	db := testdb.NewDB(t)
	dbConf := testdb.GetDBConfig(t, db.Name)

	conf := config.Config{
		SocketPath: testhelper.GetTemporaryGitalySocketFileName(t),
		VirtualStorages: []*config.VirtualStorage{
			{
				Name: "praefect",
				Nodes: []*config.Node{
					{Storage: "storage-0", Address: "tcp://someaddress"},
				},
			},
		},
		DB: dbConf,
		Failover: config.Failover{
			Enabled:          true,
			ElectionStrategy: config.ElectionStrategyPerRepository,
		},
		MemoryQueueEnabled: true,
	}

	starterConfigs, err := getStarterConfigs(conf)
	require.NoError(t, err)

	excludeDatabaseMetricsSettings := []bool{true, false}
	for _, excludeDatabaseMetrics := range excludeDatabaseMetricsSettings {
		t.Run(fmt.Sprintf("exclude database metrics %v", excludeDatabaseMetrics), func(t *testing.T) {
			conf.PrometheusExcludeDatabaseFromDefaultMetrics = excludeDatabaseMetrics

			stopped := make(chan struct{})
			bootstrapper := bootstrap.NewNoop()

			metricRegisterer, dbMetricsRegisterer := newMockRegisterer(), newMockRegisterer()
			go func() {
				defer close(stopped)
				assert.NoError(t, run(starterConfigs, conf, bootstrapper, metricRegisterer, dbMetricsRegisterer))
			}()

			bootstrapper.Terminate()
			<-stopped

			assert.True(t, dbMetricsRegisterer.repositoryCollectorRegistered)
			assert.Equal(t, excludeDatabaseMetrics, !metricRegisterer.repositoryCollectorRegistered)
		})
	}
}
