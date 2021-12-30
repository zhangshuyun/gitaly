package praefect

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/datastore"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/datastore/migrations"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/nodes"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testdb"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
)

func TestPraefectMigrations_success(t *testing.T) {
	testCases := []struct {
		desc        string
		prepare     func(cfg config.Config) error
		expectedErr error
	}{
		{
			desc: "no migrations have run",
			prepare: func(cfg config.Config) error {
				_, err := datastore.MigrateDown(cfg, len(migrations.All()))
				if err != nil {
					return err
				}

				return nil
			},
			expectedErr: fmt.Errorf("%d migrations have not been run", len(migrations.All())),
		},
		{
			desc: "some migrations have run",
			prepare: func(cfg config.Config) error {
				_, err := datastore.MigrateDown(cfg, 3)
				if err != nil {
					return err
				}

				return nil
			},
			expectedErr: fmt.Errorf("3 migrations have not been run"),
		},
		{
			desc: "all migrations have run",
			prepare: func(cfg config.Config) error {
				return nil
			},
			expectedErr: nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			ctx, cancel := testhelper.Context()
			defer cancel()

			var cfg config.Config
			db := testdb.New(t)
			cfg.DB = testdb.GetConfig(t, db.Name)

			require.NoError(t, tc.prepare(cfg))

			migrationCheck := NewPraefectMigrationCheck(cfg, io.Discard, false)
			assert.Equal(t, "praefect migrations", migrationCheck.Name)
			assert.Equal(t, "confirms whether or not all praefect migrations have run", migrationCheck.Description)
			assert.Equal(t, tc.expectedErr, migrationCheck.Run(ctx))
		})
	}
}

type nodeAssertion struct {
	storage                         string
	token                           string
	servingStatus                   grpc_health_v1.HealthCheckResponse_ServingStatus
	serverReadable, serverWriteable bool
}

type mockServerServer struct {
	gitalypb.UnimplementedServerServiceServer
	node nodeAssertion
}

func (m *mockServerServer) ServerInfo(ctx context.Context, in *gitalypb.ServerInfoRequest) (*gitalypb.ServerInfoResponse, error) {
	return &gitalypb.ServerInfoResponse{
		StorageStatuses: []*gitalypb.ServerInfoResponse_StorageStatus{
			{
				StorageName: m.node.storage,
				Readable:    m.node.serverReadable,
				Writeable:   m.node.serverWriteable,
			},
		},
	}, nil
}

func TestGitalyNodeConnectivityCheck(t *testing.T) {
	testCases := []struct {
		desc      string
		expectErr bool
		nodes     []nodeAssertion
	}{
		{
			desc:      "all nodes are healthy",
			expectErr: false,
			nodes: []nodeAssertion{
				{
					storage:         "storage-0",
					token:           "token-0",
					servingStatus:   grpc_health_v1.HealthCheckResponse_SERVING,
					serverReadable:  true,
					serverWriteable: true,
				},
				{
					storage:         "storage-1",
					token:           "token-1",
					servingStatus:   grpc_health_v1.HealthCheckResponse_SERVING,
					serverReadable:  true,
					serverWriteable: true,
				},
			},
		},
		{
			desc:      "one node failed healthcheck",
			expectErr: true,
			nodes: []nodeAssertion{
				{
					storage:         "storage-0",
					token:           "token-0",
					servingStatus:   grpc_health_v1.HealthCheckResponse_SERVING,
					serverReadable:  true,
					serverWriteable: true,
				},
				{
					storage:         "storage-1",
					token:           "token-1",
					servingStatus:   grpc_health_v1.HealthCheckResponse_NOT_SERVING,
					serverReadable:  true,
					serverWriteable: true,
				},
			},
		},
		{
			desc:      "one node failed consistency check",
			expectErr: true,
			nodes: []nodeAssertion{
				{
					storage:         "storage-0",
					token:           "token-0",
					servingStatus:   grpc_health_v1.HealthCheckResponse_SERVING,
					serverReadable:  false,
					serverWriteable: true,
				},
				{
					storage:         "storage-1",
					token:           "token-1",
					servingStatus:   grpc_health_v1.HealthCheckResponse_SERVING,
					serverReadable:  true,
					serverWriteable: true,
				},
			},
		},
		{
			desc:      "all nodes failed",
			expectErr: true,
			nodes: []nodeAssertion{
				{
					storage:         "storage-0",
					token:           "token-0",
					servingStatus:   grpc_health_v1.HealthCheckResponse_NOT_SERVING,
					serverReadable:  false,
					serverWriteable: true,
				},
				{
					storage:         "storage-1",
					token:           "token-1",
					servingStatus:   grpc_health_v1.HealthCheckResponse_NOT_SERVING,
					serverReadable:  true,
					serverWriteable: false,
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			cfgNodes, cleanup := runNodes(t, tc.nodes)
			defer cleanup()
			check := NewGitalyNodeConnectivityCheck(
				config.Config{
					VirtualStorages: []*config.VirtualStorage{
						{
							Name:  "default",
							Nodes: cfgNodes,
						},
					},
				},
				io.Discard,
				false,
			)

			ctx, cancel := testhelper.Context()
			defer cancel()

			err := check.Run(ctx)
			if tc.expectErr {
				assert.Regexp(t, "^the following nodes are not healthy: .+", err)
				return
			}

			assert.Nil(t, err)
		})
	}

	t.Run("server not listening", func(t *testing.T) {
		tmp := testhelper.TempDir(t)
		socketAddr := fmt.Sprintf("unix://%s", filepath.Join(tmp, "storage"))
		cfgNodes := []*config.Node{
			{
				Storage: "storage",
				Token:   "token",
				Address: socketAddr,
			},
		}

		check := NewGitalyNodeConnectivityCheck(
			config.Config{
				VirtualStorages: []*config.VirtualStorage{
					{
						Name:  "default",
						Nodes: cfgNodes,
					},
				},
			},
			io.Discard,
			false,
		)

		ctx, cancel := testhelper.Context()

		// Cancel the context directly such that dialling the node will fail.
		cancel()

		require.Equal(t, &nodes.PingError{
			UnhealthyAddresses: []string{socketAddr},
		}, check.Run(ctx))
	})

	t.Run("output check details", func(t *testing.T) {
		quietSettings := []bool{true, false}
		nodes := []nodeAssertion{
			{
				storage:         "storage-0",
				token:           "token-0",
				servingStatus:   grpc_health_v1.HealthCheckResponse_SERVING,
				serverReadable:  true,
				serverWriteable: true,
			},
		}
		expectedLogLines := []string{
			"dialing...",
			"dialed successfully!",
			"checking health...",
			"SUCCESS: node is healthy!",
			"checking consistency...",
			"SUCCESS: confirmed Gitaly storage \"storage-0\" in virtual storages [default] is served",
			"SUCCESS: node configuration is consistent!",
		}

		for _, isQuiet := range quietSettings {
			var output bytes.Buffer
			cfgNodes, cleanup := runNodes(t, nodes)
			defer cleanup()
			check := NewGitalyNodeConnectivityCheck(
				config.Config{
					VirtualStorages: []*config.VirtualStorage{
						{
							Name:  "default",
							Nodes: cfgNodes,
						},
					},
				},
				&output,
				isQuiet,
			)

			ctx, cancel := testhelper.Context()
			defer cancel()

			require.NoError(t, check.Run(ctx))

			for _, logLine := range expectedLogLines {
				if isQuiet {
					assert.NotContains(t, output.String(), logLine)
					continue
				}
				assert.Contains(t, output.String(), logLine)
			}
		}
	})
}

func runNodes(t *testing.T, nodes []nodeAssertion) ([]*config.Node, func()) {
	tmp := testhelper.TempDir(t)
	var cfgNodes []*config.Node

	var cleanupFns []func()

	for _, n := range nodes {
		socket := filepath.Join(tmp, n.storage)
		ln, err := net.Listen("unix", socket)
		require.NoError(t, err)
		healthSrv := health.NewServer()
		healthSrv.SetServingStatus("", n.servingStatus)

		srvSrv := &mockServerServer{
			node: n,
		}

		srv := grpc.NewServer()
		grpc_health_v1.RegisterHealthServer(srv, healthSrv)
		gitalypb.RegisterServerServiceServer(srv, srvSrv)
		cleanupFns = append(cleanupFns, srv.Stop)

		go func() {
			assert.NoError(t, srv.Serve(ln))
		}()

		cfgNodes = append(cfgNodes, &config.Node{
			Storage: n.storage,
			Token:   n.token,
			Address: fmt.Sprintf("%s://%s", ln.Addr().Network(), ln.Addr().String()),
		})
	}

	return cfgNodes, func() {
		for _, cleanupFn := range cleanupFns {
			cleanupFn()
		}
	}
}

func TestPostgresReadWriteCheck(t *testing.T) {
	testCases := []struct {
		desc        string
		setup       func(t *testing.T, db testdb.DB) config.DB
		expectedErr string
		expectedLog string
	}{
		{
			desc: "read and write work",
			setup: func(t *testing.T, db testdb.DB) config.DB {
				return testdb.GetConfig(t, db.Name)
			},
			expectedLog: "successfully read from database\nsuccessfully wrote to database\n",
		},
		{
			desc: "read only",
			setup: func(t *testing.T, db testdb.DB) config.DB {
				role := "praefect_ro_role_" + strings.ReplaceAll(uuid.New().String(), "-", "")

				_, err := db.Exec(fmt.Sprintf(`
					CREATE ROLE %[1]s LOGIN;
					GRANT SELECT ON ALL TABLES IN SCHEMA public TO %[1]s;`, role))
				require.NoError(t, err)

				t.Cleanup(func() {
					_, err := db.Exec(fmt.Sprintf(`
						DROP OWNED BY %[1]s;
						DROP ROLE %[1]s;`, role))
					require.NoError(t, err)
				})

				dbCfg := testdb.GetConfig(t, db.Name)
				dbCfg.User = role
				dbCfg.Password = ""

				return dbCfg
			},
			expectedErr: "error writing to table: ERROR: permission denied for table hello_world",
			expectedLog: "successfully read from database\n",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			ctx, cancel := testhelper.Context()
			defer cancel()

			db := testdb.New(t)
			t.Cleanup(func() { require.NoError(t, db.Close()) })

			dbConf := tc.setup(t, db)

			conf := config.Config{DB: dbConf}
			var out bytes.Buffer
			c := NewPostgresReadWriteCheck(conf, &out, false)

			err := c.Run(ctx)
			if tc.expectedErr != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.expectedErr)
			} else {
				require.NoError(t, err)
			}

			require.Equal(t, tc.expectedLog, out.String())
		})
	}
}

func TestNewUnavailableReposCheck(t *testing.T) {
	conf := config.Config{
		VirtualStorages: []*config.VirtualStorage{
			{
				Name: "virtual-storage-1",
				Nodes: []*config.Node{
					{Storage: "storage-0"},
					{Storage: "storage-1"},
					{Storage: "storage-2"},
				},
			},
		},
	}

	testCases := []struct {
		desc         string
		healthyNodes map[string]map[string][]string
		expectedMsg  string
		expectedErr  error
	}{
		{
			desc: "all repos available",
			healthyNodes: map[string]map[string][]string{
				"praefect-0": {"virtual-storage-1": []string{"storage-0", "storage-1", "storage-2"}},
			},
			expectedMsg: "All repositories are available.\n",
			expectedErr: nil,
		},
		{
			desc: "one unavailable",
			healthyNodes: map[string]map[string][]string{
				"praefect-0": {"virtual-storage-1": []string{"storage-1", "storage-2"}},
			},
			expectedMsg: "virtual-storage \"virtual-storage-1\" has 1 repository that is unavailable.\n",
			expectedErr: errors.New("repositories unavailable"),
		},
		{
			desc: "three unavailable",
			healthyNodes: map[string]map[string][]string{
				"praefect-0": {"virtual-storage-1": []string{}},
			},
			expectedMsg: "virtual-storage \"virtual-storage-1\" has 3 repositories that are unavailable.\n",
			expectedErr: errors.New("repositories unavailable"),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			ctx, cancel := testhelper.Context()
			defer cancel()

			db := testdb.New(t)
			dbCfg := testdb.GetConfig(t, db.Name)
			conf.DB = dbCfg

			rs := datastore.NewPostgresRepositoryStore(db, nil)
			for path, storage := range map[string]string{
				"repo-0": "storage-0",
				"repo-1": "storage-1",
				"repo-2": "storage-2",
			} {
				repositoryID, err := rs.ReserveRepositoryID(ctx, "virtual-storage-1", path)
				require.NoError(t, err)
				require.NoError(t, rs.CreateRepository(
					ctx,
					repositoryID,
					"virtual-storage-1",
					path,
					path,
					storage,
					nil, nil, true, false,
				))

				require.NoError(t, err)
				require.NoError(t, rs.SetGeneration(ctx, repositoryID, storage, path, 1))
				require.NoError(t, err)
			}

			testdb.SetHealthyNodes(t, ctx, db, tc.healthyNodes)
			var stdout bytes.Buffer
			check := NewUnavailableReposCheck(conf, &stdout, false)

			assert.Equal(t, tc.expectedErr, check.Run(ctx))
			assert.Equal(t, tc.expectedMsg, stdout.String())
		})
	}
}

func TestNewClockSyncCheck(t *testing.T) {
	for _, tt := range []struct {
		desc        string
		offsetCheck func(ntpURL string, allowedOffset time.Duration) (bool, error)
		setup       func(t *testing.T)
		expErr      error
	}{
		{
			desc:        "synced",
			offsetCheck: func(_ string, _ time.Duration) (bool, error) { return true, nil },
		},
		{
			desc:        "not synced",
			offsetCheck: func(_ string, _ time.Duration) (bool, error) { return false, nil },
			expErr:      errors.New("praefect: clock is not synced"),
		},
		{
			desc:        "failure",
			offsetCheck: func(_ string, _ time.Duration) (bool, error) { return false, assert.AnError },
			expErr:      fmt.Errorf("praefect: %w", assert.AnError),
		},
		{
			desc: "custom url",
			offsetCheck: func(url string, _ time.Duration) (bool, error) {
				if url != "custom" {
					return false, assert.AnError
				}
				return true, nil
			},
			setup: func(t *testing.T) {
				testhelper.ModifyEnvironment(t, "NTP_HOST", "custom")
			},
		},
	} {
		t.Run(tt.desc, func(t *testing.T) {
			ctx, cancel := testhelper.Context()
			defer cancel()

			if tt.setup != nil {
				tt.setup(t)
			}

			check := NewClockSyncCheck(tt.offsetCheck)
			err := check(config.Config{}, bytes.NewBuffer(nil), false).Run(ctx)
			require.Equal(t, tt.expErr, err)
		})
	}
}
