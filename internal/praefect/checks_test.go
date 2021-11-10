package praefect

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/datastore"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/datastore/glsql"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/datastore/migrations"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
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
			db := glsql.NewDB(t)
			cfg.DB = glsql.GetDBConfig(t, db.Name)

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

		ctx, cancel := testhelper.Context(testhelper.ContextWithTimeout(1 * time.Second))
		defer cancel()

		assert.Errorf(t, check.Run(ctx), "the following nodes are not healthy: %s", socketAddr)
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
		desc     string
		setup    func(t *testing.T, db glsql.DB) error
		errMsg   string
		logLines string
	}{
		{
			desc: "read and write work",
			setup: func(t *testing.T, db glsql.DB) error {
				return nil
			},
			errMsg:   "",
			logLines: "successfully read from database\nsuccessfully wrote to database\n",
		},
		{
			desc: "read only",
			setup: func(t *testing.T, db glsql.DB) error {
				tx := db.Begin(t)

				if _, err := tx.Exec("LOCK TABLE hello_world IN ACCESS EXCLUSIVE MODE"); err != nil {
					return err
				}

				t.Cleanup(func() { tx.Rollback(t) })
				return nil
			},
			errMsg:   "error reading from table",
			logLines: "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			ctx, cancel := testhelper.Context(testhelper.ContextWithTimeout(5 * time.Second))
			defer cancel()

			db := glsql.NewDB(t)
			defer func() {
				require.NoError(t, db.Close())
			}()

			require.NoError(t, tc.setup(t, db))

			dbConf := glsql.GetDBConfig(t, db.Name)

			conf := config.Config{DB: dbConf}
			var out bytes.Buffer
			c := NewPostgresReadWriteCheck(conf, &out, false)

			err := c.Run(ctx)
			if tc.errMsg != "" {
				assert.Contains(t, err.Error(), tc.errMsg)
			} else {
				assert.NoError(t, err)
			}

			assert.Equal(t, tc.logLines, out.String())
		})
	}
}
