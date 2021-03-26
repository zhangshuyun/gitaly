// +build postgres

package main

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/datastore"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/datastore/glsql"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/service/info"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc"
)

func getDB(t *testing.T) glsql.DB {
	return glsql.GetDB(t, "cmd_praefect")
}

func registerPraefectInfoServer(impl gitalypb.PraefectInfoServiceServer) svcRegistrar {
	return func(srv *grpc.Server) {
		gitalypb.RegisterPraefectInfoServiceServer(srv, impl)
	}
}

func TestDatalossSubcommand(t *testing.T) {
	for _, scope := range []struct {
		desc             string
		electionStrategy config.ElectionStrategy
		primaries        map[string]string
	}{
		{
			desc:             "sql elector",
			electionStrategy: config.ElectionStrategySQL,
			primaries: map[string]string{
				"repository-1": "gitaly-1",
				"repository-2": "gitaly-1",
			},
		},
		{
			desc:             "per_repository elector",
			electionStrategy: config.ElectionStrategyPerRepository,
			primaries: map[string]string{
				"repository-1": "gitaly-1",
				"repository-2": "gitaly-3",
			},
		},
	} {
		t.Run(scope.desc, func(t *testing.T) {
			cfg := config.Config{
				Failover: config.Failover{ElectionStrategy: scope.electionStrategy},
				VirtualStorages: []*config.VirtualStorage{
					{
						Name: "virtual-storage-1",
						Nodes: []*config.Node{
							{Storage: "gitaly-1"},
							{Storage: "gitaly-2"},
							{Storage: "gitaly-3"},
						},
					},
					{
						Name: "virtual-storage-2",
						Nodes: []*config.Node{
							{Storage: "gitaly-4"},
						},
					},
				},
			}

			db := getDB(t)
			gs := datastore.NewPostgresRepositoryStore(db, cfg.StorageNames())

			ctx, cancel := testhelper.Context()
			defer cancel()

			for _, q := range []string{
				`
				INSERT INTO repositories (virtual_storage, relative_path, "primary")
				VALUES
					('virtual-storage-1', 'repository-1', 'gitaly-1'),
					('virtual-storage-1', 'repository-2', 'gitaly-3')
				`,
				`
				INSERT INTO repository_assignments (virtual_storage, relative_path, storage)
				VALUES
					('virtual-storage-1', 'repository-1', 'gitaly-1'),
					('virtual-storage-1', 'repository-1', 'gitaly-2'),
					('virtual-storage-1', 'repository-2', 'gitaly-1'),
					('virtual-storage-1', 'repository-2', 'gitaly-3')
				`,
				`
				INSERT INTO shard_primaries (shard_name, node_name, elected_by_praefect, elected_at)
				VALUES ('virtual-storage-1', 'gitaly-1', 'ignored', now())
				`,
			} {
				_, err := db.ExecContext(ctx, q)
				require.NoError(t, err)
			}

			require.NoError(t, gs.SetGeneration(ctx, "virtual-storage-1", "repository-1", "gitaly-1", 1))
			require.NoError(t, gs.SetGeneration(ctx, "virtual-storage-1", "repository-1", "gitaly-2", 0))
			require.NoError(t, gs.SetGeneration(ctx, "virtual-storage-1", "repository-1", "gitaly-3", 0))

			require.NoError(t, gs.SetGeneration(ctx, "virtual-storage-1", "repository-2", "gitaly-2", 1))
			require.NoError(t, gs.SetGeneration(ctx, "virtual-storage-1", "repository-2", "gitaly-3", 0))

			ln, clean := listenAndServe(t, []svcRegistrar{
				registerPraefectInfoServer(info.NewServer(nil, cfg, nil, gs, nil, nil, nil))})
			defer clean()
			for _, tc := range []struct {
				desc            string
				args            []string
				virtualStorages []*config.VirtualStorage
				output          string
				error           error
			}{
				{
					desc:  "positional arguments",
					args:  []string{"-virtual-storage=virtual-storage-1", "positional-arg"},
					error: unexpectedPositionalArgsError{Command: "dataloss"},
				},
				{
					desc: "data loss with read-only repositories",
					args: []string{"-virtual-storage=virtual-storage-1"},
					output: fmt.Sprintf(`Virtual storage: virtual-storage-1
  Outdated repositories:
    repository-2 (read-only):
      Primary: %s
      In-Sync Storages:
        gitaly-2
      Outdated Storages:
        gitaly-1 is behind by 2 changes or less, assigned host
        gitaly-3 is behind by 1 change or less, assigned host
`, scope.primaries["repository-2"]),
				},
				{
					desc: "data loss with partially replicated repositories",
					args: []string{"-virtual-storage=virtual-storage-1", "-partially-replicated"},
					output: fmt.Sprintf(`Virtual storage: virtual-storage-1
  Outdated repositories:
    repository-1 (writable):
      Primary: %s
      In-Sync Storages:
        gitaly-1, assigned host
      Outdated Storages:
        gitaly-2 is behind by 1 change or less, assigned host
        gitaly-3 is behind by 1 change or less
    repository-2 (read-only):
      Primary: %s
      In-Sync Storages:
        gitaly-2
      Outdated Storages:
        gitaly-1 is behind by 2 changes or less, assigned host
        gitaly-3 is behind by 1 change or less, assigned host
`, scope.primaries["repository-1"], scope.primaries["repository-2"]),
				},
				{
					desc:            "multiple virtual storages with read-only repositories",
					virtualStorages: []*config.VirtualStorage{{Name: "virtual-storage-2"}, {Name: "virtual-storage-1"}},
					output: fmt.Sprintf(`Virtual storage: virtual-storage-1
  Outdated repositories:
    repository-2 (read-only):
      Primary: %s
      In-Sync Storages:
        gitaly-2
      Outdated Storages:
        gitaly-1 is behind by 2 changes or less, assigned host
        gitaly-3 is behind by 1 change or less, assigned host
Virtual storage: virtual-storage-2
  All repositories are writable!
`, scope.primaries["repository-2"]),
				},
				{
					desc:            "multiple virtual storages with partially replicated repositories",
					args:            []string{"-partially-replicated"},
					virtualStorages: []*config.VirtualStorage{{Name: "virtual-storage-2"}, {Name: "virtual-storage-1"}},
					output: fmt.Sprintf(`Virtual storage: virtual-storage-1
  Outdated repositories:
    repository-1 (writable):
      Primary: %s
      In-Sync Storages:
        gitaly-1, assigned host
      Outdated Storages:
        gitaly-2 is behind by 1 change or less, assigned host
        gitaly-3 is behind by 1 change or less
    repository-2 (read-only):
      Primary: %s
      In-Sync Storages:
        gitaly-2
      Outdated Storages:
        gitaly-1 is behind by 2 changes or less, assigned host
        gitaly-3 is behind by 1 change or less, assigned host
Virtual storage: virtual-storage-2
  All repositories are up to date!
`, scope.primaries["repository-1"], scope.primaries["repository-2"]),
				},
			} {
				t.Run(tc.desc, func(t *testing.T) {
					cmd := newDatalossSubcommand()
					output := &bytes.Buffer{}
					cmd.output = output

					fs := cmd.FlagSet()
					require.NoError(t, fs.Parse(tc.args))
					err := cmd.Exec(fs, config.Config{
						VirtualStorages: tc.virtualStorages,
						SocketPath:      ln.Addr().String(),
					})
					require.Equal(t, tc.error, err, err)
					require.Equal(t, tc.output, output.String())
				})
			}
		})
	}
}
