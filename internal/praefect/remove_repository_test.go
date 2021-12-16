package praefect

import (
	"context"
	"net"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/service/setup"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/datastore"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/grpc-proxy/proxy"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/protoregistry"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testdb"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestRemoveRepositoryHandler(t *testing.T) {
	t.Parallel()
	testhelper.NewFeatureSets(featureflag.AtomicRemoveRepository).Run(t, testRemoveRepositoryHandler)
}

func testRemoveRepositoryHandler(t *testing.T, ctx context.Context) {
	t.Parallel()

	errServedByGitaly := status.Error(codes.Unknown, "request passed to Gitaly")
	const virtualStorage, relativePath = "virtual-storage", "relative-path"

	var errNotFound error
	if featureflag.AtomicRemoveRepository.IsEnabled(ctx) {
		errNotFound = helper.ErrNotFoundf("repository does not exist")
	}

	db := testdb.New(t)
	for _, tc := range []struct {
		desc          string
		routeToGitaly bool
		repository    *gitalypb.Repository
		repoDeleted   bool
		error         error
	}{
		{
			desc:  "missing repository",
			error: errMissingRepository,
		},
		{
			desc:       "repository not found",
			repository: &gitalypb.Repository{StorageName: "virtual-storage", RelativePath: "doesn't exist"},
			error:      errNotFound,
		},
		{
			desc:        "repository found",
			repository:  &gitalypb.Repository{StorageName: "virtual-storage", RelativePath: relativePath},
			repoDeleted: true,
		},
		{
			desc:          "routed to gitaly",
			routeToGitaly: true,
			error:         errServedByGitaly,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			db.TruncateAll(t)

			const gitaly1Storage = "gitaly-1"
			gitaly1Cfg := testcfg.Build(t, testcfg.WithStorages(gitaly1Storage))
			gitaly1RepoPath := filepath.Join(gitaly1Cfg.Storages[0].Path, relativePath)
			gitaly1Addr := testserver.RunGitalyServer(t, gitaly1Cfg, nil, setup.RegisterAll, testserver.WithDisablePraefect())

			const gitaly2Storage = "gitaly-2"
			gitaly2Cfg := testcfg.Build(t, testcfg.WithStorages(gitaly2Storage))
			gitaly2RepoPath := filepath.Join(gitaly2Cfg.Storages[0].Path, relativePath)
			gitaly2Addr := testserver.RunGitalyServer(t, gitaly2Cfg, nil, setup.RegisterAll, testserver.WithDisablePraefect())

			cfg := config.Config{VirtualStorages: []*config.VirtualStorage{
				{
					Name: virtualStorage,
					Nodes: []*config.Node{
						{Storage: gitaly1Storage, Address: gitaly1Addr},
						{Storage: gitaly2Storage, Address: gitaly2Addr},
					},
				},
			}}

			for _, repoPath := range []string{gitaly1RepoPath, gitaly2RepoPath} {
				gittest.Exec(t, gitaly1Cfg, "init", "--bare", repoPath)
			}

			rs := datastore.NewPostgresRepositoryStore(db, cfg.StorageNames())

			require.NoError(t, rs.CreateRepository(ctx, 0, virtualStorage, relativePath, relativePath, gitaly1Storage, []string{gitaly2Storage, "non-existent-storage"}, nil, false, false))

			tmp := testhelper.TempDir(t)

			ln, err := net.Listen("unix", filepath.Join(tmp, "praefect"))
			require.NoError(t, err)

			electionStrategy := config.ElectionStrategyPerRepository
			if tc.routeToGitaly {
				electionStrategy = config.ElectionStrategySQL
			}

			nodeSet, err := DialNodes(ctx, cfg.VirtualStorages, nil, nil, nil, nil)
			require.NoError(t, err)
			defer nodeSet.Close()

			srv := NewGRPCServer(
				config.Config{Failover: config.Failover{ElectionStrategy: electionStrategy}},
				testhelper.NewDiscardingLogEntry(t),
				protoregistry.GitalyProtoPreregistered,
				func(ctx context.Context, fullMethodName string, peeker proxy.StreamPeeker) (*proxy.StreamParameters, error) {
					return nil, errServedByGitaly
				},
				nil,
				nil,
				nil,
				rs,
				nil,
				nodeSet.Connections(),
				nil,
				nil,
			)
			defer srv.Stop()

			go func() { srv.Serve(ln) }()

			clientConn, err := grpc.DialContext(ctx, "unix:"+ln.Addr().String(), grpc.WithInsecure())
			require.NoError(t, err)
			defer clientConn.Close()

			client := gitalypb.NewRepositoryServiceClient(clientConn)
			_, err = client.RepositorySize(ctx, &gitalypb.RepositorySizeRequest{Repository: tc.repository})
			require.Equal(t, errServedByGitaly, err, "other RPCs should be passed through")

			assertExistence := require.DirExists
			if tc.repoDeleted {
				assertExistence = require.NoDirExists
			}

			resp, err := client.RemoveRepository(ctx, &gitalypb.RemoveRepositoryRequest{Repository: tc.repository})
			if tc.error != nil {
				testhelper.RequireGrpcError(t, tc.error, err)
				assertExistence(t, gitaly1RepoPath)
				assertExistence(t, gitaly2RepoPath)
				return
			}

			require.NoError(t, err)
			testhelper.ProtoEqual(t, &gitalypb.RemoveRepositoryResponse{}, resp)
			assertExistence(t, gitaly1RepoPath)
			assertExistence(t, gitaly2RepoPath)
		})
	}
}
