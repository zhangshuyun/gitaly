package praefect

import (
	"math/rand"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gittest"
	gconfig "gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/service/repository"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/datastore"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/nodes"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/protoregistry"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testdb"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"google.golang.org/grpc"
)

func TestInfoService_RepositoryReplicas(t *testing.T) {
	t.Parallel()
	var cfgs []gconfig.Cfg
	var cfgNodes []*config.Node
	var testRepo *gitalypb.Repository
	storages := []string{"g-1", "g-2", "g-3"}
	for i, storage := range storages {
		cfg, repo, _ := testcfg.BuildWithRepo(t, testcfg.WithStorages(storage))
		if testRepo == nil {
			testRepo = repo
		}
		cfgs = append(cfgs, cfg)
		cfgs[i].SocketPath = testserver.RunGitalyServer(t, cfgs[i], nil, func(srv *grpc.Server, deps *service.Dependencies) {
			gitalypb.RegisterRepositoryServiceServer(srv, repository.NewServer(
				deps.GetCfg(),
				deps.GetRubyServer(),
				deps.GetLocator(),
				deps.GetTxManager(),
				deps.GetGitCmdFactory(),
				deps.GetCatfileCache(),
				deps.GetConnsPool(),
				deps.GetGit2goExecutor(),
				deps.GetHousekeepingManager(),
			))
		}, testserver.WithDisablePraefect())
		cfgNodes = append(cfgNodes, &config.Node{
			Storage: cfgs[i].Storages[0].Name,
			Address: cfgs[i].SocketPath,
			Token:   cfgs[i].Auth.Token,
		})
	}

	const virtualStorage = "default"
	conf := config.Config{
		VirtualStorages: []*config.VirtualStorage{{Name: virtualStorage, Nodes: cfgNodes}},
		Failover:        config.Failover{Enabled: true},
	}

	// create a commit in the second replica so we can check that its checksum is different than the primary
	gittest.WriteCommit(t, cfgs[1], filepath.Join(cfgs[1].Storages[0].Path, testRepo.GetRelativePath()), gittest.WithBranch("master"))
	ctx := testhelper.Context(t)

	tx := testdb.New(t).Begin(t)
	defer tx.Rollback(t)

	testdb.SetHealthyNodes(t, ctx, tx, map[string]map[string][]string{
		"praefect-0": {virtualStorage: storages},
	})

	nodeSet, err := DialNodes(ctx, conf.VirtualStorages, protoregistry.GitalyProtoPreregistered, nil, nil, nil)
	require.NoError(t, err)
	defer nodeSet.Close()

	elector := nodes.NewPerRepositoryElector(tx)
	conns := nodeSet.Connections()
	rs := datastore.NewPostgresRepositoryStore(tx, conf.StorageNames())
	require.NoError(t,
		rs.CreateRepository(ctx, 1, virtualStorage, testRepo.GetRelativePath(), testRepo.GetRelativePath(), "g-1", []string{"g-2", "g-3"}, nil, true, false),
	)

	cc, _, cleanup := runPraefectServer(t, ctx, conf, buildOptions{
		withConnections: conns,
		withRepoStore:   rs,
		withRouter: NewPerRepositoryRouter(
			conns,
			elector,
			StaticHealthChecker{virtualStorage: storages},
			NewLockedRandom(rand.New(rand.NewSource(0))),
			rs,
			datastore.NewAssignmentStore(tx, conf.StorageNames()),
			rs,
			conf.DefaultReplicationFactors(),
		),
		withPrimaryGetter: elector,
	})
	defer cleanup()

	client := gitalypb.NewPraefectInfoServiceClient(cc)

	// CalculateChecksum through praefect will get the checksum of the primary
	repoClient := gitalypb.NewRepositoryServiceClient(cc)
	checksum, err := repoClient.CalculateChecksum(ctx, &gitalypb.CalculateChecksumRequest{
		Repository: &gitalypb.Repository{
			StorageName:  conf.VirtualStorages[0].Name,
			RelativePath: testRepo.GetRelativePath(),
		},
	})
	require.NoError(t, err)

	resp, err := client.RepositoryReplicas(ctx, &gitalypb.RepositoryReplicasRequest{
		Repository: &gitalypb.Repository{
			StorageName:  conf.VirtualStorages[0].Name,
			RelativePath: testRepo.GetRelativePath(),
		},
	})

	require.NoError(t, err)

	require.Equal(t, checksum.Checksum, resp.Primary.Checksum)
	var checked []string
	for _, secondary := range resp.GetReplicas() {
		switch storage := secondary.GetRepository().GetStorageName(); storage {
		case conf.VirtualStorages[0].Nodes[1].Storage:
			require.NotEqual(t, checksum.Checksum, secondary.Checksum, "should not be equal since we added a commit")
			checked = append(checked, storage)
		case conf.VirtualStorages[0].Nodes[2].Storage:
			require.Equal(t, checksum.Checksum, secondary.Checksum)
			checked = append(checked, storage)
		default:
			require.FailNow(t, "unexpected storage: %q", storage)
		}
	}
	require.ElementsMatch(t, []string{conf.VirtualStorages[0].Nodes[1].Storage, conf.VirtualStorages[0].Nodes[2].Storage}, checked)
}
