package praefect

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gittest"
	gconfig "gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/service/repository"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/nodes"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/protoregistry"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/promtest"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"google.golang.org/grpc"
)

func TestInfoService_RepositoryReplicas(t *testing.T) {
	var cfgs []gconfig.Cfg
	var cfgNodes []*config.Node
	var testRepo *gitalypb.Repository
	for i, storage := range []string{"g-1", "g-2", "g-3"} {
		cfg, repo, _ := testcfg.BuildWithRepo(t, testcfg.WithStorages(storage))
		if testRepo == nil {
			testRepo = repo
		}
		cfgs = append(cfgs, cfg)
		cfgs[i].SocketPath = testserver.RunGitalyServer(t, cfgs[i], nil, func(srv grpc.ServiceRegistrar, deps *service.Dependencies) {
			gitalypb.RegisterRepositoryServiceServer(srv, repository.NewServer(
				deps.GetCfg(),
				deps.GetRubyServer(),
				deps.GetLocator(),
				deps.GetTxManager(),
				deps.GetGitCmdFactory(),
				deps.GetCatfileCache(),
			))
		}, testserver.WithDisablePraefect())
		cfgNodes = append(cfgNodes, &config.Node{
			Storage: cfgs[i].Storages[0].Name,
			Address: cfgs[i].SocketPath,
			Token:   cfgs[i].Auth.Token,
		})
	}

	conf := config.Config{
		VirtualStorages: []*config.VirtualStorage{{Name: "default", Nodes: cfgNodes}},
		Failover:        config.Failover{Enabled: true},
	}

	// create a commit in the second replica so we can check that its checksum is different than the primary
	gittest.WriteCommit(t, cfgs[1], filepath.Join(cfgs[1].Storages[0].Path, testRepo.GetRelativePath()), gittest.WithBranch("master"))

	nodeManager, err := nodes.NewManager(testhelper.DiscardTestEntry(t), conf, nil, nil, promtest.NewMockHistogramVec(), protoregistry.GitalyProtoPreregistered, nil, nil)
	require.NoError(t, err)
	nodeManager.Start(0, time.Hour)
	cc, _, cleanup := runPraefectServer(t, conf, buildOptions{
		withPrimaryGetter: nodeManager,
		withConnections:   NodeSetFromNodeManager(nodeManager).Connections(),
	})
	defer cleanup()

	client := gitalypb.NewPraefectInfoServiceClient(cc)

	ctx, cancel := testhelper.Context()
	defer cancel()

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
