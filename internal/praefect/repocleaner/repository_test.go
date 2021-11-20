package repocleaner

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/backchannel"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/service/setup"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/datastore"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/protoregistry"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/service/transaction"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testserver"
)

func TestWalker_ExecOnRepositories(t *testing.T) {
	const (
		repo1RelPath = "repo-1.git"
		repo2RelPath = "repo-2.git"
		repo3RelPath = "repo-3.git"

		storage1 = "gitaly-1"

		virtualStorage = "praefect"
	)

	gCfg := testcfg.Build(t, testcfg.WithStorages(storage1))
	gAddr := testserver.RunGitalyServer(t, gCfg, nil, setup.RegisterAll, testserver.WithDisablePraefect())

	conf := config.Config{
		SocketPath: testhelper.GetTemporaryGitalySocketFileName(t),
		VirtualStorages: []*config.VirtualStorage{
			{
				Name: virtualStorage,
				Nodes: []*config.Node{
					{Storage: gCfg.Storages[0].Name, Address: gAddr},
				},
			},
		},
	}

	gittest.CloneRepo(t, gCfg, gCfg.Storages[0], gittest.CloneRepoOpts{RelativePath: repo1RelPath})
	gittest.CloneRepo(t, gCfg, gCfg.Storages[0], gittest.CloneRepoOpts{RelativePath: repo2RelPath})
	gittest.CloneRepo(t, gCfg, gCfg.Storages[0], gittest.CloneRepoOpts{RelativePath: repo3RelPath})

	ctx, cancel := testhelper.Context()
	defer cancel()

	entry := testhelper.NewTestLogger(t).WithContext(ctx)
	clientHandshaker := backchannel.NewClientHandshaker(entry, praefect.NewBackchannelServerFactory(entry, transaction.NewServer(nil)))
	nodeSet, err := praefect.DialNodes(ctx, conf.VirtualStorages, protoregistry.GitalyProtoPreregistered, nil, clientHandshaker)
	require.NoError(t, err)
	defer nodeSet.Close()

	for _, tc := range []struct {
		desc      string
		batchSize int
		exp       [][]datastore.RepositoryClusterPath
		expErr    error
	}{
		{
			desc:      "multiple batches",
			batchSize: 2,
			exp: [][]datastore.RepositoryClusterPath{
				{
					{ClusterPath: datastore.ClusterPath{VirtualStorage: virtualStorage, Storage: storage1}, RelativePath: repo1RelPath},
					{ClusterPath: datastore.ClusterPath{VirtualStorage: virtualStorage, Storage: storage1}, RelativePath: repo2RelPath},
				},
				{
					{ClusterPath: datastore.ClusterPath{VirtualStorage: virtualStorage, Storage: storage1}, RelativePath: repo3RelPath},
				},
			},
		},
		{
			desc:      "single batch",
			batchSize: 10,
			exp: [][]datastore.RepositoryClusterPath{
				{
					{ClusterPath: datastore.ClusterPath{VirtualStorage: virtualStorage, Storage: storage1}, RelativePath: repo1RelPath},
					{ClusterPath: datastore.ClusterPath{VirtualStorage: virtualStorage, Storage: storage1}, RelativePath: repo2RelPath},
					{ClusterPath: datastore.ClusterPath{VirtualStorage: virtualStorage, Storage: storage1}, RelativePath: repo3RelPath},
				},
			},
		},
		{
			desc:      "terminates on error",
			batchSize: 1,
			exp: [][]datastore.RepositoryClusterPath{
				{
					{ClusterPath: datastore.ClusterPath{VirtualStorage: virtualStorage, Storage: storage1}, RelativePath: repo1RelPath},
				},
			},
			expErr: assert.AnError,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			walker := NewWalker(nodeSet.Connections(), tc.batchSize)
			var iteration int
			err = walker.ExecOnRepositories(ctx, conf.VirtualStorages[0].Name, storage1, func(paths []datastore.RepositoryClusterPath) error {
				require.Less(t, iteration, len(tc.exp))
				expected := tc.exp[iteration]
				iteration++
				assert.ElementsMatch(t, paths, expected)
				return tc.expErr
			})
			require.Equal(t, tc.expErr, err)
		})
	}
}
