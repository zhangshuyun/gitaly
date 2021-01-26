package info

import (
	"context"
	"io"
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/client"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	gconfig "gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/service/internalgitaly"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/service/repository"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/datastore"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/nodes"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestServer_ConsistencyCheck(t *testing.T) {
	defer func(old gconfig.Cfg) { gconfig.Config = old }(gconfig.Config)

	primaryStorageDir, cleanupPrim := testhelper.TempDir(t)
	defer cleanupPrim()
	secondaryStorageDir, cleanupSec := testhelper.TempDir(t)
	defer cleanupSec()

	// 1.git exists on both storages and it is the same
	testhelper.NewTestRepoTo(t, primaryStorageDir, "1.git")
	testhelper.NewTestRepoTo(t, secondaryStorageDir, "1.git")
	// 2.git exists only on target storage (where traversal happens)
	testhelper.NewTestRepoTo(t, secondaryStorageDir, "2.git")
	// not.git is a folder on target storage that should be skipped as it is not a git repository
	require.NoError(t, os.MkdirAll(filepath.Join(secondaryStorageDir, "not.git"), os.ModePerm))

	gconfig.Config.Storages = []gconfig.Storage{{
		Name: "target",
		Path: secondaryStorageDir,
	}, {
		Name: "reference",
		Path: primaryStorageDir,
	}}

	conf := config.Config{
		VirtualStorages: []*config.VirtualStorage{{
			Name: "vs",
			Nodes: []*config.Node{{
				Storage: "reference",
				Address: testhelper.GetTemporaryGitalySocketFileName(t),
			}, {
				Storage: "target",
				Address: testhelper.GetTemporaryGitalySocketFileName(t),
			}},
		}},
	}

	for _, node := range conf.VirtualStorages[0].Nodes {
		gitalyListener, err := net.Listen("unix", node.Address)
		require.NoError(t, err)

		gitalySrv := grpc.NewServer()
		defer gitalySrv.Stop()
		gitalypb.RegisterRepositoryServiceServer(gitalySrv, repository.NewServer(gconfig.Config, nil, gconfig.NewLocator(gconfig.Config), git.NewExecCommandFactory(gconfig.Config)))
		gitalypb.RegisterInternalGitalyServer(gitalySrv, internalgitaly.NewServer(gconfig.Config.Storages))
		go func() { gitalySrv.Serve(gitalyListener) }()
	}

	ctx, cancel := testhelper.Context()
	defer cancel()

	referenceConn, err := client.DialContext(ctx, "unix://"+conf.VirtualStorages[0].Nodes[0].Address, nil)
	require.NoError(t, err)
	defer referenceConn.Close()

	targetConn, err := client.DialContext(ctx, "unix://"+conf.VirtualStorages[0].Nodes[1].Address, nil)
	require.NoError(t, err)
	defer targetConn.Close()

	nm := &nodes.MockManager{
		GetShardFunc: func(s string) (nodes.Shard, error) {
			if s != conf.VirtualStorages[0].Name {
				return nodes.Shard{}, nodes.ErrVirtualStorageNotExist
			}
			return nodes.Shard{
				Primary: &nodes.MockNode{
					GetStorageMethod: func() string { return gconfig.Config.Storages[0].Name },
					Conn:             referenceConn,
					Healthy:          true,
				},
				Secondaries: []nodes.Node{&nodes.MockNode{
					GetStorageMethod: func() string { return gconfig.Config.Storages[1].Name },
					Conn:             targetConn,
					Healthy:          true,
				}},
			}, nil
		},
	}

	praefectAddr := testhelper.GetTemporaryGitalySocketFileName(t)
	praefectListener, err := net.Listen("unix", praefectAddr)
	require.NoError(t, err)
	defer praefectListener.Close()

	queue := datastore.NewReplicationEventQueueInterceptor(datastore.NewMemoryReplicationEventQueue(conf))
	queue.OnEnqueue(func(ctx context.Context, e datastore.ReplicationEvent, q datastore.ReplicationEventQueue) (datastore.ReplicationEvent, error) {
		return datastore.ReplicationEvent{ID: 1}, nil
	})

	grpcSrv := grpc.NewServer()
	defer grpcSrv.Stop()

	gitalypb.RegisterPraefectInfoServiceServer(grpcSrv, NewServer(nm, conf, queue, nil, nil))
	go grpcSrv.Serve(praefectListener)

	infoConn, err := client.Dial("unix://"+praefectAddr, nil)
	require.NoError(t, err)
	defer infoConn.Close()

	infoClient := gitalypb.NewPraefectInfoServiceClient(infoConn)

	for _, tc := range []struct {
		desc   string
		req    gitalypb.ConsistencyCheckRequest
		verify func(*testing.T, []*gitalypb.ConsistencyCheckResponse, error)
	}{
		{
			desc: "with replication event created",
			req: gitalypb.ConsistencyCheckRequest{
				VirtualStorage:         "vs",
				TargetStorage:          "reference",
				ReferenceStorage:       "target",
				DisableReconcilliation: false,
			},
			verify: func(t *testing.T, resp []*gitalypb.ConsistencyCheckResponse, err error) {
				require.NoError(t, err)
				require.Equal(t, []*gitalypb.ConsistencyCheckResponse{
					{
						RepoRelativePath:  "1.git",
						TargetChecksum:    "06c4db1a33b2e48dac0bf940c7c20429d00a04ea",
						ReferenceChecksum: "06c4db1a33b2e48dac0bf940c7c20429d00a04ea",
						ReplJobId:         0,
						ReferenceStorage:  "target",
					},
					{
						RepoRelativePath:  "2.git",
						TargetChecksum:    "",
						ReferenceChecksum: "06c4db1a33b2e48dac0bf940c7c20429d00a04ea",
						ReplJobId:         1,
						ReferenceStorage:  "target",
					},
				}, resp)
			},
		},
		{
			desc: "without replication event",
			req: gitalypb.ConsistencyCheckRequest{
				VirtualStorage:         "vs",
				TargetStorage:          "reference",
				ReferenceStorage:       "target",
				DisableReconcilliation: true,
			},
			verify: func(t *testing.T, resp []*gitalypb.ConsistencyCheckResponse, err error) {
				require.NoError(t, err)
				require.Equal(t, []*gitalypb.ConsistencyCheckResponse{
					{
						RepoRelativePath:  "1.git",
						TargetChecksum:    "06c4db1a33b2e48dac0bf940c7c20429d00a04ea",
						ReferenceChecksum: "06c4db1a33b2e48dac0bf940c7c20429d00a04ea",
						ReplJobId:         0,
						ReferenceStorage:  "target",
					},
					{
						RepoRelativePath:  "2.git",
						TargetChecksum:    "",
						ReferenceChecksum: "06c4db1a33b2e48dac0bf940c7c20429d00a04ea",
						ReplJobId:         0,
						ReferenceStorage:  "target",
					},
				}, resp)
			},
		},
		{
			desc: "no target",
			req: gitalypb.ConsistencyCheckRequest{
				VirtualStorage:   "vs",
				TargetStorage:    "",
				ReferenceStorage: "reference",
			},
			verify: func(t *testing.T, resp []*gitalypb.ConsistencyCheckResponse, err error) {
				require.Equal(t, status.Error(codes.InvalidArgument, "missing target storage"), err)
			},
		},
		{
			desc: "unknown target",
			req: gitalypb.ConsistencyCheckRequest{
				VirtualStorage:   "vs",
				TargetStorage:    "unknown",
				ReferenceStorage: "reference",
			},
			verify: func(t *testing.T, resp []*gitalypb.ConsistencyCheckResponse, err error) {
				require.Equal(t, status.Error(codes.NotFound, `unable to find target storage "unknown"`), err)
			},
		},
		{
			desc: "no reference",
			req: gitalypb.ConsistencyCheckRequest{
				VirtualStorage:   "vs",
				TargetStorage:    "target",
				ReferenceStorage: "",
			},
			verify: func(t *testing.T, resp []*gitalypb.ConsistencyCheckResponse, err error) {
				require.Equal(t, status.Error(codes.InvalidArgument, `target storage "target" is same as current primary, must provide alternate reference`), err)
			},
		},
		{
			desc: "unknown reference",
			req: gitalypb.ConsistencyCheckRequest{
				VirtualStorage:   "vs",
				TargetStorage:    "target",
				ReferenceStorage: "unknown",
			},
			verify: func(t *testing.T, resp []*gitalypb.ConsistencyCheckResponse, err error) {
				require.Equal(t, status.Error(codes.NotFound, `unable to find reference storage "unknown" in nodes for shard "vs"`), err)
			},
		},
		{
			desc: "same storage",
			req: gitalypb.ConsistencyCheckRequest{
				VirtualStorage:   "vs",
				TargetStorage:    "target",
				ReferenceStorage: "target",
			},
			verify: func(t *testing.T, resp []*gitalypb.ConsistencyCheckResponse, err error) {
				require.Equal(t, status.Error(codes.InvalidArgument, `target storage "target" cannot match reference storage "target"`), err)
			},
		},
		{
			desc: "no virtual",
			req: gitalypb.ConsistencyCheckRequest{
				VirtualStorage:   "",
				TargetStorage:    "target",
				ReferenceStorage: "reference",
			},
			verify: func(t *testing.T, resp []*gitalypb.ConsistencyCheckResponse, err error) {
				require.Equal(t, status.Error(codes.InvalidArgument, "missing virtual storage"), err)
			},
		},
		{
			desc: "unknown virtual",
			req: gitalypb.ConsistencyCheckRequest{
				VirtualStorage:   "unknown",
				TargetStorage:    "target",
				ReferenceStorage: "unknown",
			},
			verify: func(t *testing.T, resp []*gitalypb.ConsistencyCheckResponse, err error) {
				require.Equal(t, status.Error(codes.NotFound, "virtual storage does not exist"), err)
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			response, err := infoClient.ConsistencyCheck(ctx, &tc.req)
			require.NoError(t, err)

			var results []*gitalypb.ConsistencyCheckResponse
			var result *gitalypb.ConsistencyCheckResponse
			for {
				result, err = response.Recv()
				if err != nil {
					break
				}
				results = append(results, result)
			}

			if err == io.EOF {
				err = nil
			}
			tc.verify(t, results, err)
		})
	}
}
