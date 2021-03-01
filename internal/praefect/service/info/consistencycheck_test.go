package info

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/client"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/gittest"
	gconfig "gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/service/internalgitaly"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/service/repository"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/transaction"
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
	cfg := gconfig.Config

	const (
		firstRepoPath  = "1.git"
		secondRepoPath = "2.git"
		thirdRepoPath  = "3.git"

		checksum = "06c4db1a33b2e48dac0bf940c7c20429d00a04ea"

		targetStorageName    = "target"
		referenceStorageName = "reference"

		virtualStorage = "virtualStorage"
	)

	primaryStorageDir, cleanupPrim := testhelper.TempDir(t)
	defer cleanupPrim()
	secondaryStorageDir, cleanupSec := testhelper.TempDir(t)
	defer cleanupSec()

	// firstRepoPath exists on both storages and has same state
	gittest.CloneRepoAtStorageRoot(t, primaryStorageDir, firstRepoPath)
	gittest.CloneRepoAtStorageRoot(t, secondaryStorageDir, firstRepoPath)

	cfg.Storages = []gconfig.Storage{{
		Name: referenceStorageName,
		Path: secondaryStorageDir,
	}, {
		Name: targetStorageName,
		Path: primaryStorageDir,
	}}

	conf := config.Config{VirtualStorages: []*config.VirtualStorage{{Name: virtualStorage}}}
	for _, storage := range cfg.Storages {
		conf.VirtualStorages[0].Nodes = append(conf.VirtualStorages[0].Nodes, &config.Node{
			Storage: storage.Name,
			Address: testhelper.GetTemporaryGitalySocketFileName(t),
		})
	}

	ctx, cancel := testhelper.Context()
	defer cancel()

	var toTerminate *grpc.Server
	var addr string
	var conns []*grpc.ClientConn
	for i, node := range conf.VirtualStorages[0].Nodes {
		gitalyListener, err := net.Listen("unix", node.Address)
		require.NoError(t, err)

		gitalySrv := grpc.NewServer()
		defer gitalySrv.Stop()

		gitalypb.RegisterRepositoryServiceServer(gitalySrv, repository.NewServer(cfg, nil, gconfig.NewLocator(cfg), transaction.NewManager(cfg), git.NewExecCommandFactory(cfg)))
		gitalypb.RegisterInternalGitalyServer(gitalySrv, internalgitaly.NewServer(cfg.Storages))

		go func() { gitalySrv.Serve(gitalyListener) }()

		if i+1 == len(conf.VirtualStorages[0].Nodes) {
			toTerminate = gitalySrv
			addr = node.Address
		}

		conn, err := client.DialContext(ctx, "unix://"+node.Address, nil)
		require.NoError(t, err)
		defer conn.Close()

		conns = append(conns, conn)
	}

	nm := &nodes.MockManager{
		GetShardFunc: func(s string) (nodes.Shard, error) {
			if s != conf.VirtualStorages[0].Name {
				return nodes.Shard{}, nodes.ErrVirtualStorageNotExist
			}
			return nodes.Shard{
				Primary: &nodes.MockNode{
					GetStorageMethod: func() string { return cfg.Storages[0].Name },
					Conn:             conns[0],
					Healthy:          true,
				},
				Secondaries: []nodes.Node{&nodes.MockNode{
					GetStorageMethod: func() string { return cfg.Storages[1].Name },
					Conn:             conns[1],
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
		if e.Job.RelativePath == secondRepoPath {
			return datastore.ReplicationEvent{}, assert.AnError
		}
		return datastore.ReplicationEvent{ID: 1}, nil
	})

	// praefect instance setup
	praefectSrv := grpc.NewServer()
	defer praefectSrv.Stop()

	gitalypb.RegisterPraefectInfoServiceServer(praefectSrv, NewServer(nm, conf, queue, nil, nil))
	go praefectSrv.Serve(praefectListener)

	praefectConn, err := client.Dial("unix://"+praefectAddr, nil)
	require.NoError(t, err)
	defer praefectConn.Close()

	infoClient := gitalypb.NewPraefectInfoServiceClient(praefectConn)

	execAndVerify := func(t *testing.T, req gitalypb.ConsistencyCheckRequest, verify func(*testing.T, []*gitalypb.ConsistencyCheckResponse, error)) {
		response, err := infoClient.ConsistencyCheck(ctx, &req)
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
		verify(t, results, err)
	}

	t.Run("all in sync", func(t *testing.T) {
		req := gitalypb.ConsistencyCheckRequest{
			VirtualStorage:         virtualStorage,
			TargetStorage:          targetStorageName,
			ReferenceStorage:       referenceStorageName,
			DisableReconcilliation: true,
		}

		execAndVerify(t, req, func(t *testing.T, responses []*gitalypb.ConsistencyCheckResponse, err error) {
			require.NoError(t, err)
			require.Equal(t, []*gitalypb.ConsistencyCheckResponse{
				{
					RepoRelativePath:  firstRepoPath,
					ReferenceStorage:  referenceStorageName,
					TargetChecksum:    checksum,
					ReferenceChecksum: checksum,
				},
			}, responses)
		})
	})

	// secondRepoPath generates an error, but it should not stop other repositories from being processed.
	// Order does matter for the test to verify the flow.
	gittest.CloneRepoAtStorageRoot(t, secondaryStorageDir, secondRepoPath)
	// thirdRepoPath exists only on the reference storage (where traversal happens).
	gittest.CloneRepoAtStorageRoot(t, secondaryStorageDir, thirdRepoPath)
	// not.git is a folder on the reference storage that should be skipped as it is not a git repository.
	require.NoError(t, os.MkdirAll(filepath.Join(secondaryStorageDir, "not.git"), os.ModePerm))

	expErrStatus := status.Error(codes.Internal, errReconciliationInternal.Error())

	for _, tc := range []struct {
		desc   string
		req    gitalypb.ConsistencyCheckRequest
		verify func(*testing.T, []*gitalypb.ConsistencyCheckResponse, error)
	}{
		{
			desc: "with replication event created",
			req: gitalypb.ConsistencyCheckRequest{
				VirtualStorage:         virtualStorage,
				TargetStorage:          targetStorageName,
				ReferenceStorage:       referenceStorageName,
				DisableReconcilliation: false,
			},
			verify: func(t *testing.T, resp []*gitalypb.ConsistencyCheckResponse, err error) {
				require.Equal(t, expErrStatus, err)
				require.Equal(t, []*gitalypb.ConsistencyCheckResponse{
					{
						RepoRelativePath:  firstRepoPath,
						TargetChecksum:    checksum,
						ReferenceChecksum: checksum,
						ReplJobId:         0,
						ReferenceStorage:  referenceStorageName,
					},
					{
						RepoRelativePath:  secondRepoPath,
						TargetChecksum:    "",
						ReferenceChecksum: checksum,
						ReplJobId:         0,
						ReferenceStorage:  referenceStorageName,
						Errors:            []string{assert.AnError.Error()},
					},
					{
						RepoRelativePath:  thirdRepoPath,
						TargetChecksum:    "",
						ReferenceChecksum: checksum,
						ReplJobId:         1,
						ReferenceStorage:  referenceStorageName,
					},
				}, resp)
			},
		},
		{
			desc: "without replication event",
			req: gitalypb.ConsistencyCheckRequest{
				VirtualStorage:         virtualStorage,
				TargetStorage:          targetStorageName,
				ReferenceStorage:       referenceStorageName,
				DisableReconcilliation: true,
			},
			verify: func(t *testing.T, resp []*gitalypb.ConsistencyCheckResponse, err error) {
				require.NoError(t, err)
				require.Equal(t, []*gitalypb.ConsistencyCheckResponse{
					{
						RepoRelativePath:  firstRepoPath,
						TargetChecksum:    checksum,
						ReferenceChecksum: checksum,
						ReplJobId:         0,
						ReferenceStorage:  referenceStorageName,
					},
					{
						RepoRelativePath:  secondRepoPath,
						TargetChecksum:    "",
						ReferenceChecksum: checksum,
						ReplJobId:         0,
						ReferenceStorage:  referenceStorageName,
					},
					{
						RepoRelativePath:  thirdRepoPath,
						TargetChecksum:    "",
						ReferenceChecksum: checksum,
						ReplJobId:         0,
						ReferenceStorage:  referenceStorageName,
					},
				}, resp)
			},
		},
		{
			desc: "no target",
			req: gitalypb.ConsistencyCheckRequest{
				VirtualStorage:   virtualStorage,
				TargetStorage:    "",
				ReferenceStorage: targetStorageName,
			},
			verify: func(t *testing.T, resp []*gitalypb.ConsistencyCheckResponse, err error) {
				require.Equal(t, status.Error(codes.InvalidArgument, "missing target storage"), err)
			},
		},
		{
			desc: "unknown target",
			req: gitalypb.ConsistencyCheckRequest{
				VirtualStorage:   virtualStorage,
				TargetStorage:    "unknown",
				ReferenceStorage: targetStorageName,
			},
			verify: func(t *testing.T, resp []*gitalypb.ConsistencyCheckResponse, err error) {
				require.Equal(t, status.Error(codes.NotFound, `unable to find target storage "unknown"`), err)
			},
		},
		{
			desc: "no reference",
			req: gitalypb.ConsistencyCheckRequest{
				VirtualStorage:   virtualStorage,
				TargetStorage:    referenceStorageName,
				ReferenceStorage: "",
			},
			verify: func(t *testing.T, resp []*gitalypb.ConsistencyCheckResponse, err error) {
				expErr := status.Error(
					codes.InvalidArgument,
					fmt.Sprintf(`target storage %q is same as current primary, must provide alternate reference`, referenceStorageName),
				)
				require.Equal(t, expErr, err)
			},
		},
		{
			desc: "unknown reference",
			req: gitalypb.ConsistencyCheckRequest{
				VirtualStorage:   virtualStorage,
				TargetStorage:    referenceStorageName,
				ReferenceStorage: "unknown",
			},
			verify: func(t *testing.T, resp []*gitalypb.ConsistencyCheckResponse, err error) {
				expErr := status.Error(
					codes.NotFound,
					fmt.Sprintf(`unable to find reference storage "unknown" in nodes for shard %q`, virtualStorage),
				)
				require.Equal(t, expErr, err)
			},
		},
		{
			desc: "same storage",
			req: gitalypb.ConsistencyCheckRequest{
				VirtualStorage:   virtualStorage,
				TargetStorage:    referenceStorageName,
				ReferenceStorage: referenceStorageName,
			},
			verify: func(t *testing.T, resp []*gitalypb.ConsistencyCheckResponse, err error) {
				expErr := status.Error(
					codes.InvalidArgument,
					fmt.Sprintf(`target storage %q cannot match reference storage %q`, referenceStorageName, referenceStorageName),
				)
				require.Equal(t, expErr, err)
			},
		},
		{
			desc: "no virtual",
			req: gitalypb.ConsistencyCheckRequest{
				VirtualStorage:   "",
				TargetStorage:    referenceStorageName,
				ReferenceStorage: targetStorageName,
			},
			verify: func(t *testing.T, resp []*gitalypb.ConsistencyCheckResponse, err error) {
				require.Equal(t, status.Error(codes.InvalidArgument, "missing virtual storage"), err)
			},
		},
		{
			desc: "unknown virtual",
			req: gitalypb.ConsistencyCheckRequest{
				VirtualStorage:   "unknown",
				TargetStorage:    referenceStorageName,
				ReferenceStorage: "unknown",
			},
			verify: func(t *testing.T, resp []*gitalypb.ConsistencyCheckResponse, err error) {
				require.Equal(t, status.Error(codes.NotFound, "virtual storage does not exist"), err)
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			execAndVerify(t, tc.req, tc.verify)
		})
	}

	// this case needs to be the last as it terminates one of the gitaly instances
	t.Run("one of gitalies is unreachable", func(t *testing.T) {
		toTerminate.Stop()

		req := gitalypb.ConsistencyCheckRequest{
			VirtualStorage:         virtualStorage,
			TargetStorage:          targetStorageName,
			ReferenceStorage:       referenceStorageName,
			DisableReconcilliation: true,
		}

		execAndVerify(t, req, func(t *testing.T, responses []*gitalypb.ConsistencyCheckResponse, err error) {
			require.Equal(t, expErrStatus, err)
			errs := []string{
				fmt.Sprintf("rpc error: code = Unavailable desc = connection error: desc = \"transport: Error while dialing dial unix //%s: connect: no such file or directory\"", addr),
				"rpc error: code = Canceled desc = context canceled",
			}
			require.Equal(t, []*gitalypb.ConsistencyCheckResponse{
				{
					RepoRelativePath: firstRepoPath,
					ReferenceStorage: referenceStorageName,
					Errors:           errs,
				},
				{
					RepoRelativePath: secondRepoPath,
					ReferenceStorage: referenceStorageName,
					Errors:           errs,
				},
				{
					RepoRelativePath: thirdRepoPath,
					ReferenceStorage: referenceStorageName,
					Errors:           errs,
				},
			}, responses)
		})
	})
}
