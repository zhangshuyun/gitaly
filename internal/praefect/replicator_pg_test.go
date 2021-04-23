// +build postgres

package praefect

import (
	"context"
	"errors"
	"net"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/client"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/service/repository"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/datastore"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc"
)

type mockRepositoryService struct {
	gitalypb.UnimplementedRepositoryServiceServer
	ReplicateRepositoryFunc func(context.Context, *gitalypb.ReplicateRepositoryRequest) (*gitalypb.ReplicateRepositoryResponse, error)
}

func (m *mockRepositoryService) ReplicateRepository(ctx context.Context, r *gitalypb.ReplicateRepositoryRequest) (*gitalypb.ReplicateRepositoryResponse, error) {
	return m.ReplicateRepositoryFunc(ctx, r)
}

func TestReplicatorInvalidSourceRepository(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	tmp := testhelper.TempDir(t)

	socketPath := filepath.Join(tmp, "socket")
	ln, err := net.Listen("unix", socketPath)
	require.NoError(t, err)

	srv := grpc.NewServer()
	gitalypb.RegisterRepositoryServiceServer(srv, &mockRepositoryService{
		ReplicateRepositoryFunc: func(context.Context, *gitalypb.ReplicateRepositoryRequest) (*gitalypb.ReplicateRepositoryResponse, error) {
			return nil, repository.ErrInvalidSourceRepository
		},
	})
	defer srv.Stop()
	go srv.Serve(ln)

	targetCC, err := client.Dial(ln.Addr().Network()+":"+ln.Addr().String(), nil)
	require.NoError(t, err)

	rs := datastore.NewPostgresRepositoryStore(getDB(t), nil)
	require.NoError(t, rs.SetGeneration(ctx, "virtual-storage-1", "relative-path-1", "gitaly-1", 0))

	r := &defaultReplicator{rs: rs, log: testhelper.DiscardTestLogger(t)}
	require.NoError(t, r.Replicate(ctx, datastore.ReplicationEvent{
		Job: datastore.ReplicationJob{
			VirtualStorage:    "virtual-storage-1",
			RelativePath:      "relative-path-1",
			SourceNodeStorage: "gitaly-1",
			TargetNodeStorage: "gitaly-2",
		},
	}, nil, targetCC))

	exists, err := rs.RepositoryExists(ctx, "virtual-storage-1", "relative-path-1")
	require.NoError(t, err)
	require.False(t, exists)
}

func TestReplicatorDestroy(t *testing.T) {
	for _, tc := range []struct {
		change datastore.ChangeType
		exists bool
		error  error
	}{
		{change: datastore.DeleteReplica, exists: true},
		{change: datastore.DeleteRepo, exists: false},
		{change: "invalid-type", exists: true, error: errors.New(`unknown change type: "invalid-type"`)},
	} {
		t.Run(string(tc.change), func(t *testing.T) {
			db := getDB(t)

			rs := datastore.NewPostgresRepositoryStore(db, nil)

			ctx, cancel := testhelper.Context()
			defer cancel()

			require.NoError(t, rs.SetGeneration(ctx, "virtual-storage-1", "relative-path-1", "storage-1", 0))
			require.NoError(t, rs.SetGeneration(ctx, "virtual-storage-1", "relative-path-1", "storage-2", 0))

			ln, err := net.Listen("tcp", "localhost:0")
			require.NoError(t, err)

			srv := grpc.NewServer(grpc.UnknownServiceHandler(func(srv interface{}, stream grpc.ServerStream) error {
				return stream.SendMsg(&gitalypb.RemoveRepositoryResponse{})
			}))

			go srv.Serve(ln)
			defer srv.Stop()

			clientConn, err := grpc.Dial(ln.Addr().String(), grpc.WithInsecure())
			require.NoError(t, err)
			defer clientConn.Close()

			require.Equal(t, tc.error, defaultReplicator{
				rs:  rs,
				log: testhelper.DiscardTestLogger(t),
			}.Destroy(
				ctx,
				datastore.ReplicationEvent{
					Job: datastore.ReplicationJob{
						Change:            tc.change,
						VirtualStorage:    "virtual-storage-1",
						RelativePath:      "relative-path-1",
						TargetNodeStorage: "storage-1",
					},
				},
				clientConn,
			))

			exists, err := rs.RepositoryExists(ctx, "virtual-storage-1", "relative-path-1")
			require.NoError(t, err)
			require.Equal(t, tc.exists, exists)
		})
	}
}
