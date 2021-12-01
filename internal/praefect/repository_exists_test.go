package praefect

import (
	"context"
	"net"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/datastore"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/datastore/glsql"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/grpc-proxy/proxy"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/protoregistry"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testassert"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestRepositoryExistsHandler(t *testing.T) {
	t.Parallel()
	errServedByGitaly := status.Error(codes.Unknown, "request passed to Gitaly")

	db := glsql.NewDB(t)
	for _, tc := range []struct {
		desc          string
		routeToGitaly bool
		repository    *gitalypb.Repository
		response      *gitalypb.RepositoryExistsResponse
		error         error
	}{
		{
			desc:  "missing repository",
			error: errMissingRepository,
		},
		{
			desc:       "missing storage name",
			repository: &gitalypb.Repository{RelativePath: "relative-path"},
			error:      errMissingStorageName,
		},
		{
			desc:       "missing relative path",
			repository: &gitalypb.Repository{StorageName: "virtual-storage"},
			error:      errMissingRelativePath,
		},
		{
			desc:       "invalid virtual storage",
			repository: &gitalypb.Repository{StorageName: "invalid virtual storage", RelativePath: "relative-path"},
			response:   &gitalypb.RepositoryExistsResponse{Exists: false},
		},
		{
			desc:       "invalid relative path",
			repository: &gitalypb.Repository{StorageName: "virtual-storage", RelativePath: "invalid relative path"},
			response:   &gitalypb.RepositoryExistsResponse{Exists: false},
		},
		{
			desc:       "repository found",
			repository: &gitalypb.Repository{StorageName: "virtual-storage", RelativePath: "relative-path"},
			response:   &gitalypb.RepositoryExistsResponse{Exists: true},
		},
		{
			desc:          "routed to gitaly",
			routeToGitaly: true,
			error:         errServedByGitaly,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			db.TruncateAll(t)
			rs := datastore.NewPostgresRepositoryStore(db, map[string][]string{"virtual-storage": {"storage"}})

			ctx, cancel := testhelper.Context()
			defer cancel()

			require.NoError(t, rs.CreateRepository(ctx, 0, "virtual-storage", "relative-path", "relative-path", "storage", nil, nil, false, false))

			tmp := testhelper.TempDir(t)

			ln, err := net.Listen("unix", filepath.Join(tmp, "praefect"))
			require.NoError(t, err)

			electionStrategy := config.ElectionStrategyPerRepository
			if tc.routeToGitaly {
				electionStrategy = config.ElectionStrategySQL
			}

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
				nil,
				nil,
				nil,
			)
			defer srv.Stop()

			go func() { srv.Serve(ln) }()

			clientConn, err := grpc.DialContext(ctx, "unix://"+ln.Addr().String(), grpc.WithInsecure())
			require.NoError(t, err)
			defer testhelper.MustClose(t, clientConn)

			client := gitalypb.NewRepositoryServiceClient(clientConn)
			_, err = client.RepositorySize(ctx, &gitalypb.RepositorySizeRequest{Repository: tc.repository})
			require.Equal(t, errServedByGitaly, err, "other RPCs should be passed through")

			resp, err := client.RepositoryExists(ctx, &gitalypb.RepositoryExistsRequest{Repository: tc.repository})
			require.Equal(t, tc.error, err)
			testassert.ProtoEqual(t, tc.response, resp)
		})
	}
}
