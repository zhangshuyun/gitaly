package cache

import (
	"context"
	"io"
	"net"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	diskcache "gitlab.com/gitlab-org/gitaly/v14/internal/cache"
	"gitlab.com/gitlab-org/gitaly/v14/internal/middleware/cache/testdata"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/protoregistry"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

//go:generate make testdata/stream.pb.go
func TestInvalidators(t *testing.T) {
	mCache := newMockCache()

	reg, err := protoregistry.NewFromPaths("middleware/cache/testdata/stream.proto")
	require.NoError(t, err)

	srvr := grpc.NewServer(
		grpc.StreamInterceptor(
			StreamInvalidator(mCache, reg),
		),
		grpc.UnaryInterceptor(
			UnaryInvalidator(mCache, reg),
		),
	)
	ctx := testhelper.Context(t)

	svc := &testSvc{}

	cli, cc, cleanup := newTestSvc(t, ctx, srvr, svc)
	defer cleanup()

	repo1 := &gitalypb.Repository{
		GitAlternateObjectDirectories: []string{"1"},
		GitObjectDirectory:            "1",
		GlProjectPath:                 "1",
		GlRepository:                  "1",
		RelativePath:                  "1",
		StorageName:                   "1",
	}

	repo2 := &gitalypb.Repository{
		GitAlternateObjectDirectories: []string{"2"},
		GitObjectDirectory:            "2",
		GlProjectPath:                 "2",
		GlRepository:                  "2",
		RelativePath:                  "2",
		StorageName:                   "2",
	}

	repo3 := &gitalypb.Repository{
		GitAlternateObjectDirectories: []string{"3"},
		GitObjectDirectory:            "3",
		GlProjectPath:                 "3",
		GlRepository:                  "3",
		RelativePath:                  "3",
		StorageName:                   "3",
	}

	expectedSvcRequests := []*gitalypb.Repository{repo1, repo1, repo2, repo3, repo1, repo2, repo2}
	expectedInvalidations := []*gitalypb.Repository{repo2, repo3, repo1}

	// Should NOT trigger cache invalidation
	c, err := cli.ClientStreamRepoAccessor(ctx, &testdata.Request{
		Destination: repo1,
	})
	assert.NoError(t, err)
	_, err = c.Recv() // make client call synchronous by waiting for close
	assert.Equal(t, err, io.EOF)

	// Should NOT trigger cache invalidation
	c, err = cli.ClientStreamRepoMaintainer(ctx, &testdata.Request{
		Destination: repo1,
	})
	assert.NoError(t, err)
	_, err = c.Recv() // make client call synchronous by waiting for close
	assert.Equal(t, err, io.EOF)

	// Should trigger cache invalidation
	c, err = cli.ClientStreamRepoMutator(ctx, &testdata.Request{
		Destination: repo2,
	})
	assert.NoError(t, err)
	_, err = c.Recv() // make client call synchronous by waiting for close
	assert.Equal(t, err, io.EOF)

	// Should trigger cache invalidation
	c, err = cli.ClientStreamRepoMutator(ctx, &testdata.Request{
		Destination: repo3,
	})
	assert.NoError(t, err)
	_, err = c.Recv() // make client call synchronous by waiting for close
	assert.Equal(t, err, io.EOF)

	// Should trigger cache invalidation
	_, err = cli.ClientUnaryRepoMutator(ctx, &testdata.Request{
		Destination: repo1,
	})
	require.NoError(t, err)

	// Should NOT trigger cache invalidation
	_, err = cli.ClientUnaryRepoAccessor(ctx, &testdata.Request{
		Destination: repo2,
	})
	require.NoError(t, err)

	// Should NOT trigger cache invalidation
	_, err = cli.ClientUnaryRepoMaintainer(ctx, &testdata.Request{
		Destination: repo2,
	})
	require.NoError(t, err)

	// Health checks should NOT trigger cache invalidation
	hcr := &grpc_health_v1.HealthCheckRequest{Service: "TestService"}
	_, err = grpc_health_v1.NewHealthClient(cc).Check(ctx, hcr)
	require.NoError(t, err)
	require.Equal(t, 0, MethodErrCount.Method["/grpc.health.v1.Health/Check"])

	_, err = testdata.NewInterceptedServiceClient(cc).IgnoredMethod(ctx, &testdata.Request{})
	testhelper.RequireGrpcError(t, status.Error(codes.Unimplemented, "method IgnoredMethod not implemented"), err)
	require.Equal(t, 0, MethodErrCount.Method["/testdata.InterceptedService/IgnoredMethod"])

	testhelper.ProtoEqual(t, expectedInvalidations, mCache.(*mockCache).invalidatedRepos)
	testhelper.ProtoEqual(t, expectedSvcRequests, svc.repoRequests)
	require.Equal(t, 3, mCache.(*mockCache).endedLeases.count)
}

// mockCache allows us to relay back via channel which repos are being
// invalidated in the cache
type mockCache struct {
	invalidatedRepos []*gitalypb.Repository
	endedLeases      *struct {
		sync.RWMutex
		count int
	}
}

func newMockCache() diskcache.Invalidator {
	return &mockCache{
		endedLeases: &struct {
			sync.RWMutex
			count int
		}{},
	}
}

func (mc *mockCache) EndLease(_ context.Context) error {
	mc.endedLeases.Lock()
	defer mc.endedLeases.Unlock()
	mc.endedLeases.count++

	return nil
}

func (mc *mockCache) StartLease(repo *gitalypb.Repository) (diskcache.LeaseEnder, error) {
	mc.invalidatedRepos = append(mc.invalidatedRepos, repo)
	return mc, nil
}

func newTestSvc(t testing.TB, ctx context.Context, srvr *grpc.Server, svc testdata.TestServiceServer) (testdata.TestServiceClient, *grpc.ClientConn, func()) {
	healthSrvr := health.NewServer()
	grpc_health_v1.RegisterHealthServer(srvr, healthSrvr)
	healthSrvr.SetServingStatus("TestService", grpc_health_v1.HealthCheckResponse_SERVING)
	testdata.RegisterTestServiceServer(srvr, svc)
	testdata.RegisterInterceptedServiceServer(srvr, &testdata.UnimplementedInterceptedServiceServer{})

	lis, err := net.Listen("tcp", ":0")
	require.NoError(t, err)

	errQ := make(chan error)

	go func() {
		errQ <- srvr.Serve(lis)
	}()

	cleanup := func() {
		srvr.Stop()
		require.NoError(t, <-errQ)
	}

	cc, err := grpc.DialContext(
		ctx,
		lis.Addr().String(),
		grpc.WithBlock(),
		grpc.WithInsecure(),
	)
	require.NoError(t, err)

	return testdata.NewTestServiceClient(cc), cc, cleanup
}

type testSvc struct {
	testdata.UnimplementedTestServiceServer
	repoRequests []*gitalypb.Repository
}

func (ts *testSvc) ClientStreamRepoMutator(req *testdata.Request, _ testdata.TestService_ClientStreamRepoMutatorServer) error {
	ts.repoRequests = append(ts.repoRequests, req.GetDestination())
	return nil
}

func (ts *testSvc) ClientStreamRepoAccessor(req *testdata.Request, _ testdata.TestService_ClientStreamRepoAccessorServer) error {
	ts.repoRequests = append(ts.repoRequests, req.GetDestination())
	return nil
}

func (ts *testSvc) ClientStreamRepoMaintainer(req *testdata.Request, _ testdata.TestService_ClientStreamRepoMaintainerServer) error {
	ts.repoRequests = append(ts.repoRequests, req.GetDestination())
	return nil
}

func (ts *testSvc) ClientUnaryRepoMutator(_ context.Context, req *testdata.Request) (*testdata.Response, error) {
	ts.repoRequests = append(ts.repoRequests, req.GetDestination())
	return &testdata.Response{}, nil
}

func (ts *testSvc) ClientUnaryRepoAccessor(_ context.Context, req *testdata.Request) (*testdata.Response, error) {
	ts.repoRequests = append(ts.repoRequests, req.GetDestination())
	return &testdata.Response{}, nil
}

func (ts *testSvc) ClientUnaryRepoMaintainer(_ context.Context, req *testdata.Request) (*testdata.Response, error) {
	ts.repoRequests = append(ts.repoRequests, req.GetDestination())
	return &testdata.Response{}, nil
}
