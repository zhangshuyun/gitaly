package interceptor_test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/protoc-gen-go/descriptor"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly-proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/internal/interceptor"
	"gitlab.com/gitlab-org/gitaly/internal/interceptor/testdata"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/protoregistry"
	"google.golang.org/grpc"
)

//go:generate make testdata/stream.pb.go
func TestStreamInvalidator(t *testing.T) {

	cache, repoQ := newMockCache()

	reg := protoregistry.New()
	require.NoError(t, reg.RegisterFiles(streamFileDesc(t)))

	srvr := grpc.NewServer(
		grpc.StreamInterceptor(
			grpc.StreamServerInterceptor(
				interceptor.StreamInvalidator(cache, reg),
			),
		),
	)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	svc := &testSvc{}

	cli, cleanup := newTestSvc(t, ctx, srvr, svc)
	defer cleanup()

	expectedInvalidations := []*gitalypb.Repository{
		&gitalypb.Repository{
			GitAlternateObjectDirectories: []string{"1"},
			GitObjectDirectory:            "1",
			GlProjectPath:                 "1",
			GlRepository:                  "1",
			RelativePath:                  "1",
			StorageName:                   "1",
		},
	}

	go func() {
		_, err := cli.ClientStreamRepoMutator(ctx, &testdata.Request{
			Destination: expectedInvalidations[0],
		})
		assert.NoError(t, err)
	}()

	for i := 0; i < len(expectedInvalidations); i++ {
		t.Logf("waiting for repo invalidation #%d", i)
		select {
		case repo := <-repoQ:
			require.Equal(t, expectedInvalidations[i], repo)
		case <-ctx.Done():
			break
		}

	}

}

// mockCache allows us to relay back via channel which repos are being
// invalidated in the cache
type mockCache struct {
	invalidatedRepo chan<- *gitalypb.Repository
}

func newMockCache() (interceptor.RepoCache, <-chan *gitalypb.Repository) {
	repoQ := make(chan *gitalypb.Repository)
	return &mockCache{repoQ}, repoQ
}

func (mc mockCache) InvalidateRepo(repo *gitalypb.Repository) error {
	mc.invalidatedRepo <- repo
	return nil
}

func streamFileDesc(t testing.TB) *descriptor.FileDescriptorProto {
	fdp, err := protoregistry.ExtractFileDescriptor(proto.FileDescriptor("stream.proto"))
	require.NoError(t, err)
	return fdp
}

func newTestSvc(t testing.TB, ctx context.Context, srvr *grpc.Server, svc testdata.TestServiceServer) (testdata.TestServiceClient, func()) {
	testdata.RegisterTestServiceServer(srvr, svc)

	lis, err := net.Listen("tcp", ":0")
	require.NoError(t, err)

	errQ := make(chan error)

	go func() {
		errQ <- srvr.Serve(lis)
	}()

	cleanup := func() {
		require.NoError(t, <-errQ)
	}

	cc, err := grpc.DialContext(
		ctx,
		lis.Addr().String(),
		grpc.WithBlock(),
		grpc.WithInsecure(),
	)
	require.NoError(t, err)

	return testdata.NewTestServiceClient(cc), cleanup
}

type testSvc struct{}

func (ts *testSvc) ClientStreamRepoMutator(*testdata.Request, testdata.TestService_ClientStreamRepoMutatorServer) error {
	return nil
}
func (ts *testSvc) ClientStreamRepoAccessor(*testdata.Request, testdata.TestService_ClientStreamRepoAccessorServer) error {
	return nil
}
