package interceptor_test

import (
	"context"
	"log"
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
		expect := expectedInvalidations[i]
		select {
		case actual := <-repoQ:
			requireReposEqual(t, actual, expect)
		case <-ctx.Done():
			require.Fail(t, "test timed out")
		}

	}

	cancel()
}

// requireReposEqual only compares "important" fields of a repo and ignores
// XXX_* fields
func requireReposEqual(t testing.TB, expect, actual *gitalypb.Repository) {
	require.Equal(t, expect.GitAlternateObjectDirectories, actual.GitAlternateObjectDirectories)
	require.Equal(t, expect.GitObjectDirectory, actual.GitObjectDirectory)
	require.Equal(t, expect.GlProjectPath, actual.GlProjectPath)
	require.Equal(t, expect.GlRepository, actual.GlRepository)
	require.Equal(t, expect.RelativePath, actual.RelativePath)
	require.Equal(t, expect.StorageName, actual.StorageName)
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

	return testdata.NewTestServiceClient(cc), cleanup
}

type testSvc struct {
	clientStreamRepoMutatorQ chan<- *testdata.Request
}

func (ts *testSvc) ClientStreamRepoMutator(req *testdata.Request, cli testdata.TestService_ClientStreamRepoMutatorServer) error {
	log.Printf("req: %#v", req)
	req = new(testdata.Request)
	cli.RecvMsg(req)
	log.Printf("req: %#v", req)
	//req <- clientStreamRepoMutatorQ
	return nil
}

func (ts *testSvc) ClientStreamRepoAccessor(*testdata.Request, testdata.TestService_ClientStreamRepoAccessorServer) error {
	return nil
}
