package middleware

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/grpc-proxy/proxy"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/mock"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/nodes/tracker"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/protoregistry"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testcfg"
	"google.golang.org/grpc"
)

type simpleService struct {
	mock.UnimplementedSimpleServiceServer
}

func (s *simpleService) RepoAccessorUnary(ctx context.Context, in *mock.RepoRequest) (*empty.Empty, error) {
	if in.GetRepo() == nil {
		return nil, helper.ErrInternalf("error")
	}

	return &empty.Empty{}, nil
}

func (s *simpleService) RepoMutatorUnary(ctx context.Context, in *mock.RepoRequest) (*empty.Empty, error) {
	if in.GetRepo() == nil {
		return nil, helper.ErrInternalf("error")
	}

	return &empty.Empty{}, nil
}

func TestStreamInterceptor(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	window := 1 * time.Second
	threshold := 5
	errTracker, err := tracker.NewErrors(ctx, window, uint32(threshold), uint32(threshold))
	require.NoError(t, err)
	nodeName := "node-1"

	internalSrv := grpc.NewServer()

	internalServerSocketPath := testhelper.GetTemporaryGitalySocketFileName(t)
	lis, err := net.Listen("unix", internalServerSocketPath)
	require.NoError(t, err)

	gz := proto.FileDescriptor("praefect/mock/mock.proto")
	fd, err := protoregistry.ExtractFileDescriptor(gz)
	require.NoError(t, err)

	registry, err := protoregistry.New(fd)
	require.NoError(t, err)

	require.NoError(t, err)
	mock.RegisterSimpleServiceServer(internalSrv, &simpleService{})

	go internalSrv.Serve(lis)
	defer internalSrv.Stop()

	srvOptions := []grpc.ServerOption{
		grpc.CustomCodec(proxy.NewCodec()),
		grpc.UnknownServiceHandler(proxy.TransparentHandler(func(ctx context.Context,
			fullMethodName string,
			peeker proxy.StreamPeeker,
		) (*proxy.StreamParameters, error) {
			cc, err := grpc.Dial("unix://"+internalServerSocketPath,
				grpc.WithDefaultCallOptions(grpc.ForceCodec(proxy.NewCodec())),
				grpc.WithInsecure(),
				grpc.WithStreamInterceptor(StreamErrorHandler(registry, errTracker, nodeName)),
			)
			require.NoError(t, err)
			f, err := peeker.Peek()
			require.NoError(t, err)
			return proxy.NewStreamParameters(proxy.Destination{Conn: cc, Ctx: ctx, Msg: f}, nil, func() error { return nil }, nil), nil
		})),
	}

	praefectSocket := testhelper.GetTemporaryGitalySocketFileName(t)
	praefectLis, err := net.Listen("unix", praefectSocket)
	require.NoError(t, err)

	praefectSrv := grpc.NewServer(srvOptions...)
	defer praefectSrv.Stop()
	go praefectSrv.Serve(praefectLis)

	praefectCC, err := grpc.Dial("unix://"+praefectSocket, grpc.WithInsecure())
	require.NoError(t, err)

	simpleClient := mock.NewSimpleServiceClient(praefectCC)

	_, repo, _ := testcfg.BuildWithRepo(t)

	for i := 0; i < threshold; i++ {
		_, err = simpleClient.RepoAccessorUnary(ctx, &mock.RepoRequest{
			Repo: repo,
		})
		require.NoError(t, err)
		_, err = simpleClient.RepoMutatorUnary(ctx, &mock.RepoRequest{
			Repo: repo,
		})
		require.NoError(t, err)
	}

	assert.False(t, errTracker.WriteThresholdReached(nodeName))
	assert.False(t, errTracker.ReadThresholdReached(nodeName))

	for i := 0; i < threshold; i++ {
		_, err = simpleClient.RepoAccessorUnary(ctx, &mock.RepoRequest{
			Repo: nil,
		})
		require.Error(t, err)
		_, err = simpleClient.RepoMutatorUnary(ctx, &mock.RepoRequest{
			Repo: nil,
		})
		require.Error(t, err)
	}

	assert.True(t, errTracker.WriteThresholdReached(nodeName))
	assert.True(t, errTracker.ReadThresholdReached(nodeName))

	time.Sleep(window)

	for i := 0; i < threshold; i++ {
		_, err = simpleClient.RepoAccessorUnary(ctx, &mock.RepoRequest{
			Repo: repo,
		})
		require.NoError(t, err)
		_, err = simpleClient.RepoMutatorUnary(ctx, &mock.RepoRequest{
			Repo: repo,
		})
		require.NoError(t, err)
	}

	assert.False(t, errTracker.WriteThresholdReached(nodeName))
	assert.False(t, errTracker.ReadThresholdReached(nodeName))
}
