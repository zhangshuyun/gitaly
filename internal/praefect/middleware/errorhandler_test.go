package middleware

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/grpc-proxy/proxy"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/mock"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/protoregistry"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func TestErrorTracker_IncrErrors(t *testing.T) {
	writeThreshold, readThreshold := 10, 10
	errors := NewErrors(100000, readThreshold, writeThreshold)

	node := "backend-node-1"

	assert.False(t, errors.WriteThresholdReached(node))
	assert.False(t, errors.ReadThresholdReached(node))

	for i := 0; i < writeThreshold; i++ {
		errors.IncrWriteErr(node)
	}

	assert.True(t, errors.WriteThresholdReached(node))

	for i := 0; i < readThreshold; i++ {
		errors.IncrReadErr(node)
	}

	assert.True(t, errors.ReadThresholdReached(node))

	errors.clear(time.Now())

	assert.False(t, errors.WriteThresholdReached(node))
	assert.False(t, errors.ReadThresholdReached(node))
}

func TestErrorTracker_ClearErrors(t *testing.T) {
	writeThreshold, readThreshold := 10, 10
	errors := NewErrors(100000, readThreshold, writeThreshold)

	node := "backend-node-1"

	errors.IncrWriteErr(node)
	errors.IncrReadErr(node)

	time.Sleep(10 * time.Millisecond)
	clearErrorsOlderThan := time.Now()
	time.Sleep(10 * time.Millisecond)

	errors.IncrWriteErr(node)
	errors.IncrReadErr(node)

	errors.clear(clearErrorsOlderThan)
	assert.Len(t, errors.readErrors[node], 1, "clear should only have cleared the read error older than the time specifiied")
	assert.Len(t, errors.writeErrors[node], 1, "clear should only have cleared the write error older than the time specifiied")
}

type simpleService struct{}

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

func (s *simpleService) ServerAccessor(ctx context.Context, in *mock.SimpleRequest) (*mock.SimpleResponse, error) {
	return &mock.SimpleResponse{}, nil
}

func TestStreamInterceptor(t *testing.T) {
	threshold := 5
	errTracker := NewErrors(10000, threshold, threshold)
	nodeName := "node-1"

	internalSrv := grpc.NewServer()

	internalServerSocketPath := testhelper.GetTemporaryGitalySocketFileName()
	lis, err := net.Listen("unix", internalServerSocketPath)

	gz := proto.FileDescriptor("mock.proto")
	fmt.Printf("\nGZ SIZE: %d\n", len(gz))
	fd, err := protoregistry.ExtractFileDescriptor(gz)
	require.NoError(t, err)

	registry, err := protoregistry.New(fd)
	require.NoError(t, err)

	require.NoError(t, err)
	mock.RegisterSimpleServiceServer(internalSrv, &simpleService{})
	reflection.Register(internalSrv)

	go internalSrv.Serve(lis)
	defer internalSrv.Stop()

	srvOptions := []grpc.ServerOption{
		grpc.CustomCodec(proxy.NewCodec()),
		grpc.UnknownServiceHandler(proxy.TransparentHandler(func(ctx context.Context,
			fullMethodName string,
			peeker proxy.StreamModifier,
		) (*proxy.StreamParameters, error) {
			cc, err := grpc.Dial("unix://"+internalServerSocketPath,
				grpc.WithDefaultCallOptions(grpc.ForceCodec(proxy.NewCodec())),
				grpc.WithInsecure(),
				grpc.WithStreamInterceptor(StreamErrorHandler(registry, errTracker, nodeName)),
			)
			require.NoError(t, err)
			return proxy.NewStreamParameters(ctx, cc, func() {}, nil), nil
		})),
	}

	praefectSocket := testhelper.GetTemporaryGitalySocketFileName()
	praefectLis, err := net.Listen("unix", praefectSocket)
	require.NoError(t, err)

	praefectSrv := grpc.NewServer(srvOptions...)
	defer praefectSrv.Stop()
	go praefectSrv.Serve(praefectLis)

	praefectCC, err := grpc.Dial("unix://"+praefectSocket, grpc.WithInsecure())
	require.NoError(t, err)

	simpleClient := mock.NewSimpleServiceClient(praefectCC)

	testRepo, _, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()

	ctx, cancel := testhelper.Context()
	defer cancel()

	for i := 0; i < threshold; i++ {
		_, err = simpleClient.RepoAccessorUnary(ctx, &mock.RepoRequest{
			Repo: testRepo,
		})
		require.NoError(t, err)
		_, err = simpleClient.RepoMutatorUnary(ctx, &mock.RepoRequest{
			Repo: testRepo,
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
}
