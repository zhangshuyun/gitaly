package repository

import (
	"context"
	"io"
	"os"
	"path/filepath"

	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"

	gitalyauth "gitlab.com/gitlab-org/gitaly/auth"
	"gitlab.com/gitlab-org/gitaly/internal/config"
	"gitlab.com/gitlab-org/gitaly/internal/git/objectpool"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/tempdir"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/streamio"
)

func (s *server) ReplicateRepository(ctx context.Context, in *gitalypb.ReplicateRepositoryRequest) (*gitalypb.ReplicateRepositoryResponse, error) {
	if _, err := s.CreateRepository(ctx, &gitalypb.CreateRepositoryRequest{
		Repository: in.GetRepository(),
	}); err != nil {
		return nil, helper.ErrInternal(err)
	}

	g, ctx := errgroup.WithContext(ctx)
	outgoingCtx := helper.IncomingToOutgoing(ctx)

	for _, f := range []func(context.Context, *gitalypb.ReplicateRepositoryRequest) error{
		syncRepository,
		syncInfoAttributes,
		s.syncObjectPool,
	} {
		f := f // rescoping f
		g.Go(func() error { return f(outgoingCtx, in) })
	}

	if err := g.Wait(); err != nil {
		return nil, helper.ErrInternal(err)
	}

	return &gitalypb.ReplicateRepositoryResponse{}, nil
}

func syncRepository(ctx context.Context, in *gitalypb.ReplicateRepositoryRequest) error {
	remoteClient, err := newRemoteClient()
	if err != nil {
		return err
	}

	if _, err = remoteClient.FetchInternalRemote(ctx, &gitalypb.FetchInternalRemoteRequest{
		Repository:       in.GetRepository(),
		RemoteRepository: in.GetSource(),
	}); err != nil {
		return helper.ErrInternal(err)
	}

	return nil
}

func syncInfoAttributes(ctx context.Context, in *gitalypb.ReplicateRepositoryRequest) error {
	repoClient, err := newRepoClient(ctx, in.GetSource().GetStorageName())
	if err != nil {
		return err
	}

	repoPath, err := helper.GetRepoPath(in.GetRepository())
	if err != nil {
		return helper.ErrInternal(err)
	}

	infoPath := filepath.Join(repoPath, "info")
	attributesPath := filepath.Join(infoPath, "attributes")

	if err := os.MkdirAll(infoPath, 0755); err != nil {
		return helper.ErrInternal(err)
	}

	tmpDir, err := tempdir.New(ctx, in.GetRepository())
	if err != nil {
		return helper.ErrInternal(err)
	}

	attributesFile, err := os.Create(filepath.Join(tmpDir, "attributes"))
	if err != nil {
		return helper.ErrInternal(err)
	}
	defer attributesFile.Close()

	stream, err := repoClient.GetInfoAttributes(ctx, &gitalypb.GetInfoAttributesRequest{
		Repository: in.GetSource(),
	})
	if err != nil {
		return helper.ErrInternal(err)
	}

	if _, err := io.Copy(attributesFile, streamio.NewReader(func() ([]byte, error) {
		resp, err := stream.Recv()
		return resp.GetAttributes(), err
	})); err != nil {
		return helper.ErrInternal(err)
	}

	if err := os.Chmod(attributesFile.Name(), attributesFileMode); err != nil {
		return helper.ErrInternal(err)
	}

	if err = os.Rename(attributesFile.Name(), attributesPath); err != nil {
		return helper.ErrInternal(err)
	}

	return nil
}

func copyObjectPoolProto(objectPoolProto *gitalypb.ObjectPool) *gitalypb.ObjectPool {
	objectPoolProtoCp := *objectPoolProto
	objectPoolRepoCp := *objectPoolProto.Repository
	objectPoolProtoCp.Repository = &objectPoolRepoCp

	return &objectPoolProtoCp
}

func (s *server) syncObjectPool(ctx context.Context, in *gitalypb.ReplicateRepositoryRequest) error {
	objectPoolClient, err := newObjectPoolClient(ctx, in.GetSource().GetStorageName())
	if err != nil {
		return err
	}

	resp, err := objectPoolClient.GetObjectPool(ctx, &gitalypb.GetObjectPoolRequest{
		Repository: in.GetSource(),
	})
	if err != nil {
		return err
	}

	sourceObjectPoolProto := resp.GetObjectPool()
	if sourceObjectPoolProto == nil {
		return nil
	}

	targetObjectPoolProto := copyObjectPoolProto(sourceObjectPoolProto)
	targetObjectPoolProto.Repository.StorageName = in.GetRepository().GetStorageName()

	targetObjectPool, err := objectpool.FromProto(targetObjectPoolProto)
	if err != nil {
		return err
	}

	if !targetObjectPool.Exists() {
		if _, err := s.CreateRepository(ctx, &gitalypb.CreateRepositoryRequest{Repository: targetObjectPoolProto.GetRepository()}); err != nil {
			return err
		}

		remoteClient, err := newRemoteClient()

		if err != nil {
			return err
		}

		poolRepository := targetObjectPoolProto.GetRepository()

		if _, err := remoteClient.FetchInternalRemote(ctx, &gitalypb.FetchInternalRemoteRequest{
			Repository:       poolRepository,
			RemoteRepository: sourceObjectPoolProto.GetRepository(),
		}); err != nil {
			return err
		}
	}

	return targetObjectPool.Link(ctx, in.GetRepository())
}

// newRemoteClient creates a new RemoteClient that talks to the same gitaly server
func newRemoteClient() (gitalypb.RemoteServiceClient, error) {
	connOpts := []grpc.DialOption{
		grpc.WithInsecure(),
		grpc.WithPerRPCCredentials(gitalyauth.RPCCredentials(config.Config.Auth.Token)),
	}
	conn, err := grpc.Dial(config.Config.SocketPath, connOpts...)
	if err != nil {
		return nil, helper.ErrInternalf("could not dial source: %v", err)
	}

	return gitalypb.NewRemoteServiceClient(conn), nil
}

// newRepoClient creates a new RepositoryClient that talks to the gitaly of the source repository
func newRepoClient(ctx context.Context, storageName string) (gitalypb.RepositoryServiceClient, error) {
	conn, err := newClientConnection(ctx, storageName)
	if err != nil {
		return nil, err
	}

	return gitalypb.NewRepositoryServiceClient(conn), nil
}

// newObjectPoolClient creates a new RepositoryClient that talks to the gitaly of the source repository
func newObjectPoolClient(ctx context.Context, storageName string) (gitalypb.ObjectPoolServiceClient, error) {
	conn, err := newClientConnection(ctx, storageName)
	if err != nil {
		return nil, err
	}

	return gitalypb.NewObjectPoolServiceClient(conn), nil
}

func newClientConnection(ctx context.Context, storageName string) (*grpc.ClientConn, error) {
	gitalyServersInfo, err := helper.ExtractGitalyServers(ctx)
	if err != nil {
		return nil, helper.ErrInternal(err)
	}

	sourceRepositoryStorageInfo, ok := gitalyServersInfo[storageName]
	if !ok {
		return nil, helper.ErrInternalf("gitaly server info for %s not found", storageName)
	}

	connOpts := []grpc.DialOption{
		grpc.WithInsecure(),
		grpc.WithPerRPCCredentials(gitalyauth.RPCCredentials(sourceRepositoryStorageInfo["token"])),
	}

	conn, err := grpc.Dial(sourceRepositoryStorageInfo["address"], connOpts...)
	if err != nil {
		return nil, helper.ErrInternalf("could not dial source: %v", err)
	}

	return conn, nil
}
