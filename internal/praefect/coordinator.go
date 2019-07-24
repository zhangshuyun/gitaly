package praefect

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"

	gitalyauth "gitlab.com/gitlab-org/gitaly/auth"
	gitalyconfig "gitlab.com/gitlab-org/gitaly/internal/config"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/models"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/protoregistry"

	"github.com/golang/protobuf/protoc-gen-go/descriptor"
	"github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/client"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/grpc-proxy/proxy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Coordinator takes care of directing client requests to the appropriate
// downstream server. The coordinator is thread safe; concurrent calls to
// register nodes are safe.
type Coordinator struct {
	log           *logrus.Logger
	failoverMutex sync.RWMutex
	connMutex     sync.RWMutex

	datastore Datastore

	nodes    map[string]*grpc.ClientConn
	registry *protoregistry.Registry
}

// NewCoordinator returns a new Coordinator that utilizes the provided logger
func NewCoordinator(l *logrus.Logger, datastore Datastore, fileDescriptors ...*descriptor.FileDescriptorProto) *Coordinator {
	registry := protoregistry.New()
	registry.RegisterFiles(fileDescriptors...)

	return &Coordinator{
		log:       l,
		datastore: datastore,
		nodes:     make(map[string]*grpc.ClientConn),
		registry:  registry,
	}
}

// RegisterProtos allows coordinator to register new protos on the fly
func (c *Coordinator) RegisterProtos(protos ...*descriptor.FileDescriptorProto) error {
	return c.registry.RegisterFiles(protos...)
}

// GetStorageNode returns the registered node for the given storage location
func (c *Coordinator) GetStorageNode(storage string) (Node, error) {
	cc, ok := c.getConn(storage)
	if !ok {
		return Node{}, fmt.Errorf("no node registered for storage location %q", storage)
	}

	return Node{
		Storage: storage,
		cc:      cc,
	}, nil
}

// streamDirector determines which downstream servers receive requests
func (c *Coordinator) streamDirector(ctx context.Context, fullMethodName string, peeker proxy.StreamModifier) (context.Context, *grpc.ClientConn, error) {
	// For phase 1, we need to route messages based on the storage location
	// to the appropriate Gitaly node.
	c.log.Debugf("Stream director received method %s", fullMethodName)

	c.failoverMutex.RLock()
	defer c.failoverMutex.RUnlock()

	serverConfig, err := c.datastore.GetDefaultPrimary()
	if err != nil {
		err := status.Error(
			codes.FailedPrecondition,
			"no downstream node registered",
		)
		return nil, nil, err
	}

	// We only need the primary node, as there's only one primary storage
	// location per praefect at this time
	cc, ok := c.getConn(serverConfig.Name)
	if !ok {
		return nil, nil, fmt.Errorf("unable to find existing client connection for %s", serverConfig.Name)
	}

	ctx, err = helper.InjectGitalyServers(ctx, serverConfig.Name, serverConfig.ListenAddr, serverConfig.Token)
	if err != nil {
		return nil, nil, err
	}

	return ctx, cc, nil
}

// RegisterNode will direct traffic to the supplied downstream connection when the storage location
// is encountered.
func (c *Coordinator) RegisterNode(storageName, listenAddr string) error {
	conn, err := client.Dial(listenAddr,
		[]grpc.DialOption{
			grpc.WithDefaultCallOptions(grpc.CallCustomCodec(proxy.Codec())),
			grpc.WithPerRPCCredentials(gitalyauth.RPCCredentials(gitalyconfig.Config.Auth.Token)),
		},
	)
	if err != nil {
		return err
	}

	c.setConn(storageName, conn)

	return nil
}

func (c *Coordinator) setConn(storageName string, conn *grpc.ClientConn) {
	c.connMutex.Lock()
	c.nodes[storageName] = conn
	c.connMutex.Unlock()
}

func (c *Coordinator) getConn(storageName string) (*grpc.ClientConn, bool) {
	c.connMutex.RLock()
	cc, ok := c.nodes[storageName]
	c.connMutex.RUnlock()

	return cc, ok
}

// FailoverRotation waits for the SIGUSR1 signal, then promotes the next secondary to be primary
func (c *Coordinator) FailoverRotation() {
	c.handleSignalAndRotate()
}

func (c *Coordinator) handleSignalAndRotate() {
	failoverChan := make(chan os.Signal, 1)
	signal.Notify(failoverChan, syscall.SIGUSR1)

	for {
		<-failoverChan

		c.failoverMutex.Lock()
		primary, err := c.datastore.GetDefaultPrimary()
		if err != nil {
			c.log.Fatalf("error when getting default primary: %v", err)
		}

		if err := c.rotateSecondaryToPrimary(primary); err != nil {
			c.log.WithError(err).Error("rotating secondary")
		}
		c.failoverMutex.Unlock()
	}
}

func (c *Coordinator) rotateSecondaryToPrimary(primary models.GitalyServer) error {
	repositories, err := c.datastore.GetRepositoriesForPrimary(primary)
	if err != nil {
		return err
	}

	for _, repoPath := range repositories {
		secondaries, err := c.datastore.GetShardSecondaries(models.Repository{
			RelativePath: repoPath,
		})
		if err != nil {
			return fmt.Errorf("getting secondaries: %v", err)
		}

		newPrimary := secondaries[0]
		secondaries = append(secondaries[1:], primary)

		if err = c.datastore.SetShardPrimary(models.Repository{
			RelativePath: repoPath,
		}, newPrimary); err != nil {
			return fmt.Errorf("setting primary: %v", err)
		}

		if err = c.datastore.SetShardSecondaries(models.Repository{
			RelativePath: repoPath,
		}, secondaries); err != nil {
			return fmt.Errorf("setting secondaries: %v", err)
		}
	}

	// set the new default primary
	primary, err = c.datastore.GetShardPrimary(models.Repository{
		RelativePath: repositories[0],
	})
	if err != nil {
		return fmt.Errorf("getting shard primary: %v", err)
	}

	return c.datastore.SetDefaultPrimary(primary)
}
