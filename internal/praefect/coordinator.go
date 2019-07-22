package praefect

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"gitlab.com/gitlab-org/gitaly-proto/go/gitalypb"
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
)

// Coordinator takes care of directing client requests to the appropriate
// downstream server. The coordinator is thread safe; concurrent calls to
// register nodes are safe.
type Coordinator struct {
	log           *logrus.Logger
	failoverMutex sync.RWMutex
	connMutex     sync.RWMutex

	datastore ReplicasDatastore

	nodes    map[string]*grpc.ClientConn
	registry *protoregistry.Registry
}

// NewCoordinator returns a new Coordinator that utilizes the provided logger
func NewCoordinator(l *logrus.Logger, datastore ReplicasDatastore, fileDescriptors ...*descriptor.FileDescriptorProto) *Coordinator {
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
func (c *Coordinator) GetStorageNode(address string) (Node, error) {
	cc, ok := c.getConn(address)
	if !ok {
		return Node{}, fmt.Errorf("no node registered for storage location %q", address)
	}

	return Node{
		Address: address,
		cc:      cc,
	}, nil
}

func targetRepo(mi protoregistry.MethodInfo, frame []byte) (*gitalypb.Repository, error) {
	m, err := mi.UnmarshalRequestProto(frame)
	if err != nil {
		return nil, err
	}

	targetRepo, err := mi.TargetRepo(m)
	if err != nil {
		return nil, err
	}

	return targetRepo, nil
}

// streamDirector determines which downstream servers receive requests
func (c *Coordinator) streamDirector(ctx context.Context, fullMethodName string, peeker proxy.StreamPeeker) (context.Context, *grpc.ClientConn, error) {
	// For phase 1, we need to route messages based on the storage location
	// to the appropriate Gitaly node.
	c.log.Debugf("Stream director received method %s", fullMethodName)

	c.failoverMutex.RLock()
	defer c.failoverMutex.RUnlock()

	frames, err := peeker.Peek(ctx, 1)
	if err != nil {
		return nil, nil, err
	}

	mi, err := c.registry.LookupMethod(fullMethodName)
	if err != nil {
		return nil, nil, err
	}

	var primary *models.StorageNode

	if mi.Scope == protoregistry.ScopeRepository {
		targetRepo, err := targetRepo(mi, frames[0])
		if err != nil {
			return nil, nil, err
		}

		primary, err = c.datastore.GetPrimary(targetRepo.GetRelativePath())
		if err != nil {
			if err != sql.ErrNoRows {
				return nil, nil, err
			}

			// if there are no primaries for this repository, pick one
			nodeStorages, err := c.datastore.GetNodesForStorage(targetRepo.GetStorageName())
			if err != nil {
				return nil, nil, err
			}

			if len(nodeStorages) == 0 {
				return nil, nil, fmt.Errorf("no nodes serve storage %s", targetRepo.GetStorageName())

			}
			newPrimary := nodeStorages[rand.New(rand.NewSource(time.Now().Unix())).Intn(len(nodeStorages))]

			// set the primary
			if err = c.datastore.SetPrimary(targetRepo.GetRelativePath(), newPrimary.ID); err != nil {
				return nil, nil, err
			}

			primary = &newPrimary
		}
	} else {
		//TODO: For now we just pick a random storage node for a non repository scoped RPC, but we will need to figure out exactly how to
		// proxy requests that are not repository scoped
		nodeStorages, err := c.datastore.GetNodeStorages()
		if err != nil {
			return nil, nil, err
		}
		if len(nodeStorages) == 0 {
			return nil, nil, errors.New("no node storages found")
		}
		primary = &nodeStorages[0]
	}

	// We only need the primary node, as there's only one primary storage
	// location per praefect at this time
	cc, ok := c.getConn(primary.Address)
	if !ok {
		return nil, nil, fmt.Errorf("unable to find existing client connection for %s", primary.Address)
	}

	ctx, err = helper.InjectGitalyServers(ctx, primary.StorageName, primary.Address, "")
	if err != nil {
		return nil, nil, err
	}

	return ctx, cc, nil
}

// RegisterNode will direct traffic to the supplied downstream connection when the storage location
// is encountered.
func (c *Coordinator) RegisterNode(address string) error {
	conn, err := client.Dial(address,
		[]grpc.DialOption{
			//lint:ignore SA1019 grpc-proxy only exposes grpc.Codec
			grpc.WithDefaultCallOptions(grpc.CallCustomCodec(proxy.Codec())),
			grpc.WithPerRPCCredentials(gitalyauth.RPCCredentials(gitalyconfig.Config.Auth.Token)),
		},
	)
	if err != nil {
		return err
	}

	c.setConn(address, conn)

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
		c.log.Info("failover happens")
		c.failoverMutex.Unlock()
	}
}
