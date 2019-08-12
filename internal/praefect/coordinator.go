package praefect

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"sort"
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
)

// Coordinator takes care of directing client requests to the appropriate
// downstream server. The coordinator is thread safe; concurrent calls to
// register nodes are safe.
type Coordinator struct {
	mutex         sync.Mutex
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

func (c *Coordinator) connDownHandler(cc *grpc.ClientConn) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	for storage, conn := range c.nodes {
		if conn == cc {
			if err := c.datastore.Failover(storage); err != nil {
				return err
			}
		}
	}

	return nil
}

// streamDirector determines which downstream servers receive requests
func (c *Coordinator) streamDirector(ctx context.Context, fullMethodName string, peeker proxy.StreamModifier) (context.Context, *grpc.ClientConn, func(), error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	// For phase 1, we need to route messages based on the storage location
	// to the appropriate Gitaly node.
	c.log.Debugf("Stream director received method %s", fullMethodName)

	c.failoverMutex.RLock()
	defer c.failoverMutex.RUnlock()

	jobUpdateFunc := func() {}

	frame, err := peeker.Peek()
	if err != nil {
		return nil, nil, jobUpdateFunc, err
	}

	mi, err := c.registry.LookupMethod(fullMethodName)
	if err != nil {
		return nil, nil, jobUpdateFunc, err
	}

	var primary *models.Node

	if mi.Scope == protoregistry.ScopeRepository {
		m, err := mi.UnmarshalRequestProto(frame)
		if err != nil {
			return nil, nil, jobUpdateFunc, err
		}

		targetRepo, err := mi.TargetRepo(m)
		if err != nil {
			return nil, nil, jobUpdateFunc, err
		}

		primary, err = c.datastore.GetPrimary(targetRepo.GetRelativePath())

		if err != nil {
			if err != ErrPrimaryNotSet {
				return nil, nil, jobUpdateFunc, err
			}
			// if there are no primaries for this repository, pick one
			nodes, err := c.datastore.GetStorageNodes()
			if err != nil {
				return nil, nil, jobUpdateFunc, err
			}

			if len(nodes) == 0 {
				return nil, nil, jobUpdateFunc, fmt.Errorf("no nodes serve storage %s", targetRepo.GetStorageName())

			}

			sort.Slice(nodes, func(i, j int) bool {
				return nodes[i].ID < nodes[j].ID
			})

			newPrimary := nodes[0]
			replicas := nodes[1:]

			// set the primary
			if err = c.datastore.SetPrimary(targetRepo.GetRelativePath(), newPrimary.ID); err != nil {
				return nil, nil, jobUpdateFunc, err
			}

			// add replicas
			for _, replica := range replicas {
				if err = c.datastore.AddReplica(targetRepo.GetRelativePath(), replica.ID); err != nil {
					return nil, nil, jobUpdateFunc, err
				}
			}

			primary = &newPrimary
		}

		targetRepo.StorageName = primary.Storage

		b, err := proxy.Codec().Marshal(m)
		if err != nil {
			return nil, nil, jobUpdateFunc, err
		}
		if err = peeker.Modify(b); err != nil {
			return nil, nil, jobUpdateFunc, err
		}
		if mi.Operation == protoregistry.OpMutator {
			jobIDs, err := c.datastore.CreateReplicaReplJobs(targetRepo.RelativePath)
			if err != nil {
				return nil, nil, nil, err
			}
			jobUpdateFunc = func() {
				for _, jobID := range jobIDs {
					c.datastore.UpdateReplJob(jobID, JobStateReady)
				}
			}
		}

	} else {
		//TODO: For now we just pick a random storage node for a non repository scoped RPC, but we will need to figure out exactly how to
		// proxy requests that are not repository scoped
		node, err := c.datastore.GetStorageNodes()
		if err != nil {
			return nil, nil, jobUpdateFunc, err
		}
		if len(node) == 0 {
			return nil, nil, jobUpdateFunc, errors.New("no node storages found")
		}
		primary = &node[0]
	}

	// We only need the primary node, as there's only one primary storage
	// location per praefect at this time
	cc, err := c.GetConnection(primary.Storage)
	if err != nil {
		return nil, nil, jobUpdateFunc, fmt.Errorf("unable to find existing client connection for %s", primary.Storage)
	}

	return helper.IncomingToOutgoing(ctx), cc, jobUpdateFunc, nil
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

// GetConnection gets the grpc client connection based on an address
func (c *Coordinator) GetConnection(storageName string) (*grpc.ClientConn, error) {
	c.connMutex.RLock()
	cc, ok := c.nodes[storageName]
	c.connMutex.RUnlock()
	if !ok {
		return nil, errors.New("client connection not found")
	}

	return cc, nil

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
		// TODO: update failover logic
		c.log.Info("failover happens")
		c.failoverMutex.Unlock()
	}
}
