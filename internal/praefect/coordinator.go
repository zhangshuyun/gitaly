package praefect

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"

	gitalyauth "gitlab.com/gitlab-org/gitaly/auth"
	gitalyconfig "gitlab.com/gitlab-org/gitaly/internal/config"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/models"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/protoregistry"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"

	"github.com/golang/protobuf/proto"
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
	log           *logrus.Entry
	failoverMutex sync.RWMutex
	connMutex     sync.RWMutex

	datastore Datastore

	nodes    map[int]*grpc.ClientConn
	registry *protoregistry.Registry
}

// NewCoordinator returns a new Coordinator that utilizes the provided logger
func NewCoordinator(l *logrus.Entry, datastore Datastore, fileDescriptors ...*descriptor.FileDescriptorProto) *Coordinator {
	registry := protoregistry.New()
	registry.RegisterFiles(fileDescriptors...)

	return &Coordinator{
		log:       l,
		datastore: datastore,
		nodes:     make(map[int]*grpc.ClientConn),
		registry:  registry,
	}
}

// RegisterProtos allows coordinator to register new protos on the fly
func (c *Coordinator) RegisterProtos(protos ...*descriptor.FileDescriptorProto) error {
	return c.registry.RegisterFiles(protos...)
}

func (c *Coordinator) connDownHandler(cc *grpc.ClientConn) error {
	c.failoverMutex.Lock()
	defer c.failoverMutex.Unlock()

	for storageNodeID, conn := range c.nodes {
		if conn == cc {
			if err := c.datastore.Failover(storageNodeID); err != nil {
				return err
			}
		}
	}

	return nil
}

// streamDirector determines which downstream servers receive requests
func (c *Coordinator) streamDirector(ctx context.Context, fullMethodName string, peeker proxy.StreamModifier) (context.Context, *grpc.ClientConn, func(), error) {
	c.failoverMutex.Lock()
	defer c.failoverMutex.Unlock()
	// For phase 1, we need to route messages based on the storage location
	// to the appropriate Gitaly node.
	c.log.Debugf("Stream director received method %s", fullMethodName)

	mi, err := c.registry.LookupMethod(fullMethodName)
	if err != nil {
		return nil, nil, nil, err
	}

	m, err := protoMessageFromPeeker(mi, peeker)
	if err != nil {
		return nil, nil, nil, err
	}

	var requestFinalizer func()
	var node models.Node

	if mi.Scope == protoregistry.ScopeRepository {
		node, requestFinalizer, err = c.getNodeForRepositoryMessage(mi, m, peeker)
		if err != nil {
			return nil, nil, nil, err
		}
	} else {
		node, requestFinalizer, err = c.getAnyStorageNode()
		if err != nil {
			return nil, nil, nil, err
		}
	}
	// We only need the primary node, as there's only one primary storage
	// location per praefect at this time
	cc, err := c.GetConnection(node.ID)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("unable to find existing client connection for node_id %d", node.ID)
	}

	return helper.IncomingToOutgoing(ctx), cc, requestFinalizer, nil
}

var noopRequestFinalizer = func() {}

func (c *Coordinator) getAnyStorageNode() (models.Node, func(), error) {
	//TODO: For now we just pick a random storage node for a non repository scoped RPC, but we will need to figure out exactly how to
	// proxy requests that are not repository scoped
	node, err := c.datastore.GetStorageNodes()
	if err != nil {
		return models.Node{}, nil, err
	}
	if len(node) == 0 {
		return models.Node{}, nil, errors.New("no node storages found")
	}

	return node[0], noopRequestFinalizer, nil
}

func (c *Coordinator) getNodeForRepositoryMessage(mi protoregistry.MethodInfo, m proto.Message, peeker proxy.StreamModifier) (models.Node, func(), error) {
	targetRepo, err := mi.TargetRepo(m)
	if err != nil {
		return models.Node{}, nil, err
	}

	primary, err := c.selectPrimary(mi, targetRepo)
	if err != nil {
		return models.Node{}, nil, err
	}

	targetRepo.StorageName = primary.Storage

	b, err := proxy.Codec().Marshal(m)
	if err != nil {
		return models.Node{}, nil, err
	}

	if err = peeker.Modify(b); err != nil {
		return models.Node{}, nil, err
	}

	requestFinalizer := noopRequestFinalizer

	if mi.Operation == protoregistry.OpMutator {
		if requestFinalizer, err = c.createReplicaJobs(targetRepo); err != nil {
			return models.Node{}, nil, err
		}
	}

	return *primary, requestFinalizer, nil
}

func (c *Coordinator) selectPrimary(mi protoregistry.MethodInfo, targetRepo *gitalypb.Repository) (*models.Node, error) {
	var primary *models.Node
	var err error

	primary, err = c.datastore.GetPrimary(targetRepo.GetRelativePath())

	if err != nil {
		if err != ErrPrimaryNotSet {
			return nil, err
		}
		// if there are no primaries for this repository, pick one
		nodes, err := c.datastore.GetStorageNodes()
		if err != nil {
			return nil, err
		}

		if len(nodes) == 0 {
			return nil, fmt.Errorf("no nodes serve storage %s", targetRepo.GetStorageName())

		}

		sort.Slice(nodes, func(i, j int) bool {
			return nodes[i].ID < nodes[j].ID
		})

		newPrimary := nodes[0]
		replicas := nodes[1:]

		// set the primary
		if err = c.datastore.SetPrimary(targetRepo.GetRelativePath(), newPrimary.ID); err != nil {
			return nil, err
		}

		// add replicas
		for _, replica := range replicas {
			if err = c.datastore.AddReplica(targetRepo.GetRelativePath(), replica.ID); err != nil {
				return nil, err
			}
		}

		primary = &newPrimary
	}

	return primary, nil
}

func protoMessageFromPeeker(mi protoregistry.MethodInfo, peeker proxy.StreamModifier) (proto.Message, error) {
	frame, err := peeker.Peek()
	if err != nil {
		return nil, err
	}

	m, err := mi.UnmarshalRequestProto(frame)

	if err != nil {
		return nil, err
	}

	return m, nil
}

func (c *Coordinator) createReplicaJobs(targetRepo *gitalypb.Repository) (func(), error) {
	jobIDs, err := c.datastore.CreateReplicaReplJobs(targetRepo.RelativePath)
	if err != nil {
		return nil, err
	}
	return func() {
		for _, jobID := range jobIDs {
			if err := c.datastore.UpdateReplJob(jobID, JobStateReady); err != nil {
				c.log.WithField("job_id", jobID).WithError(err).Errorf("error when updating replication job to %d", JobStateReady)
			}
		}
	}, nil
}

// RegisterNode will direct traffic to the supplied downstream connection when the storage location
// is encountered.
func (c *Coordinator) RegisterNode(nodeID int, listenAddr string) error {
	conn, err := client.Dial(listenAddr,
		[]grpc.DialOption{
			grpc.WithDefaultCallOptions(grpc.CallCustomCodec(proxy.Codec())),
			grpc.WithPerRPCCredentials(gitalyauth.RPCCredentials(gitalyconfig.Config.Auth.Token)),
		},
	)
	if err != nil {
		return err
	}

	c.setConn(nodeID, conn)

	c.datastore.UpdateStorageNode(nodeID, true)

	return nil
}

func (c *Coordinator) setConn(nodeID int, conn *grpc.ClientConn) {
	c.connMutex.Lock()
	c.nodes[nodeID] = conn
	c.connMutex.Unlock()
}

// GetConnection gets the grpc client connection based on an address
func (c *Coordinator) GetConnection(nodeID int) (*grpc.ClientConn, error) {
	c.connMutex.RLock()
	cc, ok := c.nodes[nodeID]
	c.connMutex.RUnlock()
	if !ok {
		return nil, errors.New("client connection not found")
	}

	return cc, nil

}
