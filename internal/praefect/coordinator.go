package praefect

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/metadata"
	"gitlab.com/gitlab-org/gitaly/internal/middleware/proxytime"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/conn"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/datastore"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/models"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/protoregistry"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/protoc-gen-go/descriptor"
	"github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/grpc-proxy/proxy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func isDestructive(methodName string) bool {
	return methodName == "/gitaly.RepositoryService/RemoveRepository"
}

// Coordinator takes care of directing client requests to the appropriate
// downstream server. The coordinator is thread safe; concurrent calls to
// register nodes are safe.
type Coordinator struct {
	connections   *conn.ClientConnections
	log           *logrus.Entry
	failoverMutex sync.RWMutex

	datastore datastore.Datastore

	registry       *protoregistry.Registry
	conf           config.Config
	trailerTracker proxytime.TrailerTracker
}

// NewCoordinator returns a new Coordinator that utilizes the provided logger
func NewCoordinator(l *logrus.Entry, ds datastore.Datastore, clientConnections *conn.ClientConnections, conf config.Config, fileDescriptors ...*descriptor.FileDescriptorProto) *Coordinator {
	registry := protoregistry.New()
	registry.RegisterFiles(fileDescriptors...)

	return &Coordinator{
		log:            l,
		datastore:      ds,
		registry:       registry,
		connections:    clientConnections,
		conf:           conf,
		trailerTracker: proxytime.NewTrailerTracker(),
	}
}

// RegisterProtos allows coordinator to register new protos on the fly
func (c *Coordinator) RegisterProtos(protos ...*descriptor.FileDescriptorProto) error {
	return c.registry.RegisterFiles(protos...)
}

// streamDirector determines which downstream servers receive requests
func (c *Coordinator) streamDirector(ctx context.Context, fullMethodName string, peeker proxy.StreamModifier) (proxy.StreamParameters, error) {
	// For phase 1, we need to route messages based on the storage location
	// to the appropriate Gitaly node.
	c.log.Debugf("Stream director received method %s", fullMethodName)

	c.failoverMutex.RLock()
	defer c.failoverMutex.RUnlock()

	mi, err := c.registry.LookupMethod(fullMethodName)
	if err != nil {
		return nil, err
	}

	m, err := protoMessageFromPeeker(mi, peeker)
	if err != nil {
		return nil, err
	}

	var requestFinalizer func()
	var storage string

	if mi.Scope == protoregistry.ScopeRepository {
		storage, requestFinalizer, err = c.getStorageForRepositoryMessage(mi, m, peeker, fullMethodName)
		if err != nil {
			return nil, err
		}
	} else {
		storage, requestFinalizer, err = c.getAnyStorageNode()
		if err != nil {
			return nil, err
		}
	}
	// We only need the primary node, as there's only one primary storage
	// location per praefect at this time
	cc, err := c.connections.GetConnection(storage)
	if err != nil {
		return nil, fmt.Errorf("unable to find existing client connection for %s", storage)
	}

	var opts []grpc.CallOption
	proxyRequestID := metadata.GetValue(ctx, proxytime.RequestIDKey)
	if proxyRequestID != "" {
		opts = append(opts, c.trailerTracker.Trailer(proxyRequestID))
	}

	return &StreamParameters{
		ctx:          helper.IncomingToOutgoing(ctx),
		conn:         cc,
		reqFinalizer: requestFinalizer,
		callOptions:  opts,
	}, nil
}

type StreamParameters struct {
	ctx          context.Context
	conn         *grpc.ClientConn
	reqFinalizer func()
	callOptions  []grpc.CallOption
}

func (s *StreamParameters) Context() context.Context {
	return s.ctx
}

func (s *StreamParameters) Conn() *grpc.ClientConn {
	return s.conn
}

func (s *StreamParameters) RequestFinalizer() func() {
	return s.reqFinalizer
}

func (s *StreamParameters) CallOptions() []grpc.CallOption {
	return s.callOptions
}

var noopRequestFinalizer = func() {}

func (c *Coordinator) getAnyStorageNode() (string, func(), error) {
	//TODO: For now we just pick a random storage node for a non repository scoped RPC, but we will need to figure out exactly how to
	// proxy requests that are not repository scoped
	node, err := c.datastore.GetStorageNodes()
	if err != nil {
		return "", nil, err
	}
	if len(node) == 0 {
		return "", nil, errors.New("no node storages found")
	}

	return node[0].Storage, noopRequestFinalizer, nil
}

func (c *Coordinator) getStorageForRepositoryMessage(mi protoregistry.MethodInfo, m proto.Message, peeker proxy.StreamModifier, method string) (string, func(), error) {
	targetRepo, err := mi.TargetRepo(m)
	if err != nil {
		return "", nil, err
	}

	if targetRepo.StorageName != c.conf.VirtualStorageName {
		return "", nil, status.Errorf(codes.InvalidArgument, "only messages for %s are allowed", c.conf.VirtualStorageName)
	}

	primary, err := c.selectPrimary(mi, targetRepo)
	if err != nil {
		return "", nil, err
	}

	targetRepo.StorageName = primary.Storage

	additionalRepo, ok, err := mi.AdditionalRepo(m)
	if err != nil {
		return "", nil, err
	}

	if ok {
		additionalRepo.StorageName = primary.Storage
	}

	b, err := proxy.Codec().Marshal(m)
	if err != nil {
		return "", nil, err
	}

	if err = peeker.Modify(b); err != nil {
		return "", nil, err
	}

	requestFinalizer := noopRequestFinalizer

	if mi.Operation == protoregistry.OpMutator {
		change := datastore.UpdateRepo
		if isDestructive(method) {
			change = datastore.DeleteRepo
		}

		if requestFinalizer, err = c.createReplicaJobs(targetRepo, change); err != nil {
			return "", nil, err
		}
	}

	return primary.Storage, requestFinalizer, nil
}

func (c *Coordinator) selectPrimary(mi protoregistry.MethodInfo, targetRepo *gitalypb.Repository) (*models.Node, error) {
	var primary *models.Node
	var err error

	primary, err = c.datastore.GetPrimary(targetRepo.GetRelativePath())

	if err != nil {
		if err != datastore.ErrPrimaryNotSet {
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

		newPrimary, err := c.datastore.PickAPrimary()
		if err != nil {
			return nil, fmt.Errorf("could not choose a primary: %v", err)
		}

		// set the primary
		if err = c.datastore.SetPrimary(targetRepo.GetRelativePath(), newPrimary.ID); err != nil {
			return nil, err
		}

		// add replicas
		for _, replica := range nodes {
			if replica.DefaultPrimary {
				continue
			}
			if err = c.datastore.AddReplica(targetRepo.GetRelativePath(), replica.ID); err != nil {
				return nil, err
			}
		}

		return newPrimary, nil
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

func (c *Coordinator) createReplicaJobs(targetRepo *gitalypb.Repository, change datastore.ChangeType) (func(), error) {
	jobIDs, err := c.datastore.CreateReplicaReplJobs(targetRepo.RelativePath, change)
	if err != nil {
		return nil, err
	}

	return func() {
		for _, jobID := range jobIDs {
			if err := c.datastore.UpdateReplJob(jobID, datastore.JobStateReady); err != nil {
				c.log.WithField("job_id", jobID).WithError(err).Errorf("error when updating replication job to %d", datastore.JobStateReady)
			}
		}
	}, nil
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
