package praefect

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/protoc-gen-go/descriptor"
	"github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/internal/metadata"
	"gitlab.com/gitlab-org/gitaly/internal/middleware/proxytime"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/conn"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/datastore"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/grpc-proxy/proxy"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/models"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/protoregistry"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
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
	trailerTracker *proxytime.TrailerTracker
}

// NewCoordinator returns a new Coordinator that utilizes the provided logger
func NewCoordinator(l *logrus.Entry, ds datastore.Datastore, clientConnections *conn.ClientConnections, conf config.Config, trailerTracker *proxytime.TrailerTracker, fileDescriptors ...*descriptor.FileDescriptorProto) *Coordinator {
	registry := protoregistry.New()
	registry.RegisterFiles(fileDescriptors...)

	return &Coordinator{
		log:            l,
		datastore:      ds,
		registry:       registry,
		connections:    clientConnections,
		conf:           conf,
		trailerTracker: trailerTracker,
	}
}

// RegisterProtos allows coordinator to register new protos on the fly
func (c *Coordinator) RegisterProtos(protos ...*descriptor.FileDescriptorProto) error {
	return c.registry.RegisterFiles(protos...)
}

// streamDirector determines which downstream servers receive requests
func (c *Coordinator) streamDirector(ctx context.Context, fullMethodName string, peeker proxy.StreamModifier) (*proxy.StreamParameters, error) {
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
		var getRepoErr error
		storage, requestFinalizer, getRepoErr = c.getStorageForRepositoryMessage(mi, m, peeker, fullMethodName)

		if getRepoErr == protoregistry.ErrTargetRepoMissing {
			return nil, status.Errorf(codes.InvalidArgument, getRepoErr.Error())
		}

		if getRepoErr != nil {
			return nil, getRepoErr
		}

		if storage == "" {
			return nil, status.Error(codes.InvalidArgument, "storage not found")
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
		trailer, err := c.trailerTracker.Trailer(proxyRequestID)
		if err == nil {
			opts = append(opts, trailer)
		}
	}

	return proxy.NewStreamParameters(ctx, cc, requestFinalizer, opts), nil
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

	primary, err := c.datastore.GetPrimary(targetRepo.GetStorageName())
	if err != nil {
		return "", nil, err
	}

	secondaries, err := c.datastore.GetSecondaries(targetRepo.GetStorageName())
	if err != nil {
		return "", nil, err
	}

	// rewrite storage name
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

	// TODO: move the logic of creating replication jobs to the streamDirector method
	if mi.Operation == protoregistry.OpMutator {
		change := datastore.UpdateRepo
		if isDestructive(method) {
			change = datastore.DeleteRepo
		}

		if requestFinalizer, err = c.createReplicaJobs(targetRepo, primary, secondaries, change); err != nil {
			return "", nil, err
		}
	}

	return primary.Storage, requestFinalizer, nil
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

func (c *Coordinator) createReplicaJobs(targetRepo *gitalypb.Repository, primary models.Node, secondaries []models.Node, change datastore.ChangeType) (func(), error) {
	jobIDs, err := c.datastore.CreateReplicaReplJobs(targetRepo.RelativePath, primary, secondaries, change)
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
