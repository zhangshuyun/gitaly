package praefect

import (
	"context"
	"errors"
	"os"
	"os/signal"
	"sync"
	"syscall"

	gitalyauth "gitlab.com/gitlab-org/gitaly/auth"
	"gitlab.com/gitlab-org/gitaly/client"

	"google.golang.org/grpc"

	"gitlab.com/gitlab-org/gitaly/internal/praefect/nodes"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/protoc-gen-go/descriptor"
	"github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/datastore"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/grpc-proxy/proxy"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/protoregistry"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
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
	nodeMgr       nodes.Manager
	log           *logrus.Entry
	failoverMutex sync.RWMutex

	datastore datastore.Datastore

	registry *protoregistry.Registry
	conf     config.Config
}

// NewCoordinator returns a new Coordinator that utilizes the provided logger
func NewCoordinator(l *logrus.Entry, ds datastore.Datastore, nodeMgr nodes.Manager, conf config.Config, fileDescriptors ...*descriptor.FileDescriptorProto) *Coordinator {
	registry := protoregistry.New()
	registry.RegisterFiles(fileDescriptors...)

	return &Coordinator{
		log:       l,
		datastore: ds,
		registry:  registry,
		nodeMgr:   nodeMgr,
		conf:      conf,
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

	var conn *grpc.ClientConn

	if mi.Scope == protoregistry.ScopeRepository {
		targetRepo, err := mi.TargetRepo(m)
		if err != nil {
			return nil, err
		}

		shard, err := c.nodeMgr.GetShard(targetRepo.GetStorageName())
		if err != nil {
			return nil, err
		}

		primary, err := shard.GetPrimary()

		if err != nil {
			return nil, err
		}

		if err = c.rewriteStorageForRepositoryMessage(mi, m, peeker, primary.GetStorage()); err != nil {
			if err == protoregistry.ErrTargetRepoMissing {
				return nil, status.Errorf(codes.InvalidArgument, err.Error())
			}

			return nil, err
		}

		if mi.Operation == protoregistry.OpMutator {
			change := datastore.UpdateRepo
			if isDestructive(fullMethodName) {
				change = datastore.DeleteRepo
			}

			secondaries, err := shard.GetSecondaries()
			if err != nil {
				return nil, err
			}

			if requestFinalizer, err = c.createReplicaJobs(targetRepo, primary, secondaries, change); err != nil {
				return nil, err
			}
		}

		return proxy.NewStreamParameters(ctx, primary.GetConnection(), requestFinalizer, nil), nil
	}

	conn, err = c.getAnyStorageNode()
	if err != nil {
		return nil, err
	}

	if requestFinalizer == nil {
		requestFinalizer = noopRequestFinalizer
	}

	return proxy.NewStreamParameters(ctx, conn, requestFinalizer, nil), nil
}

var noopRequestFinalizer = func() {}

func (c *Coordinator) getAnyStorageNode() (*grpc.ClientConn, error) {
	//TODO: For now we just pick a random storage node for a non repository scoped RPC, but we will need to figure out exactly how to
	// proxy requests that are not repository scoped
	nodes, err := c.datastore.GetStorageNodes()
	if err != nil {
		return nil, err
	}
	if len(nodes) == 0 {
		return nil, errors.New("no node storages found")
	}

	node := nodes[0]

	conn, err := client.Dial(node.Address,
		[]grpc.DialOption{
			grpc.WithDefaultCallOptions(grpc.CallCustomCodec(proxy.Codec())),
			grpc.WithPerRPCCredentials(gitalyauth.RPCCredentials(node.Token)),
		},
	)

	return conn, nil
}

func (c *Coordinator) rewriteStorageForRepositoryMessage(mi protoregistry.MethodInfo, m proto.Message, peeker proxy.StreamModifier, primaryStorage string) error {
	targetRepo, err := mi.TargetRepo(m)
	if err != nil {
		return err
	}

	// rewrite storage name
	targetRepo.StorageName = primaryStorage

	additionalRepo, ok, err := mi.AdditionalRepo(m)
	if err != nil {
		return err
	}

	if ok {
		additionalRepo.StorageName = primaryStorage
	}

	b, err := proxy.Codec().Marshal(m)
	if err != nil {
		return err
	}

	if err = peeker.Modify(b); err != nil {
		return err
	}

	return nil
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

func (c *Coordinator) createReplicaJobs(targetRepo *gitalypb.Repository, primary nodes.Node, secondaries []nodes.Node, change datastore.ChangeType) (func(), error) {
	var secondaryStorages []string
	for _, secondary := range secondaries {
		secondaryStorages = append(secondaryStorages, secondary.GetStorage())
	}
	jobIDs, err := c.datastore.CreateReplicaReplJobs(targetRepo.RelativePath, primary.GetStorage(), secondaryStorages, change)
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
