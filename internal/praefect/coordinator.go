package praefect

import (
	"context"
	"errors"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"google.golang.org/grpc"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/protoc-gen-go/descriptor"
	"github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/datastore"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/grpc-proxy/proxy"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/nodes"
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

func (c *Coordinator) directRepositoryScopedMessage(ctx context.Context, mi protoregistry.MethodInfo, peeker proxy.StreamModifier, fullMethodName string, m proto.Message) (*proxy.StreamParameters, error) {
	targetRepo, err := mi.TargetRepo(m)
	if err != nil {
		if err == protoregistry.ErrTargetRepoMissing {
			return nil, status.Errorf(codes.InvalidArgument, err.Error())
		}
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

	var requestFinalizer func()

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

	if mi.Scope == protoregistry.ScopeRepository {
		return c.directRepositoryScopedMessage(ctx, mi, peeker, fullMethodName, m)
	}

	// TODO: remove the need to handle non repository scoped RPCs. The only remaining one is FindRemoteRepository.
	// https://gitlab.com/gitlab-org/gitaly/issues/2442. One this issue is resolved, we can explicitly require that
	// any RPC that gets proxied through praefect must be repository scoped.
	shard, err := c.nodeMgr.GetShard(c.conf.VirtualStorages[0].Name)
	if err != nil {
		return nil, err
	}

	primary, err := shard.GetPrimary()
	if err != nil {
		return nil, err
	}

	return proxy.NewStreamParameters(ctx, primary.GetConnection(), func() {}, nil), nil
}

func (s *Coordinator) HandleWriteRef(srv interface{}, serverStream grpc.ServerStream) error {
	s.log.Error("I'm here!!!!")

	peeker := proxy.NewPeeker(serverStream)

	mi, err := s.registry.LookupMethod("/gitaly.RepositoryService/WriteRef")
	if err != nil {
		return err
	}

	frame, err := peeker.Peek()

	m, err := mi.UnmarshalRequestProto(frame)
	if err != nil {
		return err
	}

	writeRefReq, ok := m.(*gitalypb.WriteRefRequest)
	if !ok {
		return errors.New("sholud have been write ref request!")
	}

	shard, err := s.nodeMgr.GetShard(writeRefReq.GetRepository().GetStorageName())
	if err != nil {
		return err
	}

	primary, err := shard.GetPrimary()
	if err != nil {
		return err
	}

	secondaries, err := shard.GetSecondaries()
	if err != nil {
		return err
	}

	primaryRepo := &gitalypb.Repository{
		StorageName:  primary.GetStorage(),
		RelativePath: writeRefReq.GetRepository().GetRelativePath(),
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	primaryRepoClient := gitalypb.NewRepositoryServiceClient(primary.GetConnection())
	if _, err = primaryRepoClient.WriteRef(ctx, &gitalypb.WriteRefRequest{
		Repository:  primaryRepo,
		Ref:         writeRefReq.Ref,
		Revision:    writeRefReq.Revision,
		OldRevision: writeRefReq.OldRevision,
		Force:       writeRefReq.Force,
	}); err != nil {
		return err
	}

	for _, secondary := range secondaries {
		secondaryRepo := &gitalypb.Repository{
			StorageName:  secondary.GetStorage(),
			RelativePath: writeRefReq.GetRepository().GetRelativePath(),
		}
		secondaryRepoClient := gitalypb.NewRepositoryServiceClient(secondary.GetConnection())

		if _, err = secondaryRepoClient.WriteRef(ctx, &gitalypb.WriteRefRequest{
			Repository:  secondaryRepo,
			Ref:         writeRefReq.Ref,
			Revision:    writeRefReq.Revision,
			OldRevision: writeRefReq.OldRevision,
			Force:       writeRefReq.Force,
		}); err != nil {
			return err
		}
	}

	return nil
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
			// TODO: in case of error the job remains in queue in 'pending' state and leads to:
			//  - additional memory consumption
			//  - stale state of one of the git data stores
			if err := c.datastore.UpdateReplJobState(jobID, datastore.JobStateReady); err != nil {
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
