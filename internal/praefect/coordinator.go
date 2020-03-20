package praefect

import (
	"context"
	"fmt"

	"github.com/golang/protobuf/proto"
	"github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/datastore"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/grpc-proxy/proxy"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/nodes"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/protoregistry"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"gitlab.com/gitlab-org/labkit/correlation"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// getReplicationDetails determines the type of job and additional details based on the method name and incoming message
func getReplicationDetails(methodName string, m proto.Message) (datastore.ChangeType, datastore.Params, error) {
	switch methodName {
	case "/gitaly.RepositoryService/RemoveRepository":
		return datastore.DeleteRepo, nil, nil
	case "/gitaly.RepositoryService/RenameRepository":
		req, ok := m.(*gitalypb.RenameRepositoryRequest)
		if !ok {
			return "", nil, fmt.Errorf("protocol changed: for method %q expected  message type '%T', got '%T'", methodName, req, m)
		}
		return datastore.RenameRepo, datastore.Params{"RelativePath": req.RelativePath}, nil
	default:
		return datastore.UpdateRepo, nil, nil
	}
}

var doNotReplicate = map[string]struct{}{
	"/gitaly.RepositoryService/GarbageCollect":    struct{}{},
	"/gitaly.RepositoryService/RepackFull":        struct{}{},
	"/gitaly.RepositoryService/RepackIncremental": struct{}{},
}

func skipReplication(fullMethodName string) bool {
	_, ok := doNotReplicate[fullMethodName]
	return ok
}

// Coordinator takes care of directing client requests to the appropriate
// downstream server. The coordinator is thread safe; concurrent calls to
// register nodes are safe.
type Coordinator struct {
	nodeMgr   nodes.Manager
	log       logrus.FieldLogger
	datastore datastore.Datastore
	registry  *protoregistry.Registry
	conf      config.Config
}

// NewCoordinator returns a new Coordinator that utilizes the provided logger
func NewCoordinator(l logrus.FieldLogger, ds datastore.Datastore, nodeMgr nodes.Manager, conf config.Config, r *protoregistry.Registry) *Coordinator {
	return &Coordinator{
		log:       l,
		datastore: ds,
		registry:  r,
		nodeMgr:   nodeMgr,
		conf:      conf,
	}
}

func (c *Coordinator) directRepositoryScopedMessage(ctx context.Context, mi protoregistry.MethodInfo, peeker proxy.StreamModifier, fullMethodName string, m proto.Message) (*proxy.StreamParameters, error) {
	targetRepo, err := mi.TargetRepo(m)
	if err != nil {
		return nil, helper.ErrInvalidArgument(err)
	}

	if targetRepo.StorageName == "" || targetRepo.RelativePath == "" {
		return nil, helper.ErrInvalidArgumentf("target repo is invalid")
	}

	shard, err := c.nodeMgr.GetShard(targetRepo.GetStorageName())
	if err != nil {
		if err == nodes.ErrVirtualStorageNotExist {
			return nil, helper.ErrInvalidArgument(err)
		}
		return nil, err
	}

	primary, err := shard.GetPrimary()
	if err != nil {
		return nil, err
	}

	if err = c.rewriteStorageForRepositoryMessage(mi, m, peeker, primary.GetStorage()); err != nil {
		return nil, err
	}

	var requestFinalizer func()

	if skipReplication(fullMethodName) {
		return proxy.NewStreamParameters(ctx, primary.GetConnection(), requestFinalizer, nil), nil
	}

	if mi.Operation == protoregistry.OpMutator {
		change, params, err := getReplicationDetails(fullMethodName, m)
		if err != nil {
			return nil, err
		}

		secondaries, err := shard.GetSecondaries()
		if err != nil {
			return nil, err
		}

		if requestFinalizer, err = c.createReplicaJobs(ctx, targetRepo, primary, secondaries, change, params); err != nil {
			return nil, err
		}
	}

	return proxy.NewStreamParameters(ctx, primary.GetConnection(), requestFinalizer, nil), nil
}

// streamDirector determines which downstream servers receive requests
func (c *Coordinator) StreamDirector(ctx context.Context, fullMethodName string, peeker proxy.StreamModifier) (*proxy.StreamParameters, error) {
	// For phase 1, we need to route messages based on the storage location
	// to the appropriate Gitaly node.
	c.log.Debugf("Stream director received method %s", fullMethodName)

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
		if err == nodes.ErrVirtualStorageNotExist {
			return nil, status.Errorf(codes.InvalidArgument, err.Error())
		}
		return nil, err
	}

	primary, err := shard.GetPrimary()
	if err != nil {
		return nil, err
	}

	return proxy.NewStreamParameters(ctx, primary.GetConnection(), func() {}, nil), nil
}

func (c *Coordinator) rewriteStorageForRepositoryMessage(mi protoregistry.MethodInfo, m proto.Message, peeker proxy.StreamModifier, primaryStorage string) error {
	targetRepo, err := mi.TargetRepo(m)
	if err != nil {
		return helper.ErrInvalidArgument(err)
	}

	// rewrite storage name
	targetRepo.StorageName = primaryStorage

	additionalRepo, ok, err := mi.AdditionalRepo(m)
	if err != nil {
		return helper.ErrInvalidArgument(err)
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

func (c *Coordinator) createReplicaJobs(
	ctx context.Context,
	targetRepo *gitalypb.Repository,
	primary nodes.Node,
	secondaries []nodes.Node,
	change datastore.ChangeType,
	params datastore.Params,
) (func(), error) {
	var secondaryStorages []string
	for _, secondary := range secondaries {
		secondaryStorages = append(secondaryStorages, secondary.GetStorage())
	}

	corrID := c.ensureCorrelationID(ctx, targetRepo)

	jobIDs, err := c.datastore.CreateReplicaReplJobs(corrID, targetRepo.RelativePath, primary.GetStorage(), secondaryStorages, change, params)
	if err != nil {
		return nil, err
	}

	return func() {
		for _, jobID := range jobIDs {
			// TODO: in case of error the job remains in queue in 'pending' state and leads to:
			//  - additional memory consumption
			//  - stale state of one of the git data stores
			if err := c.datastore.UpdateReplJobState(jobID, datastore.JobStateReady); err != nil {
				c.log.WithField("job_id", jobID).WithError(err).Errorf("error when updating replication job to %q", datastore.JobStateReady)
			}
		}
	}, nil
}

func (c *Coordinator) ensureCorrelationID(ctx context.Context, targetRepo *gitalypb.Repository) string {
	corrID := correlation.ExtractFromContext(ctx)
	if corrID == "" {
		var err error
		corrID, err = correlation.RandomID()
		if err != nil {
			c.log.WithError(err).Error("unable to generate correlation ID")
			corrID = generatePseudorandomCorrelationID(targetRepo)
		}
	}
	return corrID
}
