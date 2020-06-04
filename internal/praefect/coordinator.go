package praefect

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/golang/protobuf/proto"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/internal/middleware/metadatahandler"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/datastore"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/grpc-proxy/proxy"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/metadata"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/metrics"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/nodes"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/protoregistry"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/transactions"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"gitlab.com/gitlab-org/labkit/correlation"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type ReadOnlyStorageError string

func (storage ReadOnlyStorageError) Error() string {
	return fmt.Sprintf("storage %q is in read-only mode", string(storage))
}

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
	case "/gitaly.RepositoryService/GarbageCollect":
		req, ok := m.(*gitalypb.GarbageCollectRequest)
		if !ok {
			return "", nil, fmt.Errorf("protocol changed: for method %q expected  message type '%T', got '%T'", methodName, req, m)
		}
		return datastore.GarbageCollect, datastore.Params{"CreateBitmap": req.GetCreateBitmap()}, nil
	case "/gitaly.RepositoryService/RepackFull":
		req, ok := m.(*gitalypb.RepackFullRequest)
		if !ok {
			return "", nil, fmt.Errorf("protocol changed: for method %q expected  message type '%T', got '%T'", methodName, req, m)
		}
		return datastore.RepackFull, datastore.Params{"CreateBitmap": req.GetCreateBitmap()}, nil
	case "/gitaly.RepositoryService/RepackIncremental":
		req, ok := m.(*gitalypb.RepackIncrementalRequest)
		if !ok {
			return "", nil, fmt.Errorf("protocol changed: for method %q expected  message type '%T', got '%T'", methodName, req, m)
		}
		return datastore.RepackIncremental, nil, nil

	default:
		return datastore.UpdateRepo, nil, nil
	}
}

// grpcCall is a wrapper to assemble a set of parameters that represents an gRPC call method.
type grpcCall struct {
	fullMethodName string
	methodInfo     protoregistry.MethodInfo
	msg            proto.Message
	peeker         proxy.StreamPeeker
	targetRepo     *gitalypb.Repository
}

// Coordinator takes care of directing client requests to the appropriate
// downstream server. The coordinator is thread safe; concurrent calls to
// register nodes are safe.
type Coordinator struct {
	nodeMgr  nodes.Manager
	txMgr    *transactions.Manager
	queue    datastore.ReplicationEventQueue
	registry *protoregistry.Registry
	conf     config.Config
}

// NewCoordinator returns a new Coordinator that utilizes the provided logger
func NewCoordinator(
	queue datastore.ReplicationEventQueue,
	nodeMgr nodes.Manager,
	txMgr *transactions.Manager,
	conf config.Config,
	r *protoregistry.Registry,
) *Coordinator {
	return &Coordinator{
		queue:    queue,
		registry: r,
		nodeMgr:  nodeMgr,
		txMgr:    txMgr,
		conf:     conf,
	}
}

func (c *Coordinator) directRepositoryScopedMessage(ctx context.Context, call grpcCall) (*proxy.StreamParameters, error) {
	targetRepo, err := call.methodInfo.TargetRepo(call.msg)
	if err != nil {
		return nil, helper.ErrInvalidArgument(fmt.Errorf("repo scoped: %w", err))
	}

	if targetRepo.StorageName == "" || targetRepo.RelativePath == "" {
		return nil, helper.ErrInvalidArgumentf("repo scoped: target repo is invalid")
	}

	if ctx, err = metadata.InjectPraefectServer(ctx, c.conf); err != nil {
		return nil, fmt.Errorf("repo scoped: could not inject Praefect server: %w", err)
	}

	var ps *proxy.StreamParameters
	switch call.methodInfo.Operation {
	case protoregistry.OpAccessor:
		ps, err = c.accessorStreamParameters(ctx, call, targetRepo)
	case protoregistry.OpMutator:
		ps, err = c.mutatorStreamParameters(ctx, call, targetRepo)
	default:
		err = fmt.Errorf("unknown operation type: %v", call.methodInfo.Operation)
	}

	if err != nil {
		return nil, err
	}

	return ps, nil
}

func (c *Coordinator) accessorStreamParameters(ctx context.Context, call grpcCall, targetRepo *gitalypb.Repository) (*proxy.StreamParameters, error) {
	repoPath := targetRepo.GetRelativePath()
	virtualStorage := targetRepo.StorageName

	node, err := c.nodeMgr.GetSyncedNode(ctx, virtualStorage, repoPath)
	if err != nil {
		return nil, fmt.Errorf("accessor call: get synced: %w", err)
	}

	storage := node.GetStorage()
	b, err := c.rewrittenRepositoryMessage(call.methodInfo, call.msg, storage)
	if err != nil {
		return nil, fmt.Errorf("accessor call: rewrite storage: %w", err)
	}

	metrics.ReadDistribution.WithLabelValues(virtualStorage, storage).Inc()

	return proxy.NewStreamParameters(proxy.Destination{
		Conn: node.GetConnection(),
		Msg:  b,
		Ctx:  helper.IncomingToOutgoing(ctx),
	}, nil, nil, nil), nil
}

func (c *Coordinator) injectTransaction(ctx context.Context, node nodes.Node) (context.Context, func(), error) {
	// We currently only handle single-node-transactions for the primary,
	// so we just blindly call this single node "primary".
	nodeName := "primary"

	transactionID, cancel, err := c.txMgr.RegisterTransaction(ctx, []string{nodeName})
	if err != nil {
		return nil, nil, err
	}

	ctx, err = metadata.InjectTransaction(ctx, transactionID, nodeName)
	if err != nil {
		return nil, nil, err
	}

	return ctx, cancel, nil
}

var transactionRPCs = map[string]struct{}{
	"/gitaly.SmartHTTPService/PostReceivePack": {},
	"/gitaly.SSHService/SSHReceivePack":        {},
}

func (c *Coordinator) mutatorStreamParameters(ctx context.Context, call grpcCall, targetRepo *gitalypb.Repository) (*proxy.StreamParameters, error) {
	virtualStorage := targetRepo.StorageName

	shard, err := c.nodeMgr.GetShard(virtualStorage)
	if err != nil {
		return nil, fmt.Errorf("mutator call: get shard: %w", err)
	}

	if c.conf.Failover.ReadOnlyAfterFailover && shard.IsReadOnly {
		return nil, helper.ErrPreconditionFailed(ReadOnlyStorageError(call.targetRepo.GetStorageName()))
	}

	primaryMessage, err := c.rewrittenRepositoryMessage(call.methodInfo, call.msg, shard.Primary.GetStorage())
	if err != nil {
		return nil, fmt.Errorf("mutator call: rewrite storage: %w", err)
	}

	change, params, err := getReplicationDetails(call.fullMethodName, call.msg)
	if err != nil {
		return nil, fmt.Errorf("mutator call: replication details: %w", err)
	}

	var finalizers []func()

	primaryDest := proxy.Destination{
		Conn: shard.Primary.GetConnection(),
		Msg:  primaryMessage,
		Ctx:  helper.IncomingToOutgoing(ctx),
	}

	if featureflag.IsEnabled(ctx, featureflag.ReferenceTransactions) {
		var transactionCleanup func()
		ctx, transactionCleanup, err = c.injectTransaction(ctx, shard.Primary)
		if err != nil {
			return nil, err
		}
		finalizers = append(finalizers, transactionCleanup)
		primaryDest.Ctx = helper.IncomingToOutgoing(ctx)
	}

	var secondaryDests []proxy.Destination

	if _, ok := transactionRPCs[call.fullMethodName]; ok && featureflag.IsEnabled(ctx, featureflag.ReferenceTransactions) {
		for _, secondary := range shard.Secondaries {
			secondaryMsg, err := c.rewrittenRepositoryMessage(call.methodInfo, call.msg, secondary.GetStorage())
			if err != nil {
				return nil, err
			}
			secondaryDest := proxy.Destination{
				Conn: secondary.GetConnection(),
				Msg:  secondaryMsg,
			}

			if featureflag.IsEnabled(ctx, featureflag.ReferenceTransactions) {
				var transactionCleanup func()
				ctx, transactionCleanup, err = c.injectTransaction(ctx, secondary)
				if err != nil {
					return nil, err
				}
				finalizers = append(finalizers, transactionCleanup)
				secondaryDest.Ctx = helper.IncomingToOutgoing(ctx)
			}

			secondaryDests = append(secondaryDests, secondaryDest)
		}
	}

	finalizers = append(finalizers, c.createReplicaJobs(ctx, virtualStorage, call.targetRepo, shard.Primary, shard.Secondaries, change, params))

	return proxy.NewStreamParameters(primaryDest, secondaryDests, func() {
		for _, finalizer := range finalizers {
			finalizer()
		}
	}, nil), nil
}

// streamDirector determines which downstream servers receive requests
func (c *Coordinator) StreamDirector(ctx context.Context, fullMethodName string, peeker proxy.StreamPeeker) (*proxy.StreamParameters, error) {
	// For phase 1, we need to route messages based on the storage location
	// to the appropriate Gitaly node.
	ctxlogrus.Extract(ctx).Debugf("Stream director received method %s", fullMethodName)

	mi, err := c.registry.LookupMethod(fullMethodName)
	if err != nil {
		return nil, err
	}

	frame, err := peeker.Peek()
	if err != nil {
		return nil, err
	}

	m, err := protoMessage(mi, frame)
	if err != nil {
		return nil, err
	}

	if mi.Scope == protoregistry.ScopeRepository {
		targetRepo, err := mi.TargetRepo(m)
		if err != nil {
			return nil, helper.ErrInvalidArgument(fmt.Errorf("repo scoped: %w", err))
		}

		if targetRepo.StorageName == "" || targetRepo.RelativePath == "" {
			return nil, helper.ErrInvalidArgumentf("repo scoped: target repo is invalid")
		}

		sp, err := c.directRepositoryScopedMessage(ctx, grpcCall{
			fullMethodName: fullMethodName,
			methodInfo:     mi,
			msg:            m,
			targetRepo:     targetRepo,
		},
		)
		if err != nil {
			if errors.Is(err, nodes.ErrVirtualStorageNotExist) {
				return nil, helper.ErrInvalidArgument(err)
			}
			return nil, err
		}
		return sp, nil
	}

	// TODO: remove the need to handle non repository scoped RPCs. The only remaining one is FindRemoteRepository.
	// https://gitlab.com/gitlab-org/gitaly/issues/2442. One this issue is resolved, we can explicitly require that
	// any RPC that gets proxied through praefect must be repository scoped.
	shard, err := c.nodeMgr.GetShard(c.conf.VirtualStorages[0].Name)
	if err != nil {
		if errors.Is(err, nodes.ErrVirtualStorageNotExist) {
			return nil, status.Errorf(codes.InvalidArgument, err.Error())
		}
		return nil, err
	}

	return proxy.NewStreamParameters(proxy.Destination{
		Ctx:  helper.IncomingToOutgoing(ctx),
		Conn: shard.Primary.GetConnection(),
		Msg:  frame,
	}, nil, func() {}, nil), nil
}

func (c *Coordinator) rewrittenRepositoryMessage(mi protoregistry.MethodInfo, m proto.Message, storage string) ([]byte, error) {
	targetRepo, err := mi.TargetRepo(m)
	if err != nil {
		return nil, helper.ErrInvalidArgument(err)
	}

	// rewrite storage name
	targetRepo.StorageName = storage

	additionalRepo, ok, err := mi.AdditionalRepo(m)
	if err != nil {
		return nil, helper.ErrInvalidArgument(err)
	}

	if ok {
		additionalRepo.StorageName = storage
	}

	b, err := proxy.NewCodec().Marshal(m)
	if err != nil {
		return nil, err
	}

	return b, nil
}

func protoMessage(mi protoregistry.MethodInfo, frame []byte) (proto.Message, error) {
	m, err := mi.UnmarshalRequestProto(frame)
	if err != nil {
		return nil, err
	}

	return m, nil
}

func (c *Coordinator) createReplicaJobs(
	ctx context.Context,
	virtualStorage string,
	targetRepo *gitalypb.Repository,
	primary nodes.Node,
	secondaries []nodes.Node,
	change datastore.ChangeType,
	params datastore.Params,
) func() {
	return func() {
		correlationID := c.ensureCorrelationID(ctx, targetRepo)

		var wg sync.WaitGroup
		for _, secondary := range secondaries {
			wg.Add(1)

			event := datastore.ReplicationEvent{
				Job: datastore.ReplicationJob{
					Change:            change,
					RelativePath:      targetRepo.GetRelativePath(),
					VirtualStorage:    virtualStorage,
					SourceNodeStorage: primary.GetStorage(),
					TargetNodeStorage: secondary.GetStorage(),
					Params:            params,
				},
				Meta: datastore.Params{metadatahandler.CorrelationIDKey: correlationID},
			}

			go func() {
				defer wg.Done()
				_, err := c.queue.Enqueue(ctx, event)
				if err != nil {
					ctxlogrus.Extract(ctx).WithError(err).WithFields(logrus.Fields{
						logWithReplVirtual: event.Job.VirtualStorage,
						logWithReplSource:  event.Job.SourceNodeStorage,
						logWithReplTarget:  event.Job.TargetNodeStorage,
						logWithReplChange:  event.Job.Change,
						logWithReplPath:    event.Job.RelativePath,
					}).Error("failed to persist replication event")
				}
			}()
		}
		wg.Wait()
	}
}

func (c *Coordinator) ensureCorrelationID(ctx context.Context, targetRepo *gitalypb.Repository) string {
	corrID := correlation.ExtractFromContext(ctx)
	if corrID == "" {
		var err error
		corrID, err = correlation.RandomID()
		if err != nil {
			corrID = generatePseudorandomCorrelationID(targetRepo)
		}
	}
	return corrID
}
