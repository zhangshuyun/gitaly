package praefect

import (
	"context"
	"errors"
	"fmt"

	"github.com/golang/protobuf/proto"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	glerrors "gitlab.com/gitlab-org/gitaly/internal/errors"
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
	"golang.org/x/sync/errgroup"
)

// ErrRepositoryReadOnly is returned when the repository is in read-only mode. This happens
// if the primary does not have the latest changes.
var ErrRepositoryReadOnly = helper.ErrPreconditionFailedf("repository is in read-only mode")

type transactionsCondition func(context.Context) bool

func transactionsEnabled(context.Context) bool  { return true }
func transactionsDisabled(context.Context) bool { return false }
func transactionsFlag(flag featureflag.FeatureFlag) transactionsCondition {
	return func(ctx context.Context) bool {
		return featureflag.IsEnabled(ctx, flag)
	}
}

// transactionRPCs contains the list of repository-scoped mutating calls which may take part in
// transactions. An optional feature flag can be added to conditionally enable transactional
// behaviour. If none is given, it's always enabled.
var transactionRPCs = map[string]transactionsCondition{
	"/gitaly.CleanupService/ApplyBfgObjectMapStream":         transactionsFlag(featureflag.TxApplyBfgObjectMapStream),
	"/gitaly.ConflictsService/ResolveConflicts":              transactionsFlag(featureflag.TxResolveConflicts),
	"/gitaly.ObjectPoolService/FetchIntoObjectPool":          transactionsFlag(featureflag.TxFetchIntoObjectPool),
	"/gitaly.OperationService/UserApplyPatch":                transactionsFlag(featureflag.TxUserApplyPatch),
	"/gitaly.OperationService/UserCherryPick":                transactionsFlag(featureflag.TxUserCherryPick),
	"/gitaly.OperationService/UserCommitFiles":               transactionsFlag(featureflag.TxUserCommitFiles),
	"/gitaly.OperationService/UserFFBranch":                  transactionsFlag(featureflag.TxUserFFBranch),
	"/gitaly.OperationService/UserMergeBranch":               transactionsFlag(featureflag.TxUserMergeBranch),
	"/gitaly.OperationService/UserMergeToRef":                transactionsFlag(featureflag.TxUserMergeToRef),
	"/gitaly.OperationService/UserRebaseConfirmable":         transactionsFlag(featureflag.TxUserRebaseConfirmable),
	"/gitaly.OperationService/UserRevert":                    transactionsFlag(featureflag.TxUserRevert),
	"/gitaly.OperationService/UserSquash":                    transactionsFlag(featureflag.TxUserSquash),
	"/gitaly.OperationService/UserUpdateSubmodule":           transactionsFlag(featureflag.TxUserUpdateSubmodule),
	"/gitaly.RefService/DeleteRefs":                          transactionsFlag(featureflag.TxDeleteRefs),
	"/gitaly.RemoteService/AddRemote":                        transactionsFlag(featureflag.TxAddRemote),
	"/gitaly.RemoteService/FetchInternalRemote":              transactionsFlag(featureflag.TxFetchInternalRemote),
	"/gitaly.RemoteService/RemoveRemote":                     transactionsFlag(featureflag.TxRemoveRemote),
	"/gitaly.RemoteService/UpdateRemoteMirror":               transactionsFlag(featureflag.TxUpdateRemoteMirror),
	"/gitaly.RepositoryService/ApplyGitattributes":           transactionsFlag(featureflag.TxApplyGitattributes),
	"/gitaly.RepositoryService/CloneFromPool":                transactionsFlag(featureflag.TxCloneFromPool),
	"/gitaly.RepositoryService/CloneFromPoolInternal":        transactionsFlag(featureflag.TxCloneFromPoolInternal),
	"/gitaly.RepositoryService/CreateFork":                   transactionsFlag(featureflag.TxCreateFork),
	"/gitaly.RepositoryService/CreateRepositoryFromBundle":   transactionsFlag(featureflag.TxCreateRepositoryFromBundle),
	"/gitaly.RepositoryService/CreateRepositoryFromSnapshot": transactionsFlag(featureflag.TxCreateRepositoryFromSnapshot),
	"/gitaly.RepositoryService/CreateRepositoryFromURL":      transactionsFlag(featureflag.TxCreateRepositoryFromURL),
	"/gitaly.RepositoryService/FetchRemote":                  transactionsFlag(featureflag.TxFetchRemote),
	"/gitaly.RepositoryService/FetchSourceBranch":            transactionsFlag(featureflag.TxFetchSourceBranch),
	"/gitaly.RepositoryService/ReplicateRepository":          transactionsFlag(featureflag.TxReplicateRepository),
	"/gitaly.RepositoryService/WriteRef":                     transactionsFlag(featureflag.TxWriteRef),
	"/gitaly.WikiService/WikiDeletePage":                     transactionsFlag(featureflag.TxWikiDeletePage),
	"/gitaly.WikiService/WikiUpdatePage":                     transactionsFlag(featureflag.TxWikiUpdatePage),
	"/gitaly.WikiService/WikiWritePage":                      transactionsFlag(featureflag.TxWikiWritePage),

	"/gitaly.OperationService/UserCreateBranch": transactionsEnabled,
	"/gitaly.OperationService/UserCreateTag":    transactionsEnabled,
	"/gitaly.OperationService/UserDeleteBranch": transactionsEnabled,
	"/gitaly.OperationService/UserDeleteTag":    transactionsEnabled,
	"/gitaly.OperationService/UserUpdateBranch": transactionsEnabled,
	"/gitaly.SSHService/SSHReceivePack":         transactionsEnabled,
	"/gitaly.SmartHTTPService/PostReceivePack":  transactionsEnabled,

	// The following RPCs don't perform any reference updates and thus
	// shouldn't use transactions.
	"/gitaly.ObjectPoolService/CreateObjectPool":               transactionsDisabled,
	"/gitaly.ObjectPoolService/DeleteObjectPool":               transactionsDisabled,
	"/gitaly.ObjectPoolService/DisconnectGitAlternates":        transactionsDisabled,
	"/gitaly.ObjectPoolService/LinkRepositoryToObjectPool":     transactionsDisabled,
	"/gitaly.ObjectPoolService/ReduplicateRepository":          transactionsDisabled,
	"/gitaly.ObjectPoolService/UnlinkRepositoryFromObjectPool": transactionsDisabled,
	"/gitaly.RefService/PackRefs":                              transactionsDisabled,
	"/gitaly.RepositoryService/Cleanup":                        transactionsDisabled,
	"/gitaly.RepositoryService/CreateRepository":               transactionsDisabled,
	"/gitaly.RepositoryService/DeleteConfig":                   transactionsDisabled,
	"/gitaly.RepositoryService/Fsck":                           transactionsDisabled,
	"/gitaly.RepositoryService/GarbageCollect":                 transactionsDisabled,
	"/gitaly.RepositoryService/MidxRepack":                     transactionsDisabled,
	"/gitaly.RepositoryService/OptimizeRepository":             transactionsDisabled,
	"/gitaly.RepositoryService/RemoveRepository":               transactionsDisabled,
	"/gitaly.RepositoryService/RenameRepository":               transactionsDisabled,
	"/gitaly.RepositoryService/RepackFull":                     transactionsDisabled,
	"/gitaly.RepositoryService/RepackIncremental":              transactionsDisabled,
	"/gitaly.RepositoryService/RestoreCustomHooks":             transactionsDisabled,
	"/gitaly.RepositoryService/SetConfig":                      transactionsDisabled,
	"/gitaly.RepositoryService/WriteCommitGraph":               transactionsDisabled,

	// These shouldn't ever use transactions for the sake of not creating
	// cyclic dependencies.
	"/gitaly.RefTransaction/StopTransaction": transactionsDisabled,
	"/gitaly.RefTransaction/VoteTransaction": transactionsDisabled,
}

func init() {
	// Safety checks to verify that all registered RPCs are in `transactionRPCs`
	for _, method := range protoregistry.GitalyProtoPreregistered.Methods() {
		if method.Operation != protoregistry.OpMutator || method.Scope != protoregistry.ScopeRepository {
			continue
		}

		if _, ok := transactionRPCs[method.FullMethodName()]; !ok {
			panic(fmt.Sprintf("transactional RPCs miss repository-scoped mutator %q", method.FullMethodName()))
		}
	}

	// Safety checks to verify that `transactionRPCs` has no unknown RPCs
	for transactionalRPC := range transactionRPCs {
		method, err := protoregistry.GitalyProtoPreregistered.LookupMethod(transactionalRPC)
		if err != nil {
			panic(fmt.Sprintf("transactional RPC not a registered method: %q", err))
		}
		if method.Operation != protoregistry.OpMutator {
			panic(fmt.Sprintf("transactional RPC is not a mutator: %q", method.FullMethodName()))
		}
		if method.Scope != protoregistry.ScopeRepository {
			panic(fmt.Sprintf("transactional RPC is not repository-scoped: %q", method.FullMethodName()))
		}
	}
}

func shouldUseTransaction(ctx context.Context, method string) bool {
	if !featureflag.IsEnabled(ctx, featureflag.ReferenceTransactions) {
		return false
	}

	condition, ok := transactionRPCs[method]
	if !ok {
		return false
	}

	return condition(ctx)
}

// getReplicationDetails determines the type of job and additional details based on the method name and incoming message
func getReplicationDetails(methodName string, m proto.Message) (datastore.ChangeType, datastore.Params, error) {
	switch methodName {
	case "/gitaly.RepositoryService/RemoveRepository":
		return datastore.DeleteRepo, nil, nil
	case "/gitaly.RepositoryService/CreateFork",
		"/gitaly.RepositoryService/CreateRepository",
		"/gitaly.RepositoryService/CreateRepositoryFromBundle",
		"/gitaly.RepositoryService/CreateRepositoryFromSnapshot",
		"/gitaly.RepositoryService/CreateRepositoryFromURL",
		"/gitaly.RepositoryService/ReplicateRepository":
		return datastore.CreateRepo, nil, nil
	case "/gitaly.RepositoryService/RenameRepository":
		req, ok := m.(*gitalypb.RenameRepositoryRequest)
		if !ok {
			return "", nil, fmt.Errorf("protocol changed: for method %q expected message type '%T', got '%T'", methodName, req, m)
		}
		return datastore.RenameRepo, datastore.Params{"RelativePath": req.RelativePath}, nil
	case "/gitaly.RepositoryService/GarbageCollect":
		req, ok := m.(*gitalypb.GarbageCollectRequest)
		if !ok {
			return "", nil, fmt.Errorf("protocol changed: for method %q expected message type '%T', got '%T'", methodName, req, m)
		}
		return datastore.GarbageCollect, datastore.Params{"CreateBitmap": req.GetCreateBitmap()}, nil
	case "/gitaly.RepositoryService/RepackFull":
		req, ok := m.(*gitalypb.RepackFullRequest)
		if !ok {
			return "", nil, fmt.Errorf("protocol changed: for method %q expected message type '%T', got '%T'", methodName, req, m)
		}
		return datastore.RepackFull, datastore.Params{"CreateBitmap": req.GetCreateBitmap()}, nil
	case "/gitaly.RepositoryService/RepackIncremental":
		req, ok := m.(*gitalypb.RepackIncrementalRequest)
		if !ok {
			return "", nil, fmt.Errorf("protocol changed: for method %q expected message type '%T', got '%T'", methodName, req, m)
		}
		return datastore.RepackIncremental, nil, nil
	case "/gitaly.RepositoryService/Cleanup":
		req, ok := m.(*gitalypb.CleanupRequest)
		if !ok {
			return "", nil, fmt.Errorf("protocol changed: for method %q expected message type '%T', got '%T'", methodName, req, m)
		}
		return datastore.Cleanup, nil, nil
	default:
		return datastore.UpdateRepo, nil, nil
	}
}

// grpcCall is a wrapper to assemble a set of parameters that represents an gRPC call method.
type grpcCall struct {
	fullMethodName string
	methodInfo     protoregistry.MethodInfo
	msg            proto.Message
	targetRepo     *gitalypb.Repository
}

// Coordinator takes care of directing client requests to the appropriate
// downstream server. The coordinator is thread safe; concurrent calls to
// register nodes are safe.
type Coordinator struct {
	router       Router
	txMgr        *transactions.Manager
	queue        datastore.ReplicationEventQueue
	rs           datastore.RepositoryStore
	registry     *protoregistry.Registry
	conf         config.Config
	votersMetric *prometheus.HistogramVec
}

// NewCoordinator returns a new Coordinator that utilizes the provided logger
func NewCoordinator(
	queue datastore.ReplicationEventQueue,
	rs datastore.RepositoryStore,
	router Router,
	txMgr *transactions.Manager,
	conf config.Config,
	r *protoregistry.Registry,
) *Coordinator {
	maxVoters := 1
	for _, storage := range conf.VirtualStorages {
		if len(storage.Nodes) > maxVoters {
			maxVoters = len(storage.Nodes)
		}
	}

	coordinator := &Coordinator{
		queue:    queue,
		rs:       rs,
		registry: r,
		router:   router,
		txMgr:    txMgr,
		conf:     conf,
		votersMetric: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "gitaly_praefect_voters_per_transaction_total",
				Help:    "The number of voters a given transaction was created with",
				Buckets: prometheus.LinearBuckets(1, 1, maxVoters),
			},
			[]string{"virtual_storage"},
		),
	}

	return coordinator
}

func (c *Coordinator) Describe(descs chan<- *prometheus.Desc) {
	prometheus.DescribeByCollect(c, descs)
}

func (c *Coordinator) Collect(metrics chan<- prometheus.Metric) {
	c.votersMetric.Collect(metrics)
}

func (c *Coordinator) directRepositoryScopedMessage(ctx context.Context, call grpcCall) (*proxy.StreamParameters, error) {
	ctxlogrus.AddFields(ctx, logrus.Fields{
		"virtual_storage": call.targetRepo.StorageName,
		"relative_path":   call.targetRepo.RelativePath,
	})

	praefectServer, err := metadata.PraefectFromConfig(c.conf)
	if err != nil {
		return nil, fmt.Errorf("repo scoped: could not create Praefect configuration: %w", err)
	}

	if ctx, err = praefectServer.Inject(ctx); err != nil {
		return nil, fmt.Errorf("repo scoped: could not inject Praefect server: %w", err)
	}

	var ps *proxy.StreamParameters
	switch call.methodInfo.Operation {
	case protoregistry.OpAccessor:
		ps, err = c.accessorStreamParameters(ctx, call)
	case protoregistry.OpMutator:
		ps, err = c.mutatorStreamParameters(ctx, call)
	default:
		err = fmt.Errorf("unknown operation type: %v", call.methodInfo.Operation)
	}

	if err != nil {
		return nil, err
	}

	return ps, nil
}

func (c *Coordinator) accessorStreamParameters(ctx context.Context, call grpcCall) (*proxy.StreamParameters, error) {
	repoPath := call.targetRepo.GetRelativePath()
	virtualStorage := call.targetRepo.StorageName

	node, err := c.router.RouteRepositoryAccessor(ctx, virtualStorage, repoPath)
	if err != nil {
		return nil, fmt.Errorf("accessor call: route repository accessor: %w", err)
	}

	b, err := rewrittenRepositoryMessage(call.methodInfo, call.msg, node.Storage)
	if err != nil {
		return nil, fmt.Errorf("accessor call: rewrite storage: %w", err)
	}

	metrics.ReadDistribution.WithLabelValues(virtualStorage, node.Storage).Inc()

	return proxy.NewStreamParameters(proxy.Destination{
		Ctx:  helper.IncomingToOutgoing(ctx),
		Conn: node.Connection,
		Msg:  b,
	}, nil, nil, nil), nil
}

func (c *Coordinator) registerTransaction(ctx context.Context, primary RouterNode, secondaries []RouterNode) (*transactions.Transaction, transactions.CancelFunc, error) {
	var voters []transactions.Voter
	var threshold uint

	// This voting-strategy is a majority-wins one: the primary always needs to agree
	// with at least half of the secondaries.

	secondaryLen := uint(len(secondaries))

	// In order to ensure that no quorum can be reached without the primary, its number
	// of votes needs to exceed the number of secondaries.
	voters = append(voters, transactions.Voter{
		Name:  primary.Storage,
		Votes: secondaryLen + 1,
	})
	threshold = secondaryLen + 1

	for _, secondary := range secondaries {
		voters = append(voters, transactions.Voter{
			Name:  secondary.Storage,
			Votes: 1,
		})
	}

	// If we only got a single secondary (or none), we don't increase the threshold so
	// that it's allowed to disagree with the primary without blocking the transaction.
	// Otherwise, we add `Math.ceil(len(secondaries) / 2.0)`, which means that at least
	// half of the secondaries need to agree with the primary.
	if len(secondaries) > 1 {
		threshold += (secondaryLen + 1) / 2
	}

	return c.txMgr.RegisterTransaction(ctx, voters, threshold)
}

func (c *Coordinator) mutatorStreamParameters(ctx context.Context, call grpcCall) (*proxy.StreamParameters, error) {
	targetRepo := call.targetRepo
	virtualStorage := call.targetRepo.StorageName

	change, params, err := getReplicationDetails(call.fullMethodName, call.msg)
	if err != nil {
		return nil, fmt.Errorf("mutator call: replication details: %w", err)
	}

	var route RepositoryMutatorRoute
	switch change {
	case datastore.CreateRepo:
		route, err = c.router.RouteRepositoryCreation(ctx, virtualStorage)
		if err != nil {
			return nil, fmt.Errorf("route repository creation: %w", err)
		}
	default:
		route, err = c.router.RouteRepositoryMutator(ctx, virtualStorage, targetRepo.RelativePath)
		if err != nil {
			if errors.Is(err, ErrRepositoryReadOnly) {
				return nil, err
			}

			return nil, fmt.Errorf("mutator call: route repository mutator: %w", err)
		}
	}

	primaryMessage, err := rewrittenRepositoryMessage(call.methodInfo, call.msg, route.Primary.Storage)
	if err != nil {
		return nil, fmt.Errorf("mutator call: rewrite storage: %w", err)
	}

	var finalizers []func() error

	primaryDest := proxy.Destination{
		Ctx:  helper.IncomingToOutgoing(ctx),
		Conn: route.Primary.Connection,
		Msg:  primaryMessage,
	}

	var secondaryDests []proxy.Destination

	if shouldUseTransaction(ctx, call.fullMethodName) {
		c.votersMetric.WithLabelValues(virtualStorage).Observe(float64(1 + len(route.Secondaries)))

		transaction, transactionCleanup, err := c.registerTransaction(ctx, route.Primary, route.Secondaries)
		if err != nil {
			return nil, err
		}

		injectedCtx, err := metadata.InjectTransaction(ctx, transaction.ID(), route.Primary.Storage, true)
		if err != nil {
			return nil, err
		}
		primaryDest.Ctx = helper.IncomingToOutgoing(injectedCtx)

		for _, secondary := range route.Secondaries {
			secondaryMsg, err := rewrittenRepositoryMessage(call.methodInfo, call.msg, secondary.Storage)
			if err != nil {
				return nil, err
			}

			injectedCtx, err := metadata.InjectTransaction(ctx, transaction.ID(), secondary.Storage, false)
			if err != nil {
				return nil, err
			}

			secondaryDests = append(secondaryDests, proxy.Destination{
				Ctx:  helper.IncomingToOutgoing(injectedCtx),
				Conn: secondary.Connection,
				Msg:  secondaryMsg,
			})
		}

		finalizers = append(finalizers,
			transactionCleanup, c.createTransactionFinalizer(ctx, transaction, route, virtualStorage, targetRepo, change, params),
		)
	} else {
		finalizers = append(finalizers,
			c.newRequestFinalizer(
				ctx,
				virtualStorage,
				targetRepo,
				route.Primary.Storage,
				nil,
				append(routerNodesToStorages(route.Secondaries), route.ReplicationTargets...),
				change,
				params,
			))
	}

	reqFinalizer := func() error {
		var firstErr error
		for _, finalizer := range finalizers {
			err := finalizer()
			if err == nil {
				continue
			}

			if firstErr == nil {
				firstErr = err
				continue
			}

			ctxlogrus.
				Extract(ctx).
				WithError(err).
				Error("coordinator proxy stream finalizer failure")
		}
		return firstErr
	}
	return proxy.NewStreamParameters(primaryDest, secondaryDests, reqFinalizer, nil), nil
}

// StreamDirector determines which downstream servers receive requests
func (c *Coordinator) StreamDirector(ctx context.Context, fullMethodName string, peeker proxy.StreamPeeker) (*proxy.StreamParameters, error) {
	// For phase 1, we need to route messages based on the storage location
	// to the appropriate Gitaly node.
	ctxlogrus.Extract(ctx).Debugf("Stream director received method %s", fullMethodName)

	mi, err := c.registry.LookupMethod(fullMethodName)
	if err != nil {
		return nil, err
	}

	payload, err := peeker.Peek()
	if err != nil {
		return nil, err
	}

	m, err := protoMessage(mi, payload)
	if err != nil {
		return nil, err
	}

	if mi.Scope == protoregistry.ScopeRepository {
		targetRepo, err := mi.TargetRepo(m)
		if err != nil {
			return nil, helper.ErrInvalidArgument(fmt.Errorf("repo scoped: %w", err))
		}

		if err := c.validateTargetRepo(targetRepo); err != nil {
			return nil, helper.ErrInvalidArgument(fmt.Errorf("repo scoped: %w", err))
		}

		sp, err := c.directRepositoryScopedMessage(ctx, grpcCall{
			fullMethodName: fullMethodName,
			methodInfo:     mi,
			msg:            m,
			targetRepo:     targetRepo,
		})
		if err != nil {
			if errors.Is(err, nodes.ErrVirtualStorageNotExist) {
				return nil, helper.ErrInvalidArgument(err)
			}
			return nil, err
		}
		return sp, nil
	}

	if mi.Scope == protoregistry.ScopeStorage {
		return c.directStorageScopedMessage(ctx, mi, m)
	}

	return nil, helper.ErrInternalf("rpc with undefined scope %q", mi.Scope)
}

func (c *Coordinator) directStorageScopedMessage(ctx context.Context, mi protoregistry.MethodInfo, msg proto.Message) (*proxy.StreamParameters, error) {
	virtualStorage, err := mi.Storage(msg)
	if err != nil {
		return nil, helper.ErrInvalidArgument(err)
	}

	if virtualStorage == "" {
		return nil, helper.ErrInvalidArgumentf("storage scoped: target storage is invalid")
	}

	var ps *proxy.StreamParameters
	switch mi.Operation {
	case protoregistry.OpAccessor:
		ps, err = c.accessorStorageStreamParameters(ctx, mi, msg, virtualStorage)
	case protoregistry.OpMutator:
		ps, err = c.mutatorStorageStreamParameters(ctx, mi, msg, virtualStorage)
	default:
		err = fmt.Errorf("storage scope: unknown operation type: %v", mi.Operation)
	}
	return ps, err
}

func (c *Coordinator) accessorStorageStreamParameters(ctx context.Context, mi protoregistry.MethodInfo, msg proto.Message, virtualStorage string) (*proxy.StreamParameters, error) {
	node, err := c.router.RouteStorageAccessor(ctx, virtualStorage)
	if err != nil {
		if errors.Is(err, nodes.ErrVirtualStorageNotExist) {
			return nil, helper.ErrInvalidArgument(err)
		}
		return nil, helper.ErrInternalf("accessor storage scoped: route storage accessor %q: %w", virtualStorage, err)
	}

	b, err := rewrittenStorageMessage(mi, msg, node.Storage)
	if err != nil {
		return nil, helper.ErrInvalidArgument(fmt.Errorf("accessor storage scoped: %w", err))
	}

	// As this is a read operation it could be routed to another storage (not only primary) if it meets constraints
	// such as: it is healthy, it belongs to the same virtual storage bundle, etc.
	// https://gitlab.com/gitlab-org/gitaly/-/issues/2972
	primaryDest := proxy.Destination{
		Ctx:  ctx,
		Conn: node.Connection,
		Msg:  b,
	}

	return proxy.NewStreamParameters(primaryDest, nil, func() error { return nil }, nil), nil
}

func (c *Coordinator) mutatorStorageStreamParameters(ctx context.Context, mi protoregistry.MethodInfo, msg proto.Message, virtualStorage string) (*proxy.StreamParameters, error) {
	route, err := c.router.RouteStorageMutator(ctx, virtualStorage)
	if err != nil {
		if errors.Is(err, nodes.ErrVirtualStorageNotExist) {
			return nil, helper.ErrInvalidArgument(err)
		}
		return nil, helper.ErrInternalf("mutator storage scoped: get shard %q: %w", virtualStorage, err)
	}

	b, err := rewrittenStorageMessage(mi, msg, route.Primary.Storage)
	if err != nil {
		return nil, helper.ErrInvalidArgument(fmt.Errorf("mutator storage scoped: %w", err))
	}

	primaryDest := proxy.Destination{
		Ctx:  ctx,
		Conn: route.Primary.Connection,
		Msg:  b,
	}

	secondaryDests := make([]proxy.Destination, len(route.Secondaries))
	for i, secondary := range route.Secondaries {
		b, err := rewrittenStorageMessage(mi, msg, secondary.Storage)
		if err != nil {
			return nil, helper.ErrInvalidArgument(fmt.Errorf("mutator storage scoped: %w", err))
		}
		secondaryDests[i] = proxy.Destination{Ctx: ctx, Conn: secondary.Connection, Msg: b}
	}

	return proxy.NewStreamParameters(primaryDest, secondaryDests, func() error { return nil }, nil), nil
}

func rewrittenRepositoryMessage(mi protoregistry.MethodInfo, m proto.Message, storage string) ([]byte, error) {
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

func rewrittenStorageMessage(mi protoregistry.MethodInfo, m proto.Message, storage string) ([]byte, error) {
	if err := mi.SetStorage(m, storage); err != nil {
		return nil, helper.ErrInvalidArgument(err)
	}

	return proxy.NewCodec().Marshal(m)
}

func protoMessage(mi protoregistry.MethodInfo, frame []byte) (proto.Message, error) {
	m, err := mi.UnmarshalRequestProto(frame)
	if err != nil {
		return nil, err
	}

	return m, nil
}

func (c *Coordinator) createTransactionFinalizer(
	ctx context.Context,
	transaction *transactions.Transaction,
	route RepositoryMutatorRoute,
	virtualStorage string,
	targetRepo *gitalypb.Repository,
	change datastore.ChangeType,
	params datastore.Params,
) func() error {
	return func() error {
		nodeStates, err := transaction.State()
		if err != nil {
			return err
		}

		// If no subtransaction happened, then the called RPC may not be aware of
		// transactions at all. We thus need to assume it changed repository state
		// and need to create replication jobs.
		if transaction.CountSubtransactions() == 0 {
			ctxlogrus.Extract(ctx).Info("transaction did not create subtransactions")

			secondaries := make([]string, 0, len(nodeStates))
			for secondary := range nodeStates {
				if secondary == route.Primary.Storage {
					continue
				}
				secondaries = append(secondaries, secondary)
			}

			return c.newRequestFinalizer(
				ctx, virtualStorage, targetRepo, route.Primary.Storage,
				nil, secondaries, change, params)()
		}

		// If the primary node failed the transaction, then
		// there's no sense in trying to replicate from primary
		// to secondaries.
		if nodeStates[route.Primary.Storage] != transactions.VoteCommitted {
			// If the transaction was gracefully stopped, then we don't want to return
			// an explicit error here as it indicates an error somewhere else which
			// already got returned.
			if nodeStates[route.Primary.Storage] == transactions.VoteStopped {
				return nil
			}
			return fmt.Errorf("transaction: primary failed vote")
		}
		delete(nodeStates, route.Primary.Storage)

		updatedSecondaries := make([]string, 0, len(nodeStates))
		var outdatedSecondaries []string

		for node, state := range nodeStates {
			if state == transactions.VoteCommitted {
				updatedSecondaries = append(updatedSecondaries, node)
				continue
			}

			outdatedSecondaries = append(outdatedSecondaries, node)
		}

		// Replication targets were not added to the transaction, most
		// likely because they are either not healthy or out of date.
		// We thus need to make sure to create replication jobs for
		// them.
		outdatedSecondaries = append(outdatedSecondaries, route.ReplicationTargets...)

		return c.newRequestFinalizer(
			ctx, virtualStorage, targetRepo, route.Primary.Storage,
			updatedSecondaries, outdatedSecondaries, change, params)()
	}
}

func routerNodesToStorages(nodes []RouterNode) []string {
	storages := make([]string, len(nodes))
	for i, n := range nodes {
		storages[i] = n.Storage
	}
	return storages
}

func (c *Coordinator) newRequestFinalizer(
	ctx context.Context,
	virtualStorage string,
	targetRepo *gitalypb.Repository,
	primary string,
	updatedSecondaries []string,
	outdatedSecondaries []string,
	change datastore.ChangeType,
	params datastore.Params,
) func() error {
	return func() error {
		switch change {
		case datastore.UpdateRepo:
			// If this fails, the primary might have changes on it that are not recorded in the database. The secondaries will appear
			// consistent with the primary but might serve different stale data. Follow-up mutator calls will solve this state although
			// the primary will be a later generation in the mean while.
			if err := c.rs.IncrementGeneration(ctx, virtualStorage, targetRepo.GetRelativePath(), primary, updatedSecondaries); err != nil {
				return fmt.Errorf("increment generation: %w", err)
			}
		case datastore.RenameRepo:
			// Renaming a repository is not idempotent on Gitaly's side. This combined with a failure here results in a problematic state,
			// where the client receives an error but can't retry the call as the repository has already been moved on the primary.
			// Ideally the rename RPC should copy the repository instead of moving so the client can retry if this failed.
			if err := c.rs.RenameRepository(ctx, virtualStorage, targetRepo.GetRelativePath(), primary, params["RelativePath"].(string)); err != nil {
				if !errors.Is(err, datastore.RepositoryNotExistsError{}) {
					return fmt.Errorf("rename repository: %w", err)
				}

				ctxlogrus.Extract(ctx).WithError(err).Info("renamed repository does not have a store entry")
			}
		case datastore.DeleteRepo:
			// If this fails, the repository was already deleted from the primary but we end up still having a record of it in the db.
			// Ideally we would delete the record from the db first and schedule the repository for deletion later in order to avoid
			// this problem. Client can reattempt this as deleting a repository is idempotent.
			if err := c.rs.DeleteRepository(ctx, virtualStorage, targetRepo.GetRelativePath(), primary); err != nil {
				if !errors.Is(err, datastore.RepositoryNotExistsError{}) {
					return fmt.Errorf("delete repository: %w", err)
				}

				ctxlogrus.Extract(ctx).WithError(err).Info("deleted repository does not have a store entry")
			}
		case datastore.CreateRepo:
			repositorySpecificPrimariesEnabled := c.conf.Failover.ElectionStrategy == config.ElectionStrategyPerRepository
			variableReplicationFactorEnabled := repositorySpecificPrimariesEnabled &&
				c.conf.DefaultReplicationFactors()[virtualStorage] > 0

			if err := c.rs.CreateRepository(ctx,
				virtualStorage,
				targetRepo.GetRelativePath(),
				primary,
				append(updatedSecondaries, outdatedSecondaries...),
				repositorySpecificPrimariesEnabled,
				variableReplicationFactorEnabled,
			); err != nil {
				if !errors.Is(err, datastore.RepositoryExistsError{}) {
					return fmt.Errorf("create repository: %w", err)
				}

				ctxlogrus.Extract(ctx).WithError(err).Info("create repository already has a store entry")
			}
			change = datastore.UpdateRepo
		}

		correlationID := correlation.ExtractFromContextOrGenerate(ctx)

		g, ctx := errgroup.WithContext(ctx)
		for _, secondary := range outdatedSecondaries {
			event := datastore.ReplicationEvent{
				Job: datastore.ReplicationJob{
					Change:            change,
					RelativePath:      targetRepo.GetRelativePath(),
					VirtualStorage:    virtualStorage,
					SourceNodeStorage: primary,
					TargetNodeStorage: secondary,
					Params:            params,
				},
				Meta: datastore.Params{metadatahandler.CorrelationIDKey: correlationID},
			}

			g.Go(func() error {
				if _, err := c.queue.Enqueue(ctx, event); err != nil {
					return fmt.Errorf("enqueue replication event: %w", err)
				}
				return nil
			})
		}
		return g.Wait()
	}
}

func (c *Coordinator) validateTargetRepo(repo *gitalypb.Repository) error {
	if repo.GetStorageName() == "" || repo.GetRelativePath() == "" {
		return glerrors.ErrInvalidRepository
	}

	if _, found := c.conf.StorageNames()[repo.StorageName]; !found {
		// this needs to be nodes.ErrVirtualStorageNotExist error, but it will break
		// existing API contract as praefect should be a transparent proxy of the gitaly
		return glerrors.ErrInvalidRepository
	}

	return nil
}
