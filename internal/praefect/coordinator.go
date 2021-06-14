package praefect

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	glerrors "gitlab.com/gitlab-org/gitaly/v14/internal/errors"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/v14/internal/middleware/metadatahandler"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/commonerr"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/datastore"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/grpc-proxy/proxy"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/metrics"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/nodes"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/protoregistry"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/transactions"
	"gitlab.com/gitlab-org/gitaly/v14/internal/transaction/txinfo"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"gitlab.com/gitlab-org/labkit/correlation"
	"golang.org/x/sync/errgroup"
	grpc_metadata "google.golang.org/grpc/metadata"
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
	"/gitaly.CleanupService/ApplyBfgObjectMapStream":         transactionsEnabled,
	"/gitaly.ConflictsService/ResolveConflicts":              transactionsEnabled,
	"/gitaly.ObjectPoolService/FetchIntoObjectPool":          transactionsEnabled,
	"/gitaly.OperationService/UserApplyPatch":                transactionsEnabled,
	"/gitaly.OperationService/UserCherryPick":                transactionsEnabled,
	"/gitaly.OperationService/UserCommitFiles":               transactionsEnabled,
	"/gitaly.OperationService/UserCreateBranch":              transactionsEnabled,
	"/gitaly.OperationService/UserCreateTag":                 transactionsEnabled,
	"/gitaly.OperationService/UserDeleteBranch":              transactionsEnabled,
	"/gitaly.OperationService/UserDeleteTag":                 transactionsEnabled,
	"/gitaly.OperationService/UserFFBranch":                  transactionsEnabled,
	"/gitaly.OperationService/UserMergeBranch":               transactionsEnabled,
	"/gitaly.OperationService/UserMergeToRef":                transactionsEnabled,
	"/gitaly.OperationService/UserRebaseConfirmable":         transactionsEnabled,
	"/gitaly.OperationService/UserRevert":                    transactionsEnabled,
	"/gitaly.OperationService/UserSquash":                    transactionsEnabled,
	"/gitaly.OperationService/UserUpdateBranch":              transactionsEnabled,
	"/gitaly.OperationService/UserUpdateSubmodule":           transactionsEnabled,
	"/gitaly.RefService/DeleteRefs":                          transactionsEnabled,
	"/gitaly.RemoteService/AddRemote":                        transactionsEnabled,
	"/gitaly.RemoteService/FetchInternalRemote":              transactionsEnabled,
	"/gitaly.RemoteService/RemoveRemote":                     transactionsEnabled,
	"/gitaly.RepositoryService/ApplyGitattributes":           transactionsEnabled,
	"/gitaly.RepositoryService/CloneFromPool":                transactionsEnabled,
	"/gitaly.RepositoryService/CloneFromPoolInternal":        transactionsEnabled,
	"/gitaly.RepositoryService/CreateFork":                   transactionsEnabled,
	"/gitaly.RepositoryService/CreateRepository":             transactionsEnabled,
	"/gitaly.RepositoryService/CreateRepositoryFromBundle":   transactionsEnabled,
	"/gitaly.RepositoryService/CreateRepositoryFromSnapshot": transactionsEnabled,
	"/gitaly.RepositoryService/CreateRepositoryFromURL":      transactionsEnabled,
	"/gitaly.RepositoryService/FetchRemote":                  transactionsEnabled,
	"/gitaly.RepositoryService/FetchSourceBranch":            transactionsEnabled,
	"/gitaly.RepositoryService/ReplicateRepository":          transactionsEnabled,
	"/gitaly.RepositoryService/WriteRef":                     transactionsEnabled,
	"/gitaly.SSHService/SSHReceivePack":                      transactionsEnabled,
	"/gitaly.SmartHTTPService/PostReceivePack":               transactionsEnabled,
	"/gitaly.WikiService/WikiUpdatePage":                     transactionsEnabled,
	"/gitaly.WikiService/WikiWritePage":                      transactionsEnabled,

	"/gitaly.RepositoryService/SetConfig":    transactionsFlag(featureflag.TxConfig),
	"/gitaly.RepositoryService/DeleteConfig": transactionsFlag(featureflag.TxConfig),

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
	"/gitaly.RepositoryService/GarbageCollect":                 transactionsDisabled,
	"/gitaly.RepositoryService/MidxRepack":                     transactionsDisabled,
	"/gitaly.RepositoryService/OptimizeRepository":             transactionsDisabled,
	"/gitaly.RepositoryService/RemoveRepository":               transactionsDisabled,
	"/gitaly.RepositoryService/RenameRepository":               transactionsDisabled,
	"/gitaly.RepositoryService/RepackFull":                     transactionsDisabled,
	"/gitaly.RepositoryService/RepackIncremental":              transactionsDisabled,
	"/gitaly.RepositoryService/RestoreCustomHooks":             transactionsDisabled,
	"/gitaly.RepositoryService/WriteCommitGraph":               transactionsDisabled,

	// These shouldn't ever use transactions for the sake of not creating
	// cyclic dependencies.
	"/gitaly.RefTransaction/StopTransaction": transactionsDisabled,
	"/gitaly.RefTransaction/VoteTransaction": transactionsDisabled,
}

// forcePrimaryRoutingRPCs tracks RPCs which need to always get routed to the primary. This should
// really be a last-resort measure for a given RPC, so each RPC added must have a strong reason why
// it's being added.
var forcePrimaryRPCs = map[string]bool{
	// GetObjectDirectorySize depends on a repository's on-disk state. It depends on when a
	// repository was last packed and on git-pack-objects(1) producing deterministic results.
	// Given that we can neither guarantee that replicas are always packed at the same time,
	// nor that git-pack-objects(1) produces the same packs. We always report sizes for the
	// primary node.
	"/gitaly.RepositoryService/GetObjectDirectorySize": true,
	// Same reasoning as for GetObjectDirectorySize.
	"/gitaly.RepositoryService/RepositorySize": true,
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
	case "/gitaly.RefService/PackRefs":
		req, ok := m.(*gitalypb.PackRefsRequest)
		if !ok {
			return "", nil, fmt.Errorf("protocol changed: for method %q expected message type '%T', got '%T'", methodName, req, m)
		}
		return datastore.PackRefs, nil, nil
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
	router                   Router
	txMgr                    *transactions.Manager
	queue                    datastore.ReplicationEventQueue
	rs                       datastore.RepositoryStore
	registry                 *protoregistry.Registry
	conf                     config.Config
	votersMetric             *prometheus.HistogramVec
	txReplicationCountMetric *prometheus.CounterVec
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
		txReplicationCountMetric: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "gitaly_praefect_tx_replications_total",
				Help: "The number of replication jobs scheduled for transactional RPCs",
			},
			[]string{"reason"},
		),
	}

	return coordinator
}

func (c *Coordinator) Describe(descs chan<- *prometheus.Desc) {
	prometheus.DescribeByCollect(c, descs)
}

func (c *Coordinator) Collect(metrics chan<- prometheus.Metric) {
	c.votersMetric.Collect(metrics)
	c.txReplicationCountMetric.Collect(metrics)
}

func (c *Coordinator) directRepositoryScopedMessage(ctx context.Context, call grpcCall) (*proxy.StreamParameters, error) {
	ctxlogrus.AddFields(ctx, logrus.Fields{
		"virtual_storage": call.targetRepo.StorageName,
		"relative_path":   call.targetRepo.RelativePath,
	})

	var err error
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

func shouldRouteRepositoryAccessorToPrimary(ctx context.Context, call grpcCall) bool {
	forcePrimary := forcePrimaryRPCs[call.fullMethodName]

	// In case the call's metadata tells us to force-route to the primary, then we must abide
	// and ignore what `forcePrimaryRPCs` says.
	if md, ok := grpc_metadata.FromIncomingContext(ctx); ok {
		header := md.Get(routeRepositoryAccessorPolicy)
		if len(header) == 0 {
			return forcePrimary
		}

		if header[0] == routeRepositoryAccessorPolicyPrimaryOnly {
			return true
		}
	}

	return forcePrimary
}

func (c *Coordinator) accessorStreamParameters(ctx context.Context, call grpcCall) (*proxy.StreamParameters, error) {
	repoPath := call.targetRepo.GetRelativePath()
	virtualStorage := call.targetRepo.StorageName

	node, err := c.router.RouteRepositoryAccessor(
		ctx, virtualStorage, repoPath, shouldRouteRepositoryAccessorToPrimary(ctx, call),
	)
	if err != nil {
		if errors.As(err, new(commonerr.RepositoryNotFoundError)) {
			return nil, helper.ErrNotFound(err)
		}

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

func (c *Coordinator) registerTransaction(ctx context.Context, primary RouterNode, secondaries []RouterNode) (transactions.Transaction, transactions.CancelFunc, error) {
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

type nodeErrors struct {
	sync.Mutex
	errByNode map[string]error
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
			return nil, fmt.Errorf("%w: %v %v", err, route.Primary, route.Secondaries)
		}
		finalizers = append(finalizers, transactionCleanup)

		nodeErrors := &nodeErrors{
			errByNode: make(map[string]error),
		}

		injectedCtx, err := txinfo.InjectTransaction(ctx, transaction.ID(), route.Primary.Storage, true)
		if err != nil {
			return nil, err
		}
		primaryDest.Ctx = helper.IncomingToOutgoing(injectedCtx)
		primaryDest.ErrHandler = func(err error) error {
			nodeErrors.Lock()
			defer nodeErrors.Unlock()
			nodeErrors.errByNode[route.Primary.Storage] = err
			return err
		}

		for _, secondary := range route.Secondaries {
			secondaryMsg, err := rewrittenRepositoryMessage(call.methodInfo, call.msg, secondary.Storage)
			if err != nil {
				return nil, err
			}

			injectedCtx, err := txinfo.InjectTransaction(ctx, transaction.ID(), secondary.Storage, false)
			if err != nil {
				return nil, err
			}

			secondaryDests = append(secondaryDests, proxy.Destination{
				Ctx:  helper.IncomingToOutgoing(injectedCtx),
				Conn: secondary.Connection,
				Msg:  secondaryMsg,
				ErrHandler: func(err error) error {
					nodeErrors.Lock()
					defer nodeErrors.Unlock()
					nodeErrors.errByNode[secondary.Storage] = err

					ctxlogrus.Extract(ctx).WithError(err).
						Error("proxying to secondary failed")

					// For now, any errors returned by secondaries are ignored.
					// This is mostly so that we do not abort transactions which
					// are ongoing and may succeed even with a subset of
					// secondaries bailing out.
					return nil
				},
			})
		}

		finalizers = append(finalizers,
			c.createTransactionFinalizer(ctx, transaction, route, virtualStorage,
				targetRepo, change, params, call.fullMethodName, nodeErrors),
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
				call.fullMethodName,
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
	transaction transactions.Transaction,
	route RepositoryMutatorRoute,
	virtualStorage string,
	targetRepo *gitalypb.Repository,
	change datastore.ChangeType,
	params datastore.Params,
	cause string,
	nodeErrors *nodeErrors,
) func() error {
	return func() error {
		primaryDirtied, updated, outdated := getUpdatedAndOutdatedSecondaries(
			ctx, route, transaction, nodeErrors, c.txReplicationCountMetric)
		if !primaryDirtied {
			// If the primary replica was not modified then we don't need to consider the secondaries
			// outdated. Praefect requires the primary to be always part of the quorum, so no changes
			// to secondaries would be made without primary being in agreement.
			return nil
		}

		return c.newRequestFinalizer(
			ctx, virtualStorage, targetRepo, route.Primary.Storage,
			updated, outdated, change, params, cause)()
	}
}

// getUpdatedAndOutdatedSecondaries returns all nodes which can be considered up-to-date or outdated
// after the given transaction. A node is considered outdated, if one of the following is true:
//
// - No subtransactions were created and the RPC was successful on the primary. This really is only
//   a safeguard in case the RPC wasn't aware of transactions and thus failed to correctly assert
//   its state matches across nodes. This is rather pessimistic, as it could also indicate that an
//   RPC simply didn't change anything. If the RPC was a failure on the primary and there were no
//   subtransactions, we assume no changes were done and that the nodes failed prior to voting.
//
// - The node failed to be part of the quorum. As a special case, if the primary fails the vote, all
//   nodes need to get replication jobs.
//
// - The node has errored. As a special case, if the primary fails all nodes need to get replication
//   jobs.
//
// Note that this function cannot and should not fail: if anything goes wrong, we need to create
// replication jobs to repair state.
func getUpdatedAndOutdatedSecondaries(
	ctx context.Context,
	route RepositoryMutatorRoute,
	transaction transactions.Transaction,
	nodeErrors *nodeErrors,
	replicationCountMetric *prometheus.CounterVec,
) (primaryDirtied bool, updated []string, outdated []string) {
	nodeErrors.Lock()
	defer nodeErrors.Unlock()

	primaryErr := nodeErrors.errByNode[route.Primary.Storage]

	// If there were subtransactions, we only assume some changes were made if one of the subtransactions
	// was committed.
	//
	// If there were no subtransactions, we assume changes were performed only if the primary successfully
	// processed the RPC. This might be an RPC that is not correctly casting votes thus we replicate everywhere.
	//
	// If there were no subtransactions and the primary failed the RPC, we assume no changes have been made and
	// the nodes simply failed before voting.
	primaryDirtied = transaction.DidCommitAnySubtransaction() ||
		(transaction.CountSubtransactions() == 0 && primaryErr == nil)

	recordReplication := func(reason string, replicationCount int) {
		// If the primary wasn't dirtied, then we never replicate any changes. While this is
		// duplicates logic defined elsewhere, it's probably good enough given that we only
		// talk about metrics here.
		if primaryDirtied && replicationCount > 0 {
			replicationCountMetric.WithLabelValues(reason).Add(float64(replicationCount))
		}
	}

	// Replication targets were not added to the transaction, most likely because they are
	// either not healthy or out of date. We thus need to make sure to create replication jobs
	// for them.
	outdated = append(outdated, route.ReplicationTargets...)
	recordReplication("outdated", len(route.ReplicationTargets))

	// If the primary errored, then we need to assume that it has modified on-disk state and
	// thus need to replicate those changes to secondaries.
	if primaryErr != nil {
		ctxlogrus.Extract(ctx).WithError(primaryErr).Info("primary failed transaction")
		outdated = append(outdated, routerNodesToStorages(route.Secondaries)...)
		recordReplication("primary-failed", len(route.Secondaries))
		return
	}

	// If no subtransaction happened, then the called RPC may not be aware of transactions or
	// the nodes failed before casting any votes. If the primary failed the RPC, we assume
	// no changes were done and the nodes hit an error prior to voting. If the primary processed
	// the RPC successfully, we assume the RPC is not correctly voting and replicate everywhere.
	if transaction.CountSubtransactions() == 0 {
		ctxlogrus.Extract(ctx).Info("transaction did not create subtransactions")
		outdated = append(outdated, routerNodesToStorages(route.Secondaries)...)
		recordReplication("no-votes", len(route.Secondaries))
		return
	}

	// If we cannot get the transaction state, then something's gone awfully wrong. We go the
	// safe route and just replicate to all secondaries.
	nodeStates, err := transaction.State()
	if err != nil {
		ctxlogrus.Extract(ctx).WithError(err).Error("could not get transaction state")
		outdated = append(outdated, routerNodesToStorages(route.Secondaries)...)
		recordReplication("missing-tx-state", len(route.Secondaries))
		return
	}

	// If the primary node did not commit the transaction but there were some subtransactions committed,
	// then we must assume that it dirtied on-disk state. This modified state may not be what we want,
	// but it's what we got. So in order to ensure a consistent state, we need to replicate.
	if state := nodeStates[route.Primary.Storage]; state != transactions.VoteCommitted {
		if state == transactions.VoteFailed {
			ctxlogrus.Extract(ctx).Error("transaction: primary failed vote")
		}
		outdated = append(outdated, routerNodesToStorages(route.Secondaries)...)
		recordReplication("primary-not-committed", len(route.Secondaries))
		return
	}

	// Now we finally got the potentially happy case: in case the secondary didn't run into an
	// error and committed, it's considered up to date and thus does not need replication.
	for _, secondary := range route.Secondaries {
		if nodeErrors.errByNode[secondary.Storage] != nil {
			outdated = append(outdated, secondary.Storage)
			recordReplication("node-failed", 1)
			continue
		}

		if nodeStates[secondary.Storage] != transactions.VoteCommitted {
			outdated = append(outdated, secondary.Storage)
			recordReplication("node-not-committed", 1)
			continue
		}

		updated = append(updated, secondary.Storage)
	}

	return
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
	cause string,
) func() error {
	return func() error {
		// Use a separate timeout for the database operations. If the request times out, the passed in context is
		// canceled. We need to perform the database updates regardless whether the request was canceled or not as
		// the primary replica could have been dirtied and secondaries become outdated. Otherwise we'd have no idea of
		// the possible changes performed on the disk.
		ctx, cancel := context.WithTimeout(helper.SuppressCancellation(ctx), 30*time.Second)
		defer cancel()

		log := ctxlogrus.Extract(ctx).WithFields(logrus.Fields{
			"replication.cause":   cause,
			"replication.change":  change,
			"replication.primary": primary,
		})
		if len(updatedSecondaries) > 0 {
			log = log.WithField("replication.updated", updatedSecondaries)
		}
		if len(outdatedSecondaries) > 0 {
			log = log.WithField("replication.outdated", outdatedSecondaries)
		}
		log.Info("queueing replication jobs")

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
				updatedSecondaries,
				outdatedSecondaries,
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
