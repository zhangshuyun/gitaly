package praefect

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	gitalyerrors "gitlab.com/gitlab-org/gitaly/v14/internal/errors"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/metadata"
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
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// ErrRepositoryReadOnly is returned when the repository is in read-only mode. This happens
// if the primary does not have the latest changes.
var ErrRepositoryReadOnly = helper.ErrFailedPreconditionf("repository is in read-only mode")

type transactionsCondition func(context.Context) bool

func transactionsEnabled(context.Context) bool  { return true }
func transactionsDisabled(context.Context) bool { return false }

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
	"/gitaly.RepositoryService/ApplyGitattributes":           transactionsEnabled,
	"/gitaly.RepositoryService/CreateFork":                   transactionsEnabled,
	"/gitaly.RepositoryService/CreateRepository":             transactionsEnabled,
	"/gitaly.RepositoryService/CreateRepositoryFromBundle":   transactionsEnabled,
	"/gitaly.RepositoryService/CreateRepositoryFromSnapshot": transactionsEnabled,
	"/gitaly.RepositoryService/CreateRepositoryFromURL":      transactionsEnabled,
	"/gitaly.RepositoryService/FetchBundle":                  transactionsEnabled,
	"/gitaly.RepositoryService/FetchRemote":                  transactionsEnabled,
	"/gitaly.RepositoryService/FetchSourceBranch":            transactionsEnabled,
	"/gitaly.RepositoryService/RemoveRepository":             transactionsEnabled,
	"/gitaly.RepositoryService/ReplicateRepository":          transactionsEnabled,
	"/gitaly.RepositoryService/SetFullPath":                  transactionsEnabled,
	"/gitaly.RepositoryService/WriteRef":                     transactionsEnabled,
	"/gitaly.SSHService/SSHReceivePack":                      transactionsEnabled,
	"/gitaly.SmartHTTPService/PostReceivePack":               transactionsEnabled,
	"/gitaly.WikiService/WikiUpdatePage":                     transactionsEnabled,
	"/gitaly.WikiService/WikiWritePage":                      transactionsEnabled,

	// The following RPCs currently aren't transactional, but we may consider making them
	// transactional in the future if the need arises.
	"/gitaly.ObjectPoolService/CreateObjectPool":           transactionsDisabled,
	"/gitaly.ObjectPoolService/DeleteObjectPool":           transactionsDisabled,
	"/gitaly.ObjectPoolService/DisconnectGitAlternates":    transactionsDisabled,
	"/gitaly.ObjectPoolService/LinkRepositoryToObjectPool": transactionsDisabled,
	"/gitaly.ObjectPoolService/ReduplicateRepository":      transactionsDisabled,
	"/gitaly.RepositoryService/RenameRepository":           transactionsDisabled,

	// The following list of RPCs are considered idempotent RPCs: while they write into the
	// target repository, this shouldn't ever have any user-visible impact given that they're
	// purely optimizations of the on-disk state. These RPCs are thus treated specially and
	// shouldn't ever cause a repository generation bump.
	"/gitaly.RefService/PackRefs":                       transactionsDisabled,
	"/gitaly.RepositoryService/Cleanup":                 transactionsDisabled,
	"/gitaly.RepositoryService/GarbageCollect":          transactionsDisabled,
	"/gitaly.RepositoryService/MidxRepack":              transactionsDisabled,
	"/gitaly.RepositoryService/OptimizeRepository":      transactionsDisabled,
	"/gitaly.RepositoryService/PruneUnreachableObjects": transactionsDisabled,
	"/gitaly.RepositoryService/RepackFull":              transactionsDisabled,
	"/gitaly.RepositoryService/RepackIncremental":       transactionsDisabled,
	"/gitaly.RepositoryService/RestoreCustomHooks":      transactionsDisabled,
	"/gitaly.RepositoryService/WriteCommitGraph":        transactionsDisabled,
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
	case "/gitaly.ObjectPoolService/CreateObjectPool",
		"/gitaly.RepositoryService/CreateFork",
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
		return datastore.GarbageCollect, datastore.Params{
			"CreateBitmap": req.GetCreateBitmap(),
			"Prune":        req.GetPrune(),
		}, nil
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
	case "/gitaly.RepositoryService/WriteCommitGraph":
		req, ok := m.(*gitalypb.WriteCommitGraphRequest)
		if !ok {
			return "", nil, fmt.Errorf("protocol changed: for method %q expected message type '%T', got '%T'", methodName, req, m)
		}
		return datastore.WriteCommitGraph, datastore.Params{
			"SplitStrategy": req.GetSplitStrategy(),
		}, nil
	case "/gitaly.RepositoryService/MidxRepack":
		req, ok := m.(*gitalypb.MidxRepackRequest)
		if !ok {
			return "", nil, fmt.Errorf("protocol changed: for method %q expected message type '%T', got '%T'", methodName, req, m)
		}
		return datastore.MidxRepack, nil, nil
	case "/gitaly.RepositoryService/OptimizeRepository":
		req, ok := m.(*gitalypb.OptimizeRepositoryRequest)
		if !ok {
			return "", nil, fmt.Errorf("protocol changed: for method %q expected message type '%T', got '%T'", methodName, req, m)
		}
		return datastore.OptimizeRepository, nil, nil
	case "/gitaly.RepositoryService/PruneUnreachableObjects":
		req, ok := m.(*gitalypb.PruneUnreachableObjectsRequest)
		if !ok {
			return "", nil, fmt.Errorf("protocol changed: for method %q expected message type '%T', got '%T'", methodName, req, m)
		}
		return datastore.PruneUnreachableObjects, nil, nil
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

//nolint: revive,stylecheck // This is unintentionally missing documentation.
func (c *Coordinator) Describe(descs chan<- *prometheus.Desc) {
	prometheus.DescribeByCollect(c, descs)
}

//nolint: revive,stylecheck // This is unintentionally missing documentation.
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
	case protoregistry.OpMaintenance:
		ps, err = c.maintenanceStreamParameters(ctx, call)
	default:
		err = fmt.Errorf("unknown operation type: %v", call.methodInfo.Operation)
	}

	if err != nil {
		if errors.As(err, new(commonerr.RepositoryNotFoundError)) {
			return nil, helper.ErrNotFound(err)
		}

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

	route, err := c.router.RouteRepositoryAccessor(
		ctx, virtualStorage, repoPath, shouldRouteRepositoryAccessorToPrimary(ctx, call),
	)
	if err != nil {
		return nil, fmt.Errorf("accessor call: route repository accessor: %w", err)
	}

	b, err := rewrittenRepositoryMessage(call.methodInfo, call.msg, route.Node.Storage, route.ReplicaPath, "")
	if err != nil {
		return nil, fmt.Errorf("accessor call: rewrite storage: %w", err)
	}

	metrics.ReadDistribution.WithLabelValues(virtualStorage, route.Node.Storage).Inc()

	return proxy.NewStreamParameters(proxy.Destination{
		Ctx:  streamParametersContext(ctx),
		Conn: route.Node.Connection,
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

	var additionalRepoRelativePath string
	if additionalRepo, ok, err := call.methodInfo.AdditionalRepo(call.msg); err != nil {
		return nil, helper.ErrInvalidArgument(err)
	} else if ok {
		additionalRepoRelativePath = additionalRepo.GetRelativePath()
	}

	var route RepositoryMutatorRoute
	switch change {
	case datastore.CreateRepo:
		route, err = c.router.RouteRepositoryCreation(ctx, virtualStorage, targetRepo.RelativePath, additionalRepoRelativePath)
		if err != nil {
			return nil, fmt.Errorf("route repository creation: %w", err)
		}
	default:
		route, err = c.router.RouteRepositoryMutator(ctx, virtualStorage, targetRepo.RelativePath, additionalRepoRelativePath)
		if err != nil {
			if errors.Is(err, ErrRepositoryReadOnly) {
				return nil, err
			}

			return nil, fmt.Errorf("mutator call: route repository mutator: %w", err)
		}
	}

	primaryMessage, err := rewrittenRepositoryMessage(call.methodInfo, call.msg, route.Primary.Storage, route.ReplicaPath, route.AdditionalReplicaPath)
	if err != nil {
		return nil, fmt.Errorf("mutator call: rewrite storage: %w", err)
	}

	var finalizers []func() error

	primaryDest := proxy.Destination{
		Ctx:  streamParametersContext(ctx),
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
		primaryDest.Ctx = streamParametersContext(injectedCtx)
		primaryDest.ErrHandler = func(err error) error {
			nodeErrors.Lock()
			defer nodeErrors.Unlock()
			nodeErrors.errByNode[route.Primary.Storage] = err
			return err
		}

		for _, secondary := range route.Secondaries {
			secondary := secondary
			secondaryMsg, err := rewrittenRepositoryMessage(call.methodInfo, call.msg, secondary.Storage, route.ReplicaPath, route.AdditionalReplicaPath)
			if err != nil {
				return nil, err
			}

			injectedCtx, err := txinfo.InjectTransaction(ctx, transaction.ID(), secondary.Storage, false)
			if err != nil {
				return nil, err
			}

			secondaryDests = append(secondaryDests, proxy.Destination{
				Ctx:  streamParametersContext(injectedCtx),
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
				route.RepositoryID,
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

// maintenanceStreamParameters returns stream parameters for a maintenance-style RPC. The RPC call
// is proxied to all nodes. Because it shouldn't matter whether a node is the primary or not in this
// context, we just pick the first node returned by the router to be the primary. Returns an error
// in case any of the nodes has failed to perform the maintenance RPC.
func (c *Coordinator) maintenanceStreamParameters(ctx context.Context, call grpcCall) (*proxy.StreamParameters, error) {
	route, err := c.router.RouteRepositoryMaintenance(ctx, call.targetRepo.StorageName, call.targetRepo.RelativePath)
	if err != nil {
		if errors.Is(err, ErrRepositoryReadOnly) {
			return nil, err
		}

		return nil, fmt.Errorf("routing repository maintenance: %w", err)
	}

	peerCtx := streamParametersContext(ctx)

	nodeDests := make([]proxy.Destination, 0, len(route.Nodes))
	nodeErrors := &nodeErrors{
		errByNode: make(map[string]error),
	}

	for _, node := range route.Nodes {
		node := node

		nodeMsg, err := rewrittenRepositoryMessage(call.methodInfo, call.msg, node.Storage, route.ReplicaPath, "")
		if err != nil {
			return nil, err
		}

		nodeDests = append(nodeDests, proxy.Destination{
			Ctx:  peerCtx,
			Conn: node.Connection,
			Msg:  nodeMsg,
			ErrHandler: func(err error) error {
				nodeErrors.Lock()
				defer nodeErrors.Unlock()
				nodeErrors.errByNode[node.Storage] = err

				ctxlogrus.Extract(ctx).WithError(err).Error("proxying maintenance RPC to node failed")

				// We ignore any errors returned by nodes such that they all have a
				// chance to finish their maintenance RPC in a best-effort strategy.
				// In case any node fails though, the RPC call will return with an
				// error after all nodes have finished.
				return nil
			},
		})
	}

	return proxy.NewStreamParameters(nodeDests[0], nodeDests[1:], func() error {
		nodeErrors.Lock()
		defer nodeErrors.Unlock()

		// In case any of the nodes has recorded an error we will return it. It shouldn't
		// matter which error we return exactly, so we just return errors in the order we've
		// got from the router. This also has the nice property that any error returned by
		// the primary node would be prioritized over all the others.
		for _, node := range route.Nodes {
			if nodeErr, ok := nodeErrors.errByNode[node.Storage]; ok && nodeErr != nil {
				return nodeErr
			}
		}

		return nil
	}, nil), nil
}

// streamParametersContexts converts the contexts with incoming metadata into a context that is
// usable by peer Gitaly nodes.
func streamParametersContext(ctx context.Context) context.Context {
	outgoingCtx := metadata.IncomingToOutgoing(ctx)

	// When upgrading Gitaly nodes where the upgrade contains feature flag default changes, then
	// there will be a window where a subset of Gitaly nodes has a different understanding of
	// the current default value. If the feature flag wasn't set explicitly on upgrade by
	// outside callers, then these Gitaly nodes will thus see different default values and run
	// different code. This is something we want to avoid: all Gitalies should have the same
	// world view of which features are enabled and which ones aren't and ideally do the same
	// thing.
	//
	// This problem isn't solveable on Gitaly side, but Praefect is in a perfect position to do
	// so. While it may have the same problem in a load-balanced multi-Praefect setup, this is
	// much less of a problem: the most important thing is that the view on feature flags is
	// consistent for a single RPC call, and that will always be the case regardless of which
	// Praefect is proxying the call. The worst case scenario is that we'll execute the RPC once
	// with a feature flag enabled, and once with it disabled, which shouldn't typically be an
	// issue.
	//
	// To fix this scenario, we thus inject all known feature flags with their current default
	// value as seen by Praefect into Gitaly's context if they haven't explicitly been set by
	// the caller. Like this, Gitalies will never even consider the default value in a cluster
	// setup, except when it is being introduced for the first time when Gitaly restarts before
	// Praefect. But given that feature flags should be introduced with a default value of
	// `false` to account for zero-dodwntime upgrades, the view would also be consistent in that
	// case.
	rawFeatureFlags := featureflag.RawFromContext(ctx)
	if rawFeatureFlags == nil {
		rawFeatureFlags = map[string]string{}
	}

	for _, ff := range featureflag.All {
		if _, ok := rawFeatureFlags[ff.MetadataKey()]; !ok {
			rawFeatureFlags[ff.MetadataKey()] = strconv.FormatBool(ff.OnByDefault)
		}
	}
	outgoingCtx = featureflag.OutgoingWithRaw(outgoingCtx, rawFeatureFlags)

	return outgoingCtx
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

			if errors.Is(err, commonerr.ErrRepositoryAlreadyExists) {
				return nil, helper.ErrAlreadyExists(err)
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

// rewrittenRepositoryMessage rewrites the repository storages and relative paths.
func rewrittenRepositoryMessage(mi protoregistry.MethodInfo, m proto.Message, storage, relativePath, additionalRelativePath string) ([]byte, error) {
	// clone the message so the original is not changed
	m = proto.Clone(m)
	targetRepo, err := mi.TargetRepo(m)
	if err != nil {
		return nil, helper.ErrInvalidArgument(err)
	}

	// rewrite the target repository
	targetRepo.StorageName = storage
	targetRepo.RelativePath = relativePath

	additionalRepo, ok, err := mi.AdditionalRepo(m)
	if err != nil {
		return nil, helper.ErrInvalidArgument(err)
	}

	if ok {
		additionalRepo.StorageName = storage
		additionalRepo.RelativePath = additionalRelativePath
	}

	return proxy.NewCodec().Marshal(m)
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
			ctx, route.RepositoryID, virtualStorage, targetRepo, route.Primary.Storage,
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
// - The node has a different error state than the primary. If both primary and secondary have
//   returned the same error, then we assume they did the same thing and failed in the same
//   controlled way.
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

	// If there were no subtransactions and the primary failed the RPC, we assume no changes
	// have been made and the nodes simply failed before voting. We can thus return directly and
	// notify the caller that the primary is not considered to be dirty.
	if transaction.CountSubtransactions() == 0 && primaryErr != nil {
		return false, nil, nil
	}

	// If there was a single subtransactions but the primary didn't cast a vote, then it means
	// that the primary node has dropped out before secondaries were able to commit any changes
	// to disk. Given that they cannot ever succeed without the primary, no change to disk
	// should have happened.
	if transaction.CountSubtransactions() == 1 && !transaction.DidVote(route.Primary.Storage) {
		return false, nil, nil
	}

	primaryDirtied = true

	nodesByState := make(map[string][]string)
	defer func() {
		ctxlogrus.Extract(ctx).
			WithField("transaction.primary", route.Primary.Storage).
			WithField("transaction.secondaries", nodesByState).
			Info("transactional node states")

		for reason, nodes := range nodesByState {
			replicationCountMetric.WithLabelValues(reason).Add(float64(len(nodes)))
		}
	}()

	markOutdated := func(reason string, nodes []string) {
		if len(nodes) != 0 {
			outdated = append(outdated, nodes...)
			nodesByState[reason] = append(nodesByState[reason], nodes...)
		}
	}

	// Replication targets were not added to the transaction, most likely because they are
	// either not healthy or out of date. We thus need to make sure to create replication jobs
	// for them.
	markOutdated("outdated", route.ReplicationTargets)

	// If no subtransaction happened, then the called RPC may not be aware of transactions or
	// the nodes failed before casting any votes. If the primary failed the RPC, we assume
	// no changes were done and the nodes hit an error prior to voting. If the primary processed
	// the RPC successfully, we assume the RPC is not correctly voting and replicate everywhere.
	if transaction.CountSubtransactions() == 0 {
		markOutdated("no-votes", routerNodesToStorages(route.Secondaries))
		return
	}

	// If we cannot get the transaction state, then something's gone awfully wrong. We go the
	// safe route and just replicate to all secondaries.
	nodeStates, err := transaction.State()
	if err != nil {
		markOutdated("missing-tx-state", routerNodesToStorages(route.Secondaries))
		return
	}

	// If the primary node did not commit the transaction but there were some subtransactions committed,
	// then we must assume that it dirtied on-disk state. This modified state may not be what we want,
	// but it's what we got. So in order to ensure a consistent state, we need to replicate.
	if state := nodeStates[route.Primary.Storage]; state != transactions.VoteCommitted {
		markOutdated("primary-not-committed", routerNodesToStorages(route.Secondaries))
		return
	}

	// Now we finally got the potentially happy case: when the secondary committed the
	// transaction and has the same error state as the primary, then it's considered up to date
	// and thus does not need replication.
	for _, secondary := range route.Secondaries {
		if !equalGrpcError(nodeErrors.errByNode[secondary.Storage], primaryErr) {
			markOutdated("node-error-status", []string{secondary.Storage})
			continue
		}

		if nodeStates[secondary.Storage] != transactions.VoteCommitted {
			markOutdated("node-not-committed", []string{secondary.Storage})
			continue
		}

		updated = append(updated, secondary.Storage)
		nodesByState["updated"] = append(nodesByState["updated"], secondary.Storage)
	}

	return
}

func equalGrpcError(err1, err2 error) bool {
	status1, ok := status.FromError(err1)
	if !ok {
		return false
	}

	status2, ok := status.FromError(err2)
	if !ok {
		return false
	}

	if status1.Code() != status2.Code() {
		return false
	}

	if status1.Message() != status2.Message() {
		return false
	}

	return true
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
	repositoryID int64,
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
			if err := c.rs.IncrementGeneration(ctx, repositoryID, primary, updatedSecondaries); err != nil {
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
		case datastore.CreateRepo:
			repositorySpecificPrimariesEnabled := c.conf.Failover.ElectionStrategy == config.ElectionStrategyPerRepository
			variableReplicationFactorEnabled := repositorySpecificPrimariesEnabled &&
				c.conf.DefaultReplicationFactors()[virtualStorage] > 0

			if err := c.rs.CreateRepository(ctx,
				repositoryID,
				virtualStorage,
				targetRepo.GetRelativePath(),
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
					RepositoryID:      repositoryID,
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
		return gitalyerrors.ErrInvalidRepository
	}

	if _, found := c.conf.StorageNames()[repo.StorageName]; !found {
		// this needs to be nodes.ErrVirtualStorageNotExist error, but it will break
		// existing API contract as praefect should be a transparent proxy of the gitaly
		return gitalyerrors.ErrInvalidRepository
	}

	return nil
}
