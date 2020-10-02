package praefect

import (
	"context"
	"errors"
	"fmt"

	"github.com/golang/protobuf/proto"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/v13/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v13/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/v13/internal/middleware/metadatahandler"
	"gitlab.com/gitlab-org/gitaly/v13/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/v13/internal/praefect/datastore"
	"gitlab.com/gitlab-org/gitaly/v13/internal/praefect/grpc-proxy/proxy"
	"gitlab.com/gitlab-org/gitaly/v13/internal/praefect/metadata"
	"gitlab.com/gitlab-org/gitaly/v13/internal/praefect/metrics"
	"gitlab.com/gitlab-org/gitaly/v13/internal/praefect/nodes"
	"gitlab.com/gitlab-org/gitaly/v13/internal/praefect/protoregistry"
	"gitlab.com/gitlab-org/gitaly/v13/internal/praefect/transactions"
	"gitlab.com/gitlab-org/gitaly/v13/proto/go/gitalypb"
	"gitlab.com/gitlab-org/labkit/correlation"
	"golang.org/x/sync/errgroup"
)

// ErrRepositoryReadOnly is returned when the repository is in read-only mode. This happens
// if the primary does not have the latest changes.
var ErrRepositoryReadOnly = helper.ErrPreconditionFailedf("repository is in read-only mode")

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
	targetRepo, err := call.methodInfo.TargetRepo(call.msg)
	if err != nil {
		return nil, helper.ErrInvalidArgument(fmt.Errorf("repo scoped: %w", err))
	}

	ctxlogrus.AddFields(ctx, logrus.Fields{
		"virtual_storage": call.targetRepo.StorageName,
		"relative_path":   call.targetRepo.RelativePath,
	})

	if targetRepo.StorageName == "" || targetRepo.RelativePath == "" {
		return nil, helper.ErrInvalidArgumentf("repo scoped: target repo is invalid")
	}

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

var transactionRPCs = map[string]interface{}{
	"/gitaly.OperationService/UserCreateBranch": nil,
	"/gitaly.OperationService/UserCreateTag":    nil,
	"/gitaly.OperationService/UserDeleteBranch": nil,
	"/gitaly.OperationService/UserDeleteTag":    nil,
	"/gitaly.OperationService/UserUpdateBranch": nil,
	"/gitaly.SSHService/SSHReceivePack":         nil,
	"/gitaly.SmartHTTPService/PostReceivePack":  nil,
}

func shouldUseTransaction(ctx context.Context, method string) bool {
	if !featureflag.IsEnabled(ctx, featureflag.ReferenceTransactions) {
		return false
	}
	_, ok := transactionRPCs[method]
	return ok
}

func (c *Coordinator) registerTransaction(ctx context.Context, primary Node, secondaries []Node) (*transactions.Transaction, transactions.CancelFunc, error) {
	var voters []transactions.Voter
	var threshold uint

	if featureflag.IsEnabled(ctx, featureflag.ReferenceTransactionsPrimaryWins) {
		// This voting strategy ensures that transactions always go ahead as long as
		// the primary doesn't fail because of unrelated reasons. Secondaries' votes do
		// not matter.

		voters = append(voters, transactions.Voter{
			Name:  primary.Storage,
			Votes: 1,
		})
		threshold = 1

		for _, node := range secondaries {
			voters = append(voters, transactions.Voter{
				Name:  node.Storage,
				Votes: 0,
			})
		}
	} else {
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
	}

	return c.txMgr.RegisterTransaction(ctx, voters, threshold)
}

func (c *Coordinator) mutatorStreamParameters(ctx context.Context, call grpcCall, targetRepo *gitalypb.Repository) (*proxy.StreamParameters, error) {
	virtualStorage := targetRepo.StorageName

	route, err := c.router.RouteRepositoryMutator(ctx, virtualStorage, call.targetRepo.RelativePath)
	if err != nil {
		if errors.Is(err, ErrRepositoryReadOnly) {
			return nil, err
		}

		return nil, fmt.Errorf("mutator call: route repository mutator: %w", err)
	}

	primaryMessage, err := rewrittenRepositoryMessage(call.methodInfo, call.msg, route.Primary.Storage)
	if err != nil {
		return nil, fmt.Errorf("mutator call: rewrite storage: %w", err)
	}

	change, params, err := getReplicationDetails(call.fullMethodName, call.msg)
	if err != nil {
		return nil, fmt.Errorf("mutator call: replication details: %w", err)
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
			transactionCleanup, c.createTransactionFinalizer(ctx, transaction, route, virtualStorage, call.targetRepo, change, params),
		)
	} else {
		finalizers = append(finalizers,
			c.newRequestFinalizer(
				ctx,
				virtualStorage,
				call.targetRepo,
				route.Primary.Storage,
				nil,
				append(nodesToStorages(route.Secondaries), route.ReplicationTargets...),
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

// streamDirector determines which downstream servers receive requests
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
		successByNode := transaction.State()

		// If no subtransaction happened, then the called RPC may not be aware of
		// transactions at all. We thus need to assume it changed repository state
		// and need to create replication jobs.
		if transaction.CountSubtransactions() == 0 {
			secondaries := make([]string, 0, len(successByNode))
			for secondary := range successByNode {
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
		if !successByNode[route.Primary.Storage] {
			return fmt.Errorf("transaction: primary failed vote")
		}
		delete(successByNode, route.Primary.Storage)

		updatedSecondaries := make([]string, 0, len(successByNode))
		var outdatedSecondaries []string

		for node, success := range successByNode {
			if success {
				updatedSecondaries = append(updatedSecondaries, node)
				continue
			}

			outdatedSecondaries = append(outdatedSecondaries, node)
		}

		return c.newRequestFinalizer(
			ctx, virtualStorage, targetRepo, route.Primary.Storage,
			updatedSecondaries, outdatedSecondaries, change, params)()
	}
}

func nodesToStorages(nodes []Node) []string {
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
		}

		correlationID := c.ensureCorrelationID(ctx, targetRepo)

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
