package praefect

import (
	"context"
	"fmt"
	"sync"

	gitalyauth "gitlab.com/gitlab-org/gitaly/auth"
	gitalyconfig "gitlab.com/gitlab-org/gitaly/internal/config"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/protoregistry"

	"github.com/golang/protobuf/protoc-gen-go/descriptor"
	"github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/client"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/grpc-proxy/proxy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Coordinator takes care of directing client requests to the appropriate
// downstream server. The coordinator is thread safe; concurrent calls to
// register nodes are safe.
type Coordinator struct {
	sync.Mutex
	log      *logrus.Logger
	connLock sync.RWMutex

	datastore PrimaryDatastore

	nodes    map[string]*grpc.ClientConn
	registry *protoregistry.Registry
}

// NewCoordinator returns a new Coordinator that utilizes the provided logger
func NewCoordinator(l *logrus.Logger, datastore PrimaryDatastore, fileDescriptors ...*descriptor.FileDescriptorProto) *Coordinator {
	registry := protoregistry.New()
	registry.RegisterFiles(fileDescriptors...)

	return &Coordinator{
		log:       l,
		datastore: datastore,
		nodes:     make(map[string]*grpc.ClientConn),
		registry:  registry,
	}
}

// RegisterProtos allows coordinator to register new protos on the fly
func (c *Coordinator) RegisterProtos(protos ...*descriptor.FileDescriptorProto) error {
	return c.registry.RegisterFiles(protos...)
}

// GetStorageNode returns the registered node for the given storage location
func (c *Coordinator) GetStorageNode(storage string) (Node, error) {
	cc, ok := c.getConn(storage)
	if !ok {
		return Node{}, fmt.Errorf("no node registered for storage location %q", storage)
	}

	return Node{
		Storage: storage,
		cc:      cc,
	}, nil
}

// streamDirector determines which downstream servers receive requests
func (c *Coordinator) streamDirector(ctx context.Context, fullMethodName string, _ proxy.StreamPeeker) (context.Context, *grpc.ClientConn, error) {
	// For phase 1, we need to route messages based on the storage location
	// to the appropriate Gitaly node.
	c.Lock()
	defer c.Unlock()

	c.log.Debugf("Stream director received method %s", fullMethodName)

	storageName, err := c.datastore.GetPrimary()
	if err != nil {
		err := status.Error(
			codes.FailedPrecondition,
			"no downstream node registered",
		)
		return nil, nil, err
	}

	// We only need the primary node, as there's only one primary storage
	// location per praefect at this time
	cc, ok := c.getConn(storageName)
	if !ok {
		return nil, nil, fmt.Errorf("unable to find existing client connection for %s", storageName)
	}

	return ctx, cc, nil
}

// RegisterNode will direct traffic to the supplied downstream connection when the storage location
// is encountered.
func (c *Coordinator) RegisterNode(storageName, listenAddr string) error {
	conn, err := client.Dial(listenAddr,
		[]grpc.DialOption{
			grpc.WithDefaultCallOptions(grpc.CallCustomCodec(proxy.Codec())),
			grpc.WithPerRPCCredentials(gitalyauth.RPCCredentials(gitalyconfig.Config.Auth.Token)),
		},
	)
	if err != nil {
		return err
	}

	c.setConn(storageName, conn)

	return nil
}

// UnregisterNode removes a node and its connection
func (c *Coordinator) UnregisterNode(storageName string) error {
	c.delConn(storageName)

	return nil
}

func (c *Coordinator) setConn(storageName string, conn *grpc.ClientConn) {
	c.connLock.Lock()
	c.nodes[storageName] = conn
	c.connLock.Unlock()
}

func (c *Coordinator) getConn(storageName string) (*grpc.ClientConn, bool) {
	c.connLock.RLock()
	cc, ok := c.nodes[storageName]
	c.connLock.RUnlock()

	return cc, ok
}

func (c *Coordinator) delConn(storageName string) {
	c.connLock.Lock()
	delete(c.nodes, storageName)
	c.connLock.Unlock()
}
