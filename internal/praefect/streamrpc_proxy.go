package praefect

import (
    "time"
    "net"
    "fmt"
    "context"
	"encoding/json"
	"io"
    "strings"
    "sync/atomic"
	"golang.org/x/sync/errgroup"
	"github.com/golang/protobuf/proto"
	"gitlab.com/gitlab-org/gitaly/v14/internal/streamrpc"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/protoregistry"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper"
	"google.golang.org/grpc/metadata"
    "google.golang.org/grpc"
)

// StreamRPCProxy should implement streamrpc.StreamRPCHandler interface
type StreamRPCProxy struct {
    interceptor grpc.UnaryServerInterceptor
    router Router
}

func NewStreamRPCProxy(router Router) *StreamRPCProxy {
    return &StreamRPCProxy{
        router: router,
    }
}

func (proxy *StreamRPCProxy) SetInterceptor(interceptor grpc.UnaryServerInterceptor) {
    proxy.interceptor = interceptor
}

func (proxy *StreamRPCProxy) Interceptor() grpc.UnaryServerInterceptor {
    return proxy.interceptor;
}

type proxySession struct {
	c        net.Conn
	deadline time.Time
}

func (proxy *StreamRPCProxy) Handle(c net.Conn) {
    defer c.Close()

	deadline := time.Now().Add(streamrpc.DefaultHandshakeTimeout)
	req, err := streamrpc.RecvFrame(c, deadline)
    if err != nil {
        return
    }

	session := &proxySession{
		c:        c,
		deadline: deadline,
	}

	if err := proxy.handleSession(session, req); err != nil {
		_ = session.reject(err)
	}

}
func (proxy *StreamRPCProxy) handleSession(session *proxySession, reqBytes []byte) error {
	req := &streamrpc.HandshakeRequest{}
	if err := json.Unmarshal(reqBytes, req); err != nil {
		return err
	}
    method, err := protoregistry.GitalyProtoPreregistered.LookupMethod(req.Method)
    if err != nil {
        return err
    }
    message, err := method.UnmarshalRequestProto(req.Message)
    if err != nil {
        return err
    }
    if method.Scope != protoregistry.ScopeRepository {
		return fmt.Errorf("StreamRPC Proxy only supports registry scope at the moment")
    }
    if method.Operation != protoregistry.OpAccessor {
		return fmt.Errorf("StreamRPC Proxy only supports accessor operation at the moment")
    }

    targetRepo, err := method.TargetRepo(message)
    if err != nil {
        return helper.ErrInvalidArgument(fmt.Errorf("repo scoped: %w", err))
    }

	ctx, cancel := proxyContext(session, req)
	defer cancel()

    // Trigger all Praefect server interceptor
    _, err = proxy.interceptor(
        ctx, message, &grpc.UnaryServerInfo{ FullMethod: method.FullMethodName() },
        func(ctx context.Context, req interface{}) (interface{}, error) {
            // The coordinator also validates target repo. I'm skipping it now
            // Also, other things like loggings?
            node, err := proxy.router.RouteRepositoryAccessor(
                ctx,
                targetRepo.StorageName,
                targetRepo.GetRelativePath(),
                false, // ForcePrimary. Okay, depending on the call, i'm skipping it now^
            )
            if err != nil {
                return nil, err
            }
            return nil, session.forward(ctx, node, method, message)
        },
    )

    return err
}

func (session *proxySession) forward(ctx context.Context, node RouterNode, method protoregistry.MethodInfo, message proto.Message) error {
    parts := strings.Split(node.Connection.Target(), ":")
    if len(parts) != 2 {
        return fmt.Errorf("Invalid endpoing: %s", node.Connection.Target())
    }
    schema := parts[0]
    addr := parts[1]
    return streamrpc.Call(
        ctx, // Okay, the context is messed up here
        streamrpc.DialNet(schema, addr),
        method.FullMethodName(),
        message,
        func(dst net.Conn) error {
            defer dst.Close()

            // After the secondary call to the upstream passes the handshaking
            // phase, we signal the client the call suceeds. Then we copy all
            // data from clients to write to the connection of upstream
            if err := streamrpc.SendFrame(session.c, nil, session.deadline); err != nil {
                return fmt.Errorf("accept session: %w", err)
            }

            // Okay, a really really naive proxy implementation. Just forward data, no error handled
            go io.Copy(dst, session.c)
            if _, err := io.Copy(session.c, dst); err != nil {
                return err
            }
            return session.c.Close()
        },
    )
}

func proxyContext(session *proxySession, req *streamrpc.HandshakeRequest) (context.Context, func()) {
	ctx := context.Background()
	ctx = metadata.NewIncomingContext(ctx, req.Metadata)
	return context.WithCancel(ctx)
}

func (s *proxySession) reject(err error) error {
	buf, err := json.Marshal(&streamrpc.HandshakeResponse{Error: err.Error()})
	if err != nil {
		return fmt.Errorf("mashal response: %w", err)
	}

	return streamrpc.SendFrame(s.c, buf, s.deadline)
}
