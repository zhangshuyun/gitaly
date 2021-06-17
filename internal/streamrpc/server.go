package streamrpc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/golang/protobuf/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

var _ grpc.ServiceRegistrar = &Server{}

// Server handles network connections and routes them to StreamRPC handlers.
type Server struct {
	methods     map[string]*method
	interceptor grpc.UnaryServerInterceptor
}

type method struct {
	*grpc.MethodDesc
	implementation interface{}
}

// ServerOption is an abstraction that lets you pass 0 or more server
// options to NewServer.
type ServerOption func(*Server)

// WithServerInterceptor adds a unary gRPC server interceptor.
func WithServerInterceptor(interceptor grpc.UnaryServerInterceptor) ServerOption {
	return func(s *Server) { s.interceptor = interceptor }
}

// NewServer returns a new StreamRPC server. You can pass the result to
// grpc-go RegisterFooServer functions.
func NewServer(opts ...ServerOption) *Server {
	s := &Server{
		methods: make(map[string]*method),
	}
	for _, o := range opts {
		o(s)
	}
	return s
}

// RegisterService implements grpc.ServiceRegistrar. It makes it possible
// to pass a *Server to grpc-go foopb.RegisterFooServer functions as the
// first argument.
func (s *Server) RegisterService(sd *grpc.ServiceDesc, impl interface{}) {
	for i := range sd.Methods {
		m := &sd.Methods[i]
		s.methods["/"+sd.ServiceName+"/"+m.MethodName] = &method{
			MethodDesc:     m,
			implementation: impl,
		}
	}
}

// Handle handles an incoming network connection with the StreamRPC
// protocol. It is intended to be called from a net.Listener.Accept loop
// (or something equivalent).
func (s *Server) Handle(c net.Conn) error {
	defer c.Close()

	deadline := time.Now().Add(defaultHandshakeTimeout)
	req, err := recvFrame(c, deadline)
	if err != nil {
		return err
	}

	session := &serverSession{
		c:        c,
		deadline: deadline,
	}
	if err := s.handleSession(session, req); err != nil {
		return session.reject(err)
	}

	return nil
}

func (s *Server) handleSession(session *serverSession, reqBytes []byte) error {
	req := &request{}
	if err := json.Unmarshal(reqBytes, req); err != nil {
		return err
	}

	method, ok := s.methods[req.Method]
	if !ok {
		return fmt.Errorf("method not found: %s", req.Method)
	}

	ctx, cancel := serverContext(session, req)
	defer cancel()

	if _, err := method.Handler(
		method.implementation,
		ctx,
		func(msg interface{}) error { return proto.Unmarshal(req.Message, msg.(proto.Message)) },
		s.interceptor,
	); err != nil {
		return err
	}

	return nil
}

func serverContext(session *serverSession, req *request) (context.Context, func()) {
	ctx := context.Background()
	ctx = context.WithValue(ctx, sessionKey{}, session)
	ctx = metadata.NewIncomingContext(ctx, req.Metadata)
	return context.WithCancel(ctx)
}

type sessionKey struct{}

// AcceptConnection completes the StreamRPC handshake on the server side.
// It notifies the client that the server has accepted the stream, and
// returns the connection.
func AcceptConnection(ctx context.Context) (net.Conn, error) {
	session, ok := ctx.Value(sessionKey{}).(*serverSession)
	if !ok {
		return nil, errors.New("context has no serverSession")
	}
	return session.Accept()
}

// serverSession wraps an incoming connection whose handshake has not
// been completed yet.
type serverSession struct {
	c        net.Conn
	accepted bool
	deadline time.Time
}

// Accept completes the handshake on the connection wrapped by ss and
// unwraps the connection.
func (ss *serverSession) Accept() (net.Conn, error) {
	if ss.accepted {
		return nil, errors.New("connection already accepted")
	}

	ss.accepted = true
	if err := sendFrame(ss.c, nil, ss.deadline); err != nil {
		return nil, fmt.Errorf("accept session: %w", err)
	}

	return ss.c, nil
}

func (ss *serverSession) reject(err error) error {
	if ss.accepted {
		return nil
	}

	buf, err := json.Marshal(&response{Error: err.Error()})
	if err != nil {
		return fmt.Errorf("mashal response: %w", err)
	}

	return sendFrame(ss.c, buf, ss.deadline)
}
