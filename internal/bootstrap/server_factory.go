package bootstrap

import (
	"net"
	"sync"

	"gitlab.com/gitlab-org/gitaly/internal/config"
	"gitlab.com/gitlab-org/gitaly/internal/rubyserver"
	"gitlab.com/gitlab-org/gitaly/internal/server"
	"google.golang.org/grpc"
)

// GitalyServerFactory is a factory of gitaly grpc servers
type GitalyServerFactory struct {
	ruby             *rubyserver.Server
	secure, insecure *grpc.Server
}

// GracefulStoppableServer allows to serve contents on a net.Listener, Stop serving and performing a GracefulStop
type GracefulStoppableServer interface {
	GracefulStop()
	Stop()
	Serve(l net.Listener, secure bool) error
}

// NewGitalyServerFactory initializes a rubyserver and then lazily initializes both secure and insecure grpc.Server
func NewGitalyServerFactory() *GitalyServerFactory {
	return &GitalyServerFactory{ruby: &rubyserver.Server{}}
}

// StartRuby starts the ruby process
func (s *GitalyServerFactory) StartRuby() error {
	return s.ruby.Start()
}

// Stop stops both the secure and insecure servers
func (s *GitalyServerFactory) Stop() {
	for _, srv := range s.all() {
		srv.Stop()
	}

	s.ruby.Stop()
	server.CleanupInternalSocketDir()
}

// GracefulStop stops both the secure and insecure servers gracefully
func (s *GitalyServerFactory) GracefulStop() {
	wg := sync.WaitGroup{}

	for _, srv := range s.all() {
		wg.Add(1)

		go func(s *grpc.Server) {
			s.GracefulStop()
			wg.Done()
		}(srv)
	}

	wg.Wait()
}

// Serve starts serving the listener
func (s *GitalyServerFactory) Serve(l net.Listener, secure bool) error {
	srv := s.get(secure)

	return srv.Serve(l)
}

func (s *GitalyServerFactory) get(secure bool) *grpc.Server {
	if secure {
		if s.secure == nil {
			s.secure = server.NewSecure(s.ruby, config.Config)
		}

		return s.secure
	}

	if s.insecure == nil {
		s.insecure = server.NewInsecure(s.ruby, config.Config)
	}

	return s.insecure
}

func (s *GitalyServerFactory) all() []*grpc.Server {
	var servers []*grpc.Server
	if s.secure != nil {
		servers = append(servers, s.secure)
	}

	if s.insecure != nil {
		servers = append(servers, s.insecure)
	}

	return servers
}
