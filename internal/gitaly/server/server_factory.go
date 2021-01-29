package server

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	gitalyauth "gitlab.com/gitlab-org/gitaly/auth"
	"gitlab.com/gitlab-org/gitaly/client"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/hook"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/maintenance"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/rubyserver"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc"
)

// GitalyServerFactory is a factory of gitaly grpc servers
type GitalyServerFactory struct {
	mtx              sync.Mutex
	cfg              config.Cfg
	ruby             *rubyserver.Server
	hookManager      hook.Manager
	txManager        transaction.Manager
	secure, insecure []*grpc.Server
	conns            *client.Pool
}

// NewGitalyServerFactory allows to create and start secure/insecure 'grpc.Server'-s with gitaly-ruby
// server shared in between.
func NewGitalyServerFactory(cfg config.Cfg, hookManager hook.Manager, txManager transaction.Manager, conns *client.Pool) *GitalyServerFactory {
	return &GitalyServerFactory{
		cfg:         cfg,
		ruby:        &rubyserver.Server{},
		hookManager: hookManager,
		txManager:   txManager,
		conns:       conns,
	}
}

// StartRuby starts the ruby process
func (s *GitalyServerFactory) StartRuby() error {
	return s.ruby.Start()
}

// StartWorkers will start any auxiliary background workers that are allowed
// to fail without stopping the rest of the server.
func (s *GitalyServerFactory) StartWorkers(ctx context.Context, l logrus.FieldLogger, cfg config.Cfg) (func(), error) {
	var opts []grpc.DialOption
	if cfg.Auth.Token != "" {
		opts = append(opts, grpc.WithPerRPCCredentials(
			gitalyauth.RPCCredentialsV2(cfg.Auth.Token),
		))
	}

	cc, err := client.Dial("unix://"+cfg.GitalyInternalSocketPath(), opts)
	if err != nil {
		return nil, err
	}

	errQ := make(chan error)

	ctx, cancel := context.WithCancel(ctx)
	go func() {
		errQ <- maintenance.NewDailyWorker().StartDaily(
			ctx,
			l,
			cfg.DailyMaintenance,
			maintenance.OptimizeReposRandomly(
				cfg.Storages,
				gitalypb.NewRepositoryServiceClient(cc),
			),
		)
	}()

	shutdown := func() {
		cancel()

		// give the worker 5 seconds to shutdown gracefully
		timeout := 5 * time.Second

		var err error
		select {
		case err = <-errQ:
			break
		case <-time.After(timeout):
			err = fmt.Errorf("timed out after %s", timeout)
		}
		if err != nil && err != context.Canceled {
			l.WithError(err).Error("maintenance worker shutdown")
		}
	}

	return shutdown, nil
}

// Stop stops all servers started by calling Serve and the gitaly-ruby server.
func (s *GitalyServerFactory) Stop() {
	for _, srv := range s.all() {
		srv.Stop()
	}

	s.ruby.Stop()
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

// Serve starts serving on the provided listener with newly created grpc.Server
func (s *GitalyServerFactory) Serve(l net.Listener, secure bool) error {
	srv := s.create(secure)

	return srv.Serve(l)
}

func (s *GitalyServerFactory) create(secure bool) *grpc.Server {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	if secure {
		s.secure = append(s.secure, NewSecure(s.ruby, s.hookManager, s.txManager, s.cfg, s.conns))
		return s.secure[len(s.secure)-1]
	}

	s.insecure = append(s.insecure, NewInsecure(s.ruby, s.hookManager, s.txManager, s.cfg, s.conns))

	return s.insecure[len(s.insecure)-1]
}

func (s *GitalyServerFactory) all() []*grpc.Server {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	return append(s.secure[:], s.insecure...)
}
