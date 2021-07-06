package server

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	gitalyauth "gitlab.com/gitlab-org/gitaly/v14/auth"
	"gitlab.com/gitlab-org/gitaly/v14/client"
	"gitlab.com/gitlab-org/gitaly/v14/internal/backchannel"
	"gitlab.com/gitlab-org/gitaly/v14/internal/cache"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/maintenance"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"google.golang.org/grpc"
)

// GitalyServerFactory is a factory of gitaly grpc servers
type GitalyServerFactory struct {
	registry         *backchannel.Registry
	cacheInvalidator cache.Invalidator
	cfg              config.Cfg
	logger           *logrus.Entry
	externalServers  []*grpc.Server
	internalServers  []*grpc.Server
}

// NewGitalyServerFactory allows to create and start secure/insecure 'grpc.Server'-s with gitaly-ruby
// server shared in between.
func NewGitalyServerFactory(
	cfg config.Cfg,
	logger *logrus.Entry,
	registry *backchannel.Registry,
	cacheInvalidator cache.Invalidator,
) *GitalyServerFactory {
	return &GitalyServerFactory{
		cfg:              cfg,
		logger:           logger,
		registry:         registry,
		cacheInvalidator: cacheInvalidator,
	}
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

	cc, err := client.Dial("unix:"+cfg.GitalyInternalSocketPath(), opts)
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
				helper.NewTimerTicker(1*time.Second),
				rand.New(rand.NewSource(time.Now().UnixNano())),
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

// Stop immediately stops all servers created by the GitalyServerFactory.
func (s *GitalyServerFactory) Stop() {
	for _, servers := range [][]*grpc.Server{
		s.externalServers,
		s.internalServers,
	} {
		for _, server := range servers {
			server.Stop()
		}
	}
}

// GracefulStop gracefully stops all servers created by the GitalyServerFactory. ExternalServers
// are stopped before the internal servers to ensure any RPCs accepted by the externals servers
// can still complete their requests to the internal servers. This is important for hooks calling
// back to Gitaly.
func (s *GitalyServerFactory) GracefulStop() {
	for _, servers := range [][]*grpc.Server{
		s.externalServers,
		s.internalServers,
	} {
		var wg sync.WaitGroup

		for _, server := range servers {
			wg.Add(1)
			go func(server *grpc.Server) {
				defer wg.Done()
				server.GracefulStop()
			}(server)
		}

		wg.Wait()
	}
}

// CreateExternal creates a new external gRPC server. The external servers are closed
// before the internal servers when gracefully shutting down.
func (s *GitalyServerFactory) CreateExternal(secure bool) (*grpc.Server, error) {
	server, err := New(secure, s.cfg, s.logger, s.registry, s.cacheInvalidator)
	if err != nil {
		return nil, err
	}

	s.externalServers = append(s.externalServers, server)
	return server, nil
}

// CreateInternal creates a new internal gRPC server. Internal servers are closed
// after the external ones when gracefully shutting down.
func (s *GitalyServerFactory) CreateInternal() (*grpc.Server, error) {
	server, err := New(false, s.cfg, s.logger, s.registry, s.cacheInvalidator)
	if err != nil {
		return nil, err
	}

	s.internalServers = append(s.internalServers, server)
	return server, nil
}
