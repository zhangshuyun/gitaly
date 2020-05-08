package service

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"gitlab.com/gitlab-org/gitaly/internal/config"
	"gitlab.com/gitlab-org/gitaly/internal/middleware/errorhandler"
	"gitlab.com/gitlab-org/gitaly/internal/rubyserver"
	"gitlab.com/gitlab-org/gitaly/internal/service/blob"
	"gitlab.com/gitlab-org/gitaly/internal/service/cleanup"
	"gitlab.com/gitlab-org/gitaly/internal/service/commit"
	"gitlab.com/gitlab-org/gitaly/internal/service/conflicts"
	"gitlab.com/gitlab-org/gitaly/internal/service/diff"
	"gitlab.com/gitlab-org/gitaly/internal/service/health"
	hook "gitlab.com/gitlab-org/gitaly/internal/service/hooks"
	"gitlab.com/gitlab-org/gitaly/internal/service/internalgitaly"
	"gitlab.com/gitlab-org/gitaly/internal/service/namespace"
	"gitlab.com/gitlab-org/gitaly/internal/service/objectpool"
	"gitlab.com/gitlab-org/gitaly/internal/service/operations"
	"gitlab.com/gitlab-org/gitaly/internal/service/ref"
	"gitlab.com/gitlab-org/gitaly/internal/service/remote"
	"gitlab.com/gitlab-org/gitaly/internal/service/repository"
	"gitlab.com/gitlab-org/gitaly/internal/service/server"
	"gitlab.com/gitlab-org/gitaly/internal/service/smarthttp"
	"gitlab.com/gitlab-org/gitaly/internal/service/ssh"
	"gitlab.com/gitlab-org/gitaly/internal/service/wiki"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc"
)

var (
	smarthttpPackfileNegotiationMetrics = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "gitaly",
			Subsystem: "smarthttp",
			Name:      "packfile_negotiation_requests_total",
			Help:      "Total number of features used for packfile negotiations",
		},
		[]string{"git_negotiation_feature"},
	)

	sshPackfileNegotiationMetrics = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "gitaly",
			Subsystem: "ssh",
			Name:      "packfile_negotiation_requests_total",
			Help:      "Total number of features used for packfile negotiations",
		},
		[]string{"git_negotiation_feature"},
	)
)

// RegisterAll will register all the known grpc services with
// the specified grpc service instance
func RegisterAll(grpcServer *grpc.Server, cfg config.Cfg, errorTracker *errorhandler.Errors, rubyServer *rubyserver.Server) {
	gitalypb.RegisterBlobServiceServer(grpcServer, blob.NewServer(rubyServer))
	gitalypb.RegisterCleanupServiceServer(grpcServer, cleanup.NewServer())
	gitalypb.RegisterCommitServiceServer(grpcServer, commit.NewServer())
	gitalypb.RegisterDiffServiceServer(grpcServer, diff.NewServer())
	gitalypb.RegisterNamespaceServiceServer(grpcServer, namespace.NewServer())
	gitalypb.RegisterOperationServiceServer(grpcServer, operations.NewServer(rubyServer))
	gitalypb.RegisterRefServiceServer(grpcServer, ref.NewServer())
	gitalypb.RegisterRepositoryServiceServer(grpcServer, repository.NewServer(rubyServer, config.GitalyInternalSocketPath()))
	gitalypb.RegisterSSHServiceServer(grpcServer, ssh.NewServer(
		ssh.WithPackfileNegotiationMetrics(sshPackfileNegotiationMetrics),
	))
	gitalypb.RegisterSmartHTTPServiceServer(grpcServer, smarthttp.NewServer(
		smarthttp.WithPackfileNegotiationMetrics(smarthttpPackfileNegotiationMetrics),
	))
	gitalypb.RegisterWikiServiceServer(grpcServer, wiki.NewServer(rubyServer))
	gitalypb.RegisterConflictsServiceServer(grpcServer, conflicts.NewServer(rubyServer))
	gitalypb.RegisterRemoteServiceServer(grpcServer, remote.NewServer(rubyServer))
	gitalypb.RegisterServerServiceServer(grpcServer, server.NewServer(cfg.Storages))
	gitalypb.RegisterObjectPoolServiceServer(grpcServer, objectpool.NewServer())
	gitalypb.RegisterHookServiceServer(grpcServer, hook.NewServer())
	gitalypb.RegisterInternalGitalyServer(grpcServer, internalgitaly.NewServer(config.Config.Storages))

	gitalypb.RegisterHealthServer(grpcServer, health.NewServer(errorTracker))
}
