package service

import (
	"gitlab.com/gitlab-org/gitaly/internal/rubyserver"
	"gitlab.com/gitlab-org/gitaly/internal/service/blob"
	"gitlab.com/gitlab-org/gitaly/internal/service/cleanup"
	"gitlab.com/gitlab-org/gitaly/internal/service/commit"
	"gitlab.com/gitlab-org/gitaly/internal/service/conflicts"
	"gitlab.com/gitlab-org/gitaly/internal/service/diff"
	hook "gitlab.com/gitlab-org/gitaly/internal/service/hooks"
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
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

// RegisterAll will register all the known grpc services with
// the specified grpc service instance
func RegisterAll(grpcServer *grpc.Server, rubyServer *rubyserver.Server) {
	gitalypb.RegisterBlobServiceServer(grpcServer, blob.NewServer(rubyServer))
	gitalypb.RegisterCleanupServiceServer(grpcServer, cleanup.NewServer())
	gitalypb.RegisterCommitServiceServer(grpcServer, commit.NewServer(rubyServer))
	gitalypb.RegisterDiffServiceServer(grpcServer, diff.NewServer(rubyServer))
	gitalypb.RegisterNamespaceServiceServer(grpcServer, namespace.NewServer())
	gitalypb.RegisterOperationServiceServer(grpcServer, operations.NewServer(rubyServer))
	gitalypb.RegisterRefServiceServer(grpcServer, ref.NewServer(rubyServer))
	gitalypb.RegisterRepositoryServiceServer(grpcServer, repository.NewServer(rubyServer))
	gitalypb.RegisterSSHServiceServer(grpcServer, ssh.NewServer())
	gitalypb.RegisterSmartHTTPServiceServer(grpcServer, smarthttp.NewServer())
	gitalypb.RegisterWikiServiceServer(grpcServer, wiki.NewServer(rubyServer))
	gitalypb.RegisterConflictsServiceServer(grpcServer, conflicts.NewServer(rubyServer))
	gitalypb.RegisterRemoteServiceServer(grpcServer, remote.NewServer(rubyServer))
	gitalypb.RegisterServerServiceServer(grpcServer, server.NewServer())
	gitalypb.RegisterObjectPoolServiceServer(grpcServer, objectpool.NewServer())
	gitalypb.RegisterHookServiceServer(grpcServer, hook.NewServer())

	healthpb.RegisterHealthServer(grpcServer, health.NewServer())
}
