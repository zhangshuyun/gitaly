package service

import (
	pb "gitlab.com/gitlab-org/gitaly-proto/go"
	"gitlab.com/gitlab-org/gitaly/internal/service/commit"
	"gitlab.com/gitlab-org/gitaly/internal/service/diff"
	"gitlab.com/gitlab-org/gitaly/internal/service/notifications"
	"gitlab.com/gitlab-org/gitaly/internal/service/ref"
	"gitlab.com/gitlab-org/gitaly/internal/service/smarthttp"
	"gitlab.com/gitlab-org/gitaly/internal/service/ssh"

	"google.golang.org/grpc"
)

// RegisterAll will register all the known grpc services with
// the specified grpc service instance
func RegisterAll(grpcServer *grpc.Server) {
	pb.RegisterNotificationsServer(grpcServer, notifications.NewServer())
	pb.RegisterRefServer(grpcServer, ref.NewServer())
	pb.RegisterSmartHTTPServer(grpcServer, smarthttp.NewServer())
	pb.RegisterDiffServer(grpcServer, diff.NewServer())
	pb.RegisterCommitServer(grpcServer, commit.NewServer())
	pb.RegisterSSHServer(grpcServer, ssh.NewServer())
}
