package main

import (
	"context"
	"log"
	"os"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	"gitlab.com/gitlab-org/gitaly-proto/go/gitalypb"
	gitalyauth "gitlab.com/gitlab-org/gitaly/auth"
	"gitlab.com/gitlab-org/gitaly/client"
	"gitlab.com/gitlab-org/gitaly/cmd/hooks"
	grpccorrelation "gitlab.com/gitlab-org/labkit/correlation/grpc"
	grpctracing "gitlab.com/gitlab-org/labkit/tracing/grpc"
	"google.golang.org/grpc"
)

func main() {
	refs, err := hooks.ReadRefs(os.Stdin)
	if err != nil {
		log.Fatalf("error when reading refs: %v", err)
	}
	keyID := os.Getenv("GL_ID")
	protocol := os.Getenv("GL_PROTOCOL")
	repoPath, err := os.Getwd()
	if err != nil {
		log.Fatalf("error when getting pwd: %v", err)
	}
	glRepository := os.Getenv("GL_REPOSITORY")
	url := os.Getenv("GL_URL")

	conn, err := client.Dial(url, dialOpts())
	if err != nil {
		log.Fatalf("error when dialing: %v", err)
	}

	c := gitalypb.NewHookServiceClient(conn)

	if _, err = c.PreReceive(context.Background(), &gitalypb.PreReceiveHookRequest{
		GlRepository: glRepository,
		RepoPath:     repoPath,
		KeyId:        keyID,
		Protocol:     protocol,
		Refs:         refs,
	}); err != nil {
		log.Fatalf("error when calling pre receive hook: %v", err)
	}
}

func dialOpts() []grpc.DialOption {
	connOpts := client.DefaultDialOpts
	if token := os.Getenv("GITALY_TOKEN"); token != "" {
		connOpts = append(connOpts, grpc.WithPerRPCCredentials(gitalyauth.RPCCredentials(token)))
	}

	// Add grpc client interceptors
	connOpts = append(connOpts, grpc.WithStreamInterceptor(
		grpc_middleware.ChainStreamClient(
			grpctracing.StreamClientTracingInterceptor(),         // Tracing
			grpccorrelation.StreamClientCorrelationInterceptor(), // Correlation
		)),

		grpc.WithUnaryInterceptor(
			grpc_middleware.ChainUnaryClient(
				grpctracing.UnaryClientTracingInterceptor(),         // Tracing
				grpccorrelation.UnaryClientCorrelationInterceptor(), // Correlation
			)))

	return connOpts
}
