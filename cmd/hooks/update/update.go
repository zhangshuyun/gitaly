package main

import (
	"context"
	"log"
	"os"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	gitalyauth "gitlab.com/gitlab-org/gitaly/auth"
	"gitlab.com/gitlab-org/gitaly/client"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	grpccorrelation "gitlab.com/gitlab-org/labkit/correlation/grpc"
	grpctracing "gitlab.com/gitlab-org/labkit/tracing/grpc"
	"google.golang.org/grpc"
)

func main() {
	keyID := os.Getenv("GL_ID")
	repoPath, err := os.Getwd()
	if err != nil {
		log.Fatalf("error when getting pwd: %v", err)
	}

	if len(os.Args) < 4 {
		log.Fatalf("only found %d arguments, 3 required", len(os.Args)-1)
	}

	refName, oldVal, newVal := os.Args[1], os.Args[2], os.Args[3]

	url := os.Getenv("GL_URL")

	conn, err := client.Dial(url, dialOpts())
	if err != nil {
		log.Fatalf("error when dialing: %v", err)
	}

	c := gitalypb.NewHookServiceClient(conn)

	if _, err = c.Update(context.Background(), &gitalypb.UpdateHookRequest{
		RepoPath: repoPath,
		KeyId:    keyID,
		Ref:      refName,
		OldValue: oldVal,
		NewValue: newVal,
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
