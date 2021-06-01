package main

import (
	"context"
	"fmt"
	"os"

	"github.com/golang/protobuf/jsonpb"
	"gitlab.com/gitlab-org/gitaly/v14/client"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"google.golang.org/grpc"
)

func receivePack(ctx context.Context, conn *grpc.ClientConn, req string) (int32, error) {
	var request gitalypb.SSHReceivePackRequest
	if err := jsonpb.UnmarshalString(req, &request); err != nil {
		return 0, fmt.Errorf("json unmarshal: %w", err)
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	return client.ReceivePack(ctx, conn, os.Stdin, os.Stdout, os.Stderr, &request)
}
