package main

import (
	"context"
	"fmt"
	"os"

	"gitlab.com/gitlab-org/gitaly/v14/client"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/protojson"
)

func receivePack(ctx context.Context, conn *grpc.ClientConn, req string) (int32, error) {
	var request gitalypb.SSHReceivePackRequest

	if err := protojson.Unmarshal([]byte(req), &request); err != nil {
		return 0, fmt.Errorf("json unmarshal: %w", err)
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	return client.ReceivePack(ctx, conn, os.Stdin, os.Stdout, os.Stderr, &request)
}
