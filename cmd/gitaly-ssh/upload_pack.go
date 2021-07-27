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

const (
	// GitConfigShowAllRefs is a git-config option.
	// We have to use a negative transfer.hideRefs since this is the only way
	// to undo an already set parameter: https://www.spinics.net/lists/git/msg256772.html
	GitConfigShowAllRefs = "transfer.hideRefs=!refs"
)

func uploadPack(ctx context.Context, conn *grpc.ClientConn, req string) (int32, error) {
	var request gitalypb.SSHUploadPackRequest
	if err := protojson.Unmarshal([]byte(req), &request); err != nil {
		return 0, fmt.Errorf("json unmarshal: %w", err)
	}

	request.GitConfigOptions = append([]string{GitConfigShowAllRefs}, request.GitConfigOptions...)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	return client.UploadPack(ctx, conn, os.Stdin, os.Stdout, os.Stderr, &request)
}
