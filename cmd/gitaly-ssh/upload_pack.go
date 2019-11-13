package main

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/golang/protobuf/jsonpb"
	"gitlab.com/gitlab-org/gitaly/client"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc"
)

func uploadPack(ctx context.Context, conn *grpc.ClientConn, req string) (int32, error) {
	var request gitalypb.SSHUploadPackRequest
	if err := jsonpb.UnmarshalString(req, &request); err != nil {
		return 0, fmt.Errorf("json unmarshal: %v", err)
	}

	gitConfig := os.Getenv("GL_CONFIG_OPTIONS")

	if len(gitConfig) > 0 {
		request.GitConfigOptions = append(request.GitConfigOptions, strings.Split(gitConfig, " ")...)
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	return client.UploadPack(ctx, conn, os.Stdin, os.Stdout, os.Stderr, &request)
}
