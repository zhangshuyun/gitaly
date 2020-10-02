package helper

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"

	"gitlab.com/gitlab-org/gitaly/v13/internal/storage"
	"google.golang.org/grpc/metadata"
)

// ExtractGitalyServers extracts `storage.GitalyServers` from an incoming context.
func ExtractGitalyServers(ctx context.Context) (gitalyServersInfo storage.GitalyServers, err error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, fmt.Errorf("empty metadata")
	}

	gitalyServersJSONEncoded := md["gitaly-servers"]
	if len(gitalyServersJSONEncoded) == 0 {
		return nil, fmt.Errorf("empty gitaly-servers metadata")
	}

	gitalyServersJSON, err := base64.StdEncoding.DecodeString(gitalyServersJSONEncoded[0])
	if err != nil {
		return nil, fmt.Errorf("failed decoding base64: %v", err)
	}

	if err := json.Unmarshal(gitalyServersJSON, &gitalyServersInfo); err != nil {
		return nil, fmt.Errorf("failed unmarshalling json: %v", err)
	}

	return
}

// ExtractGitalyServer extracts server information for a specific storage
func ExtractGitalyServer(ctx context.Context, storageName string) (map[string]string, error) {
	gitalyServers, err := ExtractGitalyServers(ctx)
	if err != nil {
		return nil, err
	}

	gitalyServer, ok := gitalyServers[storageName]
	if !ok {
		return nil, errors.New("storage name not found")
	}

	return gitalyServer, nil
}

// IncomingToOutgoing creates an outgoing context out of an incoming context with the same storage metadata
func IncomingToOutgoing(ctx context.Context) context.Context {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ctx
	}

	return metadata.NewOutgoingContext(ctx, md)
}

// InjectGitalyServers injects gitaly-servers metadata into an outgoing context
func InjectGitalyServers(ctx context.Context, name, address, token string) (context.Context, error) {
	gitalyServers := storage.GitalyServers{
		name: {
			"address": address,
			"token":   token,
		},
	}

	gitalyServersJSON, err := json.Marshal(gitalyServers)
	if err != nil {
		return nil, err
	}

	return metadata.NewOutgoingContext(ctx, metadata.Pairs("gitaly-servers", base64.StdEncoding.EncodeToString(gitalyServersJSON))), nil
}
