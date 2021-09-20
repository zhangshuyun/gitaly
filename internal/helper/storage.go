package helper

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"

	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/storage"
	"google.golang.org/grpc/metadata"
)

// ErrEmptyMetadata indicates that the gRPC metadata was not found in the
// context
var ErrEmptyMetadata = errors.New("empty metadata")

// ExtractGitalyServers extracts `storage.GitalyServers` from an incoming context.
func ExtractGitalyServers(ctx context.Context) (gitalyServersInfo storage.GitalyServers, err error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, ErrEmptyMetadata
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
func ExtractGitalyServer(ctx context.Context, storageName string) (storage.ServerInfo, error) {
	gitalyServers, err := ExtractGitalyServers(ctx)
	if err != nil {
		return storage.ServerInfo{}, err
	}

	gitalyServer, ok := gitalyServers[storageName]
	if !ok {
		return storage.ServerInfo{}, errors.New("storage name not found")
	}

	return gitalyServer, nil
}

// InjectGitalyServers injects gitaly-servers metadata into an outgoing context
func InjectGitalyServers(ctx context.Context, name, address, token string) (context.Context, error) {
	gitalyServers := storage.GitalyServers{
		name: {
			Address: address,
			Token:   token,
		},
	}

	gitalyServersJSON, err := json.Marshal(gitalyServers)
	if err != nil {
		return nil, err
	}

	return metadata.AppendToOutgoingContext(ctx, "gitaly-servers", base64.StdEncoding.EncodeToString(gitalyServersJSON)), nil
}
