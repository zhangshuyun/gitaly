package helper

import (
	"encoding/base64"
	"encoding/json"
	"fmt"

	"gitlab.com/gitlab-org/gitaly/internal/storage"
	"golang.org/x/net/context"
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
