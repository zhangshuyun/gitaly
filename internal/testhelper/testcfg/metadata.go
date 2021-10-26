package testcfg

import (
	"encoding/base64"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/storage"
	"google.golang.org/grpc/metadata"
)

// GitalyServersMetadataFromCfg returns a metadata pair for gitaly-servers to be used in
// inter-gitaly operations.
func GitalyServersMetadataFromCfg(t testing.TB, cfg config.Cfg) metadata.MD {
	gitalyServers := storage.GitalyServers{}
storages:
	for _, s := range cfg.Storages {
		// It picks up the first address configured: TLS, TCP or UNIX.
		for _, addr := range []string{cfg.TLSListenAddr, cfg.ListenAddr, cfg.SocketPath} {
			if addr != "" {
				gitalyServers[s.Name] = storage.ServerInfo{
					Address: addr,
					Token:   cfg.Auth.Token,
				}
				continue storages
			}
		}
		require.FailNow(t, "no address found on the config")
	}

	gitalyServersJSON, err := json.Marshal(gitalyServers)
	if err != nil {
		t.Fatal(err)
	}

	return metadata.Pairs("gitaly-servers", base64.StdEncoding.EncodeToString(gitalyServersJSON))
}
