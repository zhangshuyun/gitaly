package metadata

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/storage"
)

func TestOutgoingToIncoming(t *testing.T) {
	ctx := context.Background()
	ctx, err := storage.InjectGitalyServers(ctx, "a", "b", "c")
	require.NoError(t, err)

	_, err = storage.ExtractGitalyServer(ctx, "a")
	require.Equal(t, storage.ErrEmptyMetadata, err,
		"server should not be found in the incoming context")

	ctx = OutgoingToIncoming(ctx)

	info, err := storage.ExtractGitalyServer(ctx, "a")
	require.NoError(t, err)
	require.Equal(t, storage.ServerInfo{Address: "b", Token: "c"}, info)
}
