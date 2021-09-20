package metadata

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper"
)

func TestOutgoingToIncoming(t *testing.T) {
	ctx := context.Background()
	ctx, err := helper.InjectGitalyServers(ctx, "a", "b", "c")
	require.NoError(t, err)

	_, err = helper.ExtractGitalyServer(ctx, "a")
	require.Equal(t, helper.ErrEmptyMetadata, err,
		"server should not be found in the incoming context")

	ctx = OutgoingToIncoming(ctx)

	info, err := helper.ExtractGitalyServer(ctx, "a")
	require.NoError(t, err)
	require.Equal(t, storage.ServerInfo{Address: "b", Token: "c"}, info)
}
