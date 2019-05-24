package cleanup_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly-proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"google.golang.org/grpc/metadata"
)

func TestCloseSession(t *testing.T) {
	server, serverSocketPath := runCleanupServiceServer(t)
	defer server.Stop()

	client, conn := newCleanupServiceClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	ctx, cancel := testhelper.Context()
	defer cancel()

	md := metadata.New(map[string]string{
		featureflag.HeaderKey(catfile.CacheFeatureFlagKey): "true",
		"gitaly-session-id": t.Name(),
	})

	ctx = metadata.NewOutgoingContext(ctx, md)

	catfile.New(ctx, testRepo)

	ctx, cancel = testhelper.Context()
	defer cancel()
	_, err := client.CloseSession(ctx, &gitalypb.CloseSessionRequest{
		SessionId: t.Name(),
	})

	require.NoError(t, err)
}
