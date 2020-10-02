package gitalyssh

import (
	"context"
	"encoding/base64"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/golang/protobuf/jsonpb"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v13/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v13/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v13/proto/go/gitalypb"
	"gitlab.com/gitlab-org/labkit/correlation"
	"google.golang.org/grpc/metadata"
)

func TestUploadPackEnv(t *testing.T) {
	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	ctx, cancel := testhelper.Context()
	defer cancel()

	md := metadata.Pairs("gitaly-servers", base64.StdEncoding.EncodeToString([]byte(`{"default":{"address":"unix:///tmp/sock","token":"hunter1"}}`)))
	ctx = metadata.NewIncomingContext(ctx, md)
	ctx = correlation.ContextWithCorrelation(ctx, "correlation-id-1")

	req := gitalypb.SSHUploadPackRequest{
		Repository: testRepo,
	}

	var pbMarshaler jsonpb.Marshaler
	expectedPayload, err := pbMarshaler.MarshalToString(&req)
	require.NoError(t, err)

	env, err := UploadPackEnv(ctx, &req)

	require.NoError(t, err)
	require.Subset(t, env, []string{
		fmt.Sprintf("GIT_SSH_COMMAND=%s upload-pack", filepath.Join(config.Config.BinDir, "gitaly-ssh")),
		fmt.Sprintf("GITALY_PAYLOAD=%s", expectedPayload),
		"CORRELATION_ID=correlation-id-1",
	})
}

func TestGetCorrelationID(t *testing.T) {
	t.Run("not provided in context", func(t *testing.T) {
		ctx := context.Background()
		cid1 := getCorrelationID(ctx)
		require.NotEmpty(t, cid1)

		cid2 := getCorrelationID(ctx)
		require.NotEqual(t, cid1, cid2, "it should return a new correlation_id each time as it is not injected into the context")
	})

	t.Run("provided in context", func(t *testing.T) {
		const cid = "1-2-3-4"
		ctx := correlation.ContextWithCorrelation(context.Background(), cid)

		require.Equal(t, cid, getCorrelationID(ctx))
		require.Equal(t, cid, getCorrelationID(ctx))
	})
}
