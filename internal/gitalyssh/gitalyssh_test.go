package gitalyssh

import (
	"encoding/base64"
	"fmt"
	"testing"

	"github.com/golang/protobuf/jsonpb"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
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

	env, err := UploadPackEnv(ctx, config.Cfg{BinDir: "/path/bin"}, &req)

	require.NoError(t, err)
	require.Subset(t, env, []string{
		"GIT_SSH_COMMAND=/path/bin/gitaly-ssh upload-pack",
		fmt.Sprintf("GITALY_PAYLOAD=%s", expectedPayload),
		"CORRELATION_ID=correlation-id-1",
		"GIT_SSH_VARIANT=simple",
	})
}
