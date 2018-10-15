package helper

import (
	"encoding/base64"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/storage"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"google.golang.org/grpc/metadata"
)

func TestExtractGitalyServers(t *testing.T) {
	ctxOuter, cancel := testhelper.Context()
	defer cancel()

	testCases := []struct {
		desc     string
		metadata metadata.MD
		info     storage.GitalyServers
	}{
		{
			desc:     "no gitaly-servers metadata",
			metadata: metadata.Pairs("not-gitaly-servers", "definitely not JSON camouflaging in base64"),
		},
		{
			desc:     "metadata not encoded in base64",
			metadata: metadata.Pairs("gitaly-servers", "definitely not base64"),
		},
		{
			desc:     "encoded metadata is not JSON",
			metadata: metadata.Pairs("gitaly-servers", base64.StdEncoding.EncodeToString([]byte("definitely not JSON"))),
		},
		{
			desc:     "encoded JSON is not of the expected format",
			metadata: metadata.Pairs("gitaly-servers", base64.StdEncoding.EncodeToString([]byte(`{"default":"string"}`))),
		},
		{
			desc:     "properly-encoded string",
			metadata: metadata.Pairs("gitaly-servers", base64.StdEncoding.EncodeToString([]byte(`{"default":{"address":"unix:///tmp/sock","token":"hunter1"}}`))),
			info:     storage.GitalyServers{"default": {"address": "unix:///tmp/sock", "token": "hunter1"}},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			ctx := metadata.NewIncomingContext(ctxOuter, testCase.metadata)

			info, err := ExtractGitalyServers(ctx)
			if testCase.info == nil {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, testCase.info, info)
			}
		})
	}
}
