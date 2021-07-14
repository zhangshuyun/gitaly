package streamrpc

import (
	"context"
	"fmt"
	"io/ioutil"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	testpb "gitlab.com/gitlab-org/gitaly/v14/internal/streamrpc/testdata"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/emptypb"
)

// TestProtocol exclusively uses hard-coded strings to prevent breaking
// changes in the wire protocol.
func TestProtocol(t *testing.T) {
	testCases := []struct {
		desc string
		in   string
		out  string
	}{
		{
			desc: "successful request",
			in:   "\x00\x00\x00\x28" + `{"Method":"/test.streamrpc.Test/Stream"}`,
			out: strings.Join([]string{
				"\x00\x00\x00\x00", // Server accepts
				"\n",               // Handler prints request field (empty) followed by newline
			}, ""),
		},
		{
			desc: "unknown method",
			in:   "\x00\x00\x00\x1b" + `{"Method":"does not exist"}`,
			out: strings.Join([]string{
				"\x00\x00\x00\x2c", // Server rejects by sending non-empty error message
				`{"Error":"method not found: does not exist"}`,
			}, ""),
		},
		{
			desc: "request with message and metadata",
			in: strings.Join([]string{
				"\x00\x00\x00\x73",
				`{`,
				`"Method":"/test.streamrpc.Test/Stream",`,
				`"Message":"EgtoZWxsbyB3b3JsZA==",`, // &testpb.StreamRequest{StringField: "hello world"}
				`"Metadata":{"k1":["v1","v2"],"k2":["v3"]}`,
				`}`,
			}, ""),
			out: strings.Join([]string{
				"\x00\x00\x00\x00",         // Server accepts
				"k1: v1\nk1: v2\nk2: v3\n", // Server echoes metadata key-value pairs
				"hello world\n",            // Server echoes field from request message
			}, ""),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			dial := startServer(
				t,
				NewServer(),
				func(ctx context.Context, in *testpb.StreamRequest) (*emptypb.Empty, error) {
					c, err := AcceptConnection(ctx)
					if err != nil {
						return nil, err
					}

					var mdKeys []string
					var md metadata.MD
					if ctxMD, ok := metadata.FromIncomingContext(ctx); ok {
						md = ctxMD
					}

					for k := range md {
						mdKeys = append(mdKeys, k)
					}

					// Direct go map iteration is non-deterministic. Sort the keys to make it
					// deterministic.
					sort.Strings(mdKeys)

					// Echo back metadata so tests can see it was received correctly
					for _, k := range mdKeys {
						for _, v := range md[k] {
							if _, err := fmt.Fprintf(c, "%s: %s\n", k, v); err != nil {
								return nil, err
							}
						}
					}

					// Echo back string field so tests can see request was received correctly
					if _, err := fmt.Fprintln(c, in.StringField); err != nil {
						return nil, err
					}

					return nil, nil
				},
			)

			c, err := dial(10 * time.Second)
			require.NoError(t, err)
			defer c.Close()
			require.NoError(t, c.SetDeadline(time.Now().Add(10*time.Second)))

			n, err := c.Write([]byte(tc.in))
			require.NoError(t, err)
			require.Equal(t, len(tc.in), n)

			out, err := ioutil.ReadAll(c)
			require.NoError(t, err)
			require.Equal(t, tc.out, string(out))
		})
	}
}
