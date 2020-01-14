package hook

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/config"
	"gitlab.com/gitlab-org/gitaly/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/streamio"
	"google.golang.org/grpc/codes"
)

func TestPostReceiveInvalidArgument(t *testing.T) {
	server, serverSocketPath := runHooksServer(t)
	defer server.Stop()

	client, conn := newHooksClient(t, serverSocketPath)
	defer conn.Close()

	ctx, cancel := testhelper.Context()
	defer cancel()

	stream, err := client.PostReceiveHook(ctx)
	require.NoError(t, err)
	require.NoError(t, stream.Send(&gitalypb.PostReceiveHookRequest{}), "empty repository should result in an error")
	_, err = stream.Recv()

	testhelper.RequireGrpcError(t, err, codes.InvalidArgument)
}

func TestPostReceive(t *testing.T) {
	rubyDir := config.Config.Ruby.Dir
	defer func(rubyDir string) {
		config.Config.Ruby.Dir = rubyDir
	}(rubyDir)

	cwd, err := os.Getwd()
	require.NoError(t, err)
	config.Config.Ruby.Dir = filepath.Join(cwd, "testdata")

	server, serverSocketPath := runHooksServer(t)
	defer server.Stop()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	client, conn := newHooksClient(t, serverSocketPath)
	defer conn.Close()

	testCases := []struct {
		desc   string
		stdin  io.Reader
		req    gitalypb.PostReceiveHookRequest
		status int32
		stdout string
		stderr string
	}{
		{
			desc:   "valid stdin",
			stdin:  bytes.NewBufferString("a\nb\nc\nd\ne\nf\ng"),
			req:    gitalypb.PostReceiveHookRequest{Repository: testRepo, KeyId: "key_id", GitPushOptions: []string{"option0", "option1"}},
			status: 0,
			stdout: "OK",
			stderr: "",
		},
		{
			desc:   "missing stdin",
			stdin:  bytes.NewBuffer(nil),
			req:    gitalypb.PostReceiveHookRequest{Repository: testRepo, KeyId: "key_id", GitPushOptions: []string{"option0"}},
			status: 1,
			stdout: "",
			stderr: "FAIL",
		},
		{
			desc:   "missing key_id",
			stdin:  bytes.NewBuffer(nil),
			req:    gitalypb.PostReceiveHookRequest{Repository: testRepo, GitPushOptions: []string{"option0"}},
			status: 1,
			stdout: "",
			stderr: "FAIL",
		},
		{
			desc:   "missing git push option",
			stdin:  bytes.NewBuffer(nil),
			req:    gitalypb.PostReceiveHookRequest{Repository: testRepo, KeyId: "key_id"},
			status: 1,
			stdout: "",
			stderr: "FAIL",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			ctx, cancel := testhelper.Context()
			defer cancel()

			stream, err := client.PostReceiveHook(ctx)
			require.NoError(t, err)
			require.NoError(t, stream.Send(&tc.req))

			go func() {
				writer := streamio.NewWriter(func(p []byte) error {
					return stream.Send(&gitalypb.PostReceiveHookRequest{Stdin: p})
				})
				_, err := io.Copy(writer, tc.stdin)
				require.NoError(t, err)
				require.NoError(t, stream.CloseSend(), "close send")
			}()

			var status int32
			var stdout, stderr bytes.Buffer
			for {
				resp, err := stream.Recv()
				if err == io.EOF {
					break
				}

				_, err = stdout.Write(resp.GetStdout())
				require.NoError(t, err)
				stderr.Write(resp.GetStderr())
				status = resp.GetExitStatus().GetValue()
			}

			require.Equal(t, tc.status, status)
			assert.Equal(t, tc.stderr, text.ChompBytes(stderr.Bytes()), "hook stderr")
			assert.Equal(t, tc.stdout, text.ChompBytes(stdout.Bytes()), "hook stdout")
		})
	}
}
