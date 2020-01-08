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
	"google.golang.org/grpc/codes"
)

func TestUpdateInvalidArgument(t *testing.T) {
	stop, serverSocketPath := runHooksServer(t)
	defer stop()

	client, conn := newHooksClient(t, serverSocketPath)
	defer conn.Close()

	ctx, cancel := testhelper.Context()
	defer cancel()

	stream, err := client.UpdateHook(ctx, &gitalypb.UpdateHookRequest{})
	require.NoError(t, err)
	_, err = stream.Recv()

	testhelper.RequireGrpcError(t, err, codes.InvalidArgument)
}

func TestUpdate(t *testing.T) {
	rubyDir := config.Config.Ruby.Dir
	defer func() {
		config.Config.Ruby.Dir = rubyDir
	}()

	cwd, err := os.Getwd()
	require.NoError(t, err)
	config.Config.Ruby.Dir = filepath.Join(cwd, "testdata")

	stop, serverSocketPath := runHooksServer(t)
	defer stop()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	client, conn := newHooksClient(t, serverSocketPath)
	defer conn.Close()

	ctx, cancel := testhelper.Context()
	defer cancel()

	testCases := []struct {
		desc           string
		req            gitalypb.UpdateHookRequest
		status         int32
		stdout, stderr string
	}{
		{
			desc: "valid inputs",
			req: gitalypb.UpdateHookRequest{
				Repository: testRepo,
				Ref:        []byte("master"),
				OldValue:   "a",
				NewValue:   "b",
				KeyId:      "key",
			},
			status: 0,
			stdout: "OK",
			stderr: "",
		},
		{
			desc: "missing ref",
			req: gitalypb.UpdateHookRequest{
				Repository: testRepo,
				Ref:        nil,
				OldValue:   "a",
				NewValue:   "b",
				KeyId:      "key",
			},
			status: 1,
			stdout: "",
			stderr: "FAIL",
		},
		{
			desc: "missing old value",
			req: gitalypb.UpdateHookRequest{
				Repository: testRepo,
				Ref:        []byte("master"),
				OldValue:   "",
				NewValue:   "b",
				KeyId:      "key",
			},
			status: 1,
			stdout: "",
			stderr: "FAIL",
		},
		{
			desc: "missing new value",
			req: gitalypb.UpdateHookRequest{
				Repository: testRepo,
				Ref:        []byte("master"),
				OldValue:   "a",
				NewValue:   "",
				KeyId:      "key",
			},
			status: 1,
			stdout: "",
			stderr: "FAIL",
		},
		{
			desc: "missing key_id value",
			req: gitalypb.UpdateHookRequest{
				Repository: testRepo,
				Ref:        []byte("master"),
				OldValue:   "a",
				NewValue:   "b",
				KeyId:      "",
			},
			status: 1,
			stdout: "",
			stderr: "FAIL",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			stream, err := client.UpdateHook(ctx, &tc.req)
			require.NoError(t, err)

			var status int32
			var stderr, stdout bytes.Buffer
			for {
				resp, err := stream.Recv()
				if err == io.EOF {
					break
				}

				stderr.Write(resp.GetStderr())
				stdout.Write(resp.GetStdout())

				if err != nil {
					t.Errorf("error when receiving stream: %v", err)
				}

				status = resp.GetExitStatus().GetValue()
				require.NoError(t, err)
			}

			require.Equal(t, tc.status, status)
			assert.Equal(t, tc.stderr, text.ChompBytes(stderr.Bytes()), "hook stderr")
			assert.Equal(t, tc.stdout, text.ChompBytes(stdout.Bytes()), "hook stdout")
		})
	}
}
