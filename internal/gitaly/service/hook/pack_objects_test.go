package hook

import (
	"bytes"
	"io"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
)

func TestServer_PackObjectsHook_invalidArgument(t *testing.T) {
	serverSocketPath, stop := runHooksServer(t, config.Config)
	defer stop()

	testRepo, _, cleanupFn := gittest.CloneRepo(t)
	defer cleanupFn()

	client, conn := newHooksClient(t, serverSocketPath)
	defer conn.Close()

	ctx, cancel := testhelper.Context()
	defer cancel()

	testCases := []struct {
		desc string
		req  *gitalypb.PackObjectsHookRequest
	}{
		{desc: "empty", req: &gitalypb.PackObjectsHookRequest{}},
		{desc: "repo, no args", req: &gitalypb.PackObjectsHookRequest{Repository: testRepo}},
		{desc: "repo, bad args", req: &gitalypb.PackObjectsHookRequest{Repository: testRepo, Args: []string{"rm", "-rf"}}},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			stream, err := client.PackObjectsHook(ctx)
			require.NoError(t, err)
			require.NoError(t, stream.Send(tc.req))

			_, err = stream.Recv()
			testhelper.RequireGrpcError(t, err, codes.InvalidArgument)
		})
	}
}

func TestServer_PackObjectsHook(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	testRepo, testRepoPath, cleanupFn := gittest.CloneRepo(t)
	defer cleanupFn()

	testCases := []struct {
		desc  string
		stdin string
		args  []string
	}{
		{
			desc:  "clone 1 branch",
			stdin: "3dd08961455abf80ef9115f4afdc1c6f968b503c\n--not\n\n",
			args:  []string{"pack-objects", "--revs", "--thin", "--stdout", "--progress", "--delta-base-offset"},
		},
		{
			desc:  "shallow clone 1 branch",
			stdin: "--shallow 1e292f8fedd741b75372e19097c76d327140c312\n1e292f8fedd741b75372e19097c76d327140c312\n--not\n\n",
			args:  []string{"--shallow-file", "", "pack-objects", "--revs", "--thin", "--stdout", "--shallow", "--progress", "--delta-base-offset", "--include-tag"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			logger, hook := test.NewNullLogger()

			serverSocketPath, stop := runHooksServerWithLogger(t, config.Config, logger)
			defer stop()

			client, conn := newHooksClient(t, serverSocketPath)
			defer conn.Close()

			stream, err := client.PackObjectsHook(ctx)
			require.NoError(t, err)

			require.NoError(t, stream.Send(&gitalypb.PackObjectsHookRequest{
				Repository: testRepo,
				Args:       tc.args,
			}))

			require.NoError(t, stream.Send(&gitalypb.PackObjectsHookRequest{
				Stdin: []byte(tc.stdin),
			}))
			require.NoError(t, stream.CloseSend())

			var stdout []byte
			for err == nil {
				var resp *gitalypb.PackObjectsHookResponse
				resp, err = stream.Recv()
				stdout = append(stdout, resp.GetStdout()...)
				if stderr := resp.GetStderr(); len(stderr) > 0 {
					t.Log(string(stderr))
				}
			}
			require.Equal(t, io.EOF, err)

			testhelper.MustRunCommand(
				t,
				bytes.NewReader(stdout),
				"git", "-C", testRepoPath, "index-pack", "--stdin", "--fix-thin",
			)

			var stats *logrus.Entry
			for _, e := range hook.AllEntries() {
				if e.Message == "git-pack-objects stats" {
					stats = e
				}
			}

			require.NotNil(t, stats)
			require.NotEmpty(t, stats.Data["cache_key"])
			for _, k := range []string{"stdin_bytes", "stdout_bytes", "stderr_bytes"} {
				require.Greater(t, stats.Data[k], int64(0), k)
			}
		})
	}
}

func TestParsePackObjectsArgs(t *testing.T) {
	testCases := []struct {
		desc string
		args []string
		out  *packObjectsArgs
		err  error
	}{
		{desc: "no args", args: []string{"pack-objects", "--stdout"}, out: &packObjectsArgs{}},
		{desc: "no args shallow", args: []string{"--shallow-file", "", "pack-objects", "--stdout"}, out: &packObjectsArgs{shallowFile: true}},
		{desc: "with args", args: []string{"pack-objects", "--foo", "-x", "--stdout"}, out: &packObjectsArgs{flags: []string{"--foo", "-x"}}},
		{desc: "with args shallow", args: []string{"--shallow-file", "", "pack-objects", "--foo", "--stdout", "-x"}, out: &packObjectsArgs{shallowFile: true, flags: []string{"--foo", "-x"}}},
		{desc: "missing stdout", args: []string{"pack-objects"}, err: errNoStdout},
		{desc: "no pack objects", args: []string{"zpack-objects"}, err: errNoPackObjects},
		{desc: "non empty shallow", args: []string{"--shallow-file", "z", "pack-objects"}, err: errNoPackObjects},
		{desc: "bad global", args: []string{"-c", "foo=bar", "pack-objects"}, err: errNoPackObjects},
		{desc: "non flag arg", args: []string{"pack-objects", "--foo", "x"}, err: errNonFlagArg},
		{desc: "non flag arg shallow", args: []string{"--shallow-file", "", "pack-objects", "--foo", "x"}, err: errNonFlagArg},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			args, err := parsePackObjectsArgs(tc.args)
			require.Equal(t, tc.out, args)
			require.Equal(t, tc.err, err)
		})
	}
}
