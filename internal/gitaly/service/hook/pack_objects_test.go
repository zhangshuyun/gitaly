package hook

import (
	"bytes"
	"io"
	"net"
	"strings"
	"sync"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/pktline"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
	hookPkg "gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/hook"
	"gitlab.com/gitlab-org/gitaly/v14/internal/streamcache"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
)

func cfgWithCache(t *testing.T) (config.Cfg, *gitalypb.Repository, string) {
	cfg, repo, repoPath := testcfg.BuildWithRepo(t)
	cfg.PackObjectsCache.Enabled = true
	cfg.PackObjectsCache.Dir = testhelper.TempDir(t)
	return cfg, repo, repoPath
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

func TestServer_PackObjectsHook_separateContext(t *testing.T) {
	cfg, repo, repoPath := cfgWithCache(t)

	req := &gitalypb.PackObjectsHookWithSidechannelRequest{
		Repository: repo,
		Args:       []string{"pack-objects", "--revs", "--thin", "--stdout", "--progress", "--delta-base-offset"},
	}
	const stdin = "3dd08961455abf80ef9115f4afdc1c6f968b503c\n--not\n\n"

	start1 := make(chan struct{})
	start2 := make(chan struct{})
	wg := &sync.WaitGroup{}
	wg.Add(2)
	serverSocketPath := runHooksServer(t, cfg, nil)

	// Call 1: sends a valid request but hangs up without reading response.
	// This should not break call 2.
	client1, conn1 := newHooksClient(t, serverSocketPath)
	defer conn1.Close()

	ctx1, cancel1 := testhelper.Context()
	defer cancel1()

	ctx1, wt1, err := hookPkg.SetupSidechannel(
		ctx1,
		func(c *net.UnixConn) error {
			defer close(start2)
			<-start1
			if _, err := io.WriteString(c, stdin); err != nil {
				return err
			}
			if err := c.CloseWrite(); err != nil {
				return err
			}

			// Read one byte of the response to ensure that this call got handled
			// before the next one.
			buf := make([]byte, 1)
			_, err := io.ReadFull(c, buf)
			return err
		},
	)
	require.NoError(t, err)
	defer wt1.Close()

	go func() {
		defer wg.Done()
		_, err := client1.PackObjectsHookWithSidechannel(ctx1, req)
		testhelper.RequireGrpcError(t, err, codes.Canceled)
		require.NoError(t, wt1.Wait())
	}()

	// Call 2: this is a normal call with the same request as call 1
	client2, conn2 := newHooksClient(t, serverSocketPath)
	defer conn2.Close()

	ctx2, cancel2 := testhelper.Context()
	defer cancel2()

	var stdout2 []byte
	ctx2, wt2, err := hookPkg.SetupSidechannel(
		ctx2,
		func(c *net.UnixConn) error {
			<-start2
			if _, err := io.WriteString(c, stdin); err != nil {
				return err
			}
			if err := c.CloseWrite(); err != nil {
				return err
			}

			return pktline.EachSidebandPacket(c, func(band byte, data []byte) error {
				if band == 1 {
					stdout2 = append(stdout2, data...)
				}
				return nil
			})
		},
	)
	require.NoError(t, err)
	defer wt2.Close()

	go func() {
		defer wg.Done()
		_, err := client2.PackObjectsHookWithSidechannel(ctx2, req)
		require.NoError(t, err)
		require.NoError(t, wt2.Wait())
	}()

	close(start1)
	wg.Wait()

	// Sanity check: second call received valid response
	gittest.ExecOpts(
		t,
		cfg,
		gittest.ExecConfig{Stdin: bytes.NewReader(stdout2)},
		"-C", repoPath, "index-pack", "--stdin", "--fix-thin",
	)
}

func TestServer_PackObjectsHook_usesCache(t *testing.T) {
	cfg, repo, repoPath := cfgWithCache(t)

	tlc := &streamcache.TestLoggingCache{}
	serverSocketPath := runHooksServer(t, cfg, []serverOption{func(s *server) {
		tlc.Cache = s.packObjectsCache
		s.packObjectsCache = tlc
	}})

	doRequest := func() {
		ctx, cancel := testhelper.Context()
		defer cancel()

		var stdout []byte
		ctx, wt, err := hookPkg.SetupSidechannel(
			ctx,
			func(c *net.UnixConn) error {
				if _, err := io.WriteString(c, "3dd08961455abf80ef9115f4afdc1c6f968b503c\n--not\n\n"); err != nil {
					return err
				}
				if err := c.CloseWrite(); err != nil {
					return err
				}

				return pktline.EachSidebandPacket(c, func(band byte, data []byte) error {
					if band == 1 {
						stdout = append(stdout, data...)
					}
					return nil
				})
			},
		)
		require.NoError(t, err)
		defer wt.Close()

		client, conn := newHooksClient(t, serverSocketPath)
		defer conn.Close()

		_, err = client.PackObjectsHookWithSidechannel(ctx, &gitalypb.PackObjectsHookWithSidechannelRequest{
			Repository: repo,
			Args:       []string{"pack-objects", "--revs", "--thin", "--stdout", "--progress", "--delta-base-offset"},
		})
		require.NoError(t, err)
		require.NoError(t, wt.Wait())

		gittest.ExecOpts(
			t,
			cfg,
			gittest.ExecConfig{Stdin: bytes.NewReader(stdout)},
			"-C", repoPath, "index-pack", "--stdin", "--fix-thin",
		)
	}

	const N = 5
	for i := 0; i < N; i++ {
		doRequest()
	}

	entries := tlc.Entries()
	require.Len(t, entries, N)
	first := entries[0]
	require.NotEmpty(t, first.Key)
	require.True(t, first.Created)
	require.NoError(t, first.Err)

	for i := 1; i < N; i++ {
		require.Equal(t, first.Key, entries[i].Key, "all requests had the same cache key")
		require.False(t, entries[i].Created, "all requests except the first were cache hits")
		require.NoError(t, entries[i].Err)
	}
}

func TestServer_PackObjectsHookWithSidechannel(t *testing.T) {
	t.Parallel()

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
			cfg, repo, repoPath := cfgWithCache(t)

			ctx, cancel := testhelper.Context()
			defer cancel()

			logger, hook := test.NewNullLogger()
			serverSocketPath := runHooksServer(t, cfg, nil, testserver.WithLogger(logger))

			var packets []string
			ctx, wt, err := hookPkg.SetupSidechannel(
				ctx,
				func(c *net.UnixConn) error {
					if _, err := io.WriteString(c, tc.stdin); err != nil {
						return err
					}
					if err := c.CloseWrite(); err != nil {
						return err
					}

					scanner := pktline.NewScanner(c)
					for scanner.Scan() {
						packets = append(packets, scanner.Text())
					}
					return scanner.Err()
				},
			)
			require.NoError(t, err)
			defer wt.Close()

			client, conn := newHooksClient(t, serverSocketPath)
			defer conn.Close()

			_, err = client.PackObjectsHookWithSidechannel(ctx, &gitalypb.PackObjectsHookWithSidechannelRequest{
				Repository: repo,
				Args:       tc.args,
			})
			require.NoError(t, err)

			require.NoError(t, wt.Wait())
			require.NotEmpty(t, packets)

			var packdata []byte
			for _, pkt := range packets {
				require.Greater(t, len(pkt), 4)

				switch band := pkt[4]; band {
				case 1:
					packdata = append(packdata, pkt[5:]...)
				case 2:
				default:
					t.Fatalf("unexpected band: %d", band)
				}
			}

			gittest.ExecOpts(
				t,
				cfg,
				gittest.ExecConfig{Stdin: bytes.NewReader(packdata)},
				"-C", repoPath, "index-pack", "--stdin", "--fix-thin",
			)

			for _, msg := range []string{"served bytes", "generated bytes"} {
				t.Run(msg, func(t *testing.T) {
					var entry *logrus.Entry
					for _, e := range hook.AllEntries() {
						if e.Message == msg {
							entry = e
						}
					}

					require.NotNil(t, entry)
					require.NotEmpty(t, entry.Data["cache_key"])
					require.Greater(t, entry.Data["bytes"], int64(0))
				})
			}

			t.Run("pack file compression statistic", func(t *testing.T) {
				var entry *logrus.Entry
				for _, e := range hook.AllEntries() {
					if e.Message == "pack file compression statistic" {
						entry = e
					}
				}

				require.NotNil(t, entry)
				total := entry.Data["pack.stat"].(string)
				require.True(t, strings.HasPrefix(total, "Total "))
				require.False(t, strings.Contains(total, "\n"))
			})
		})
	}
}

func TestServer_PackObjectsHookWithSidechannel_invalidArgument(t *testing.T) {
	cfg, repo, _ := testcfg.BuildWithRepo(t)
	serverSocketPath := runHooksServer(t, cfg, nil)

	ctx, cancel := testhelper.Context()
	defer cancel()

	testCases := []struct {
		desc string
		req  *gitalypb.PackObjectsHookWithSidechannelRequest
	}{
		{
			desc: "empty",
			req:  &gitalypb.PackObjectsHookWithSidechannelRequest{},
		},
		{
			desc: "repo, no args",
			req:  &gitalypb.PackObjectsHookWithSidechannelRequest{Repository: repo},
		},
		{
			desc: "repo, bad args",
			req:  &gitalypb.PackObjectsHookWithSidechannelRequest{Repository: repo, Args: []string{"rm", "-rf"}},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			client, conn := newHooksClient(t, serverSocketPath)
			defer conn.Close()

			_, err := client.PackObjectsHookWithSidechannel(ctx, tc.req)
			testhelper.RequireGrpcError(t, err, codes.InvalidArgument)
		})
	}
}

func TestServer_PackObjectsHookWithSidechannel_Canceled(t *testing.T) {
	cfg, repo, _ := cfgWithCache(t)
	ctx, cancel := testhelper.Context()
	defer cancel()

	ctx, wt, err := hookPkg.SetupSidechannel(
		ctx,
		func(c *net.UnixConn) error {
			// Simulate a client that successfully initiates a request, but hangs up
			// before fully consuming the response.
			_, err := io.WriteString(c, "3dd08961455abf80ef9115f4afdc1c6f968b503c\n--not\n\n")
			return err
		},
	)
	require.NoError(t, err)
	defer wt.Close()

	client, conn := newHooksClient(t, runHooksServer(t, cfg, nil))
	defer conn.Close()

	_, err = client.PackObjectsHookWithSidechannel(ctx, &gitalypb.PackObjectsHookWithSidechannelRequest{
		Repository: repo,
		Args:       []string{"pack-objects", "--revs", "--thin", "--stdout", "--progress", "--delta-base-offset"},
	})
	testhelper.RequireGrpcError(t, err, codes.Canceled)

	require.NoError(t, wt.Wait())
}
