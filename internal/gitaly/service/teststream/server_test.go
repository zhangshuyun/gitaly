package teststream

import (
	"bytes"
	"io"
	"io/ioutil"
	"math/rand"
	"net"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v14/internal/streamrpc"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"google.golang.org/grpc"
)

func TestTestStreamPingPong(t *testing.T) {
	const size = 1024 * 1024

	addr, repo := runGitalyServer(t)

	ctx, cancel := testhelper.Context()
	defer cancel()

	in := make([]byte, size)
	_, err := rand.Read(in)
	require.NoError(t, err)

	var out []byte
	require.NotEqual(t, in, out)
	require.NoError(t, streamrpc.Call(
		ctx,
		streamrpc.DialNet(addr),
		"/gitaly.TestStreamService/TestStream",
		&gitalypb.TestStreamRequest{
			Repository: repo,
			Size:       size,
		},
		func(c net.Conn) error {
			errC := make(chan error, 1)
			go func() {
				var err error
				out, err = ioutil.ReadAll(c)
				errC <- err
			}()

			if _, err := io.Copy(c, bytes.NewReader(in)); err != nil {
				return err
			}
			if err := <-errC; err != nil {
				return err
			}

			return nil
		},
	))

	require.Equal(t, in, out, "byte stream works")
}

func TestTestStreamPingPongWithInvalidRepo(t *testing.T) {
	addr, repo := runGitalyServer(t)

	ctx, cancel := testhelper.Context()
	defer cancel()

	err := streamrpc.Call(
		ctx,
		streamrpc.DialNet(addr),
		"/gitaly.TestStreamService/TestStream",
		&gitalypb.TestStreamRequest{
			Repository: &gitalypb.Repository{
				StorageName:   repo.StorageName,
				RelativePath:  "@hashed/94/00/notexist.git",
				GlRepository:  repo.GlRepository,
				GlProjectPath: repo.GlProjectPath,
			},
			Size: 1024 * 1024,
		},
		func(c net.Conn) error {
			panic("Should not reach here")
		},
	)

	require.Error(t, err)
	require.Contains(
		t, err.Error(),
		"rpc error: code = NotFound desc = GetRepoPath: not a git repository",
	)
}

func runGitalyServer(t *testing.T) (string, *gitalypb.Repository) {
	t.Helper()
	testhelper.Configure()

	cfg, repo, _ := testcfg.BuildWithRepo(t)

	addr := testserver.RunGitalyServer(
		t, cfg, nil,
		func(srv grpc.ServiceRegistrar, deps *service.Dependencies) {
			gitalypb.RegisterTestStreamServiceServer(srv, NewServer(deps.GetLocator()))
		},
		// TODO: At the moment, stream RPC doesn't work well with Praefect,
		// hence we have to disable Praefect. We can remove this option after
		// https://gitlab.com/gitlab-com/gl-infra/scalability/-/issues/1127 is
		// done
		testserver.WithDisablePraefect(),
	)

	return addr, repo
}
