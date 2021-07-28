package internalgitaly

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"google.golang.org/grpc"
)

func TestMain(m *testing.M) {
	os.Exit(testMain(m))
}

func testMain(m *testing.M) int {
	defer testhelper.MustHaveNoChildProcess()
	cleanup := testhelper.Configure()
	defer cleanup()
	return m.Run()
}

func setupInternalGitalyService(t *testing.T, cfg config.Cfg, internalService gitalypb.InternalGitalyServer) gitalypb.InternalGitalyClient {
	add := testserver.RunGitalyServer(t, cfg, nil, func(srv grpc.ServiceRegistrar, deps *service.Dependencies) {
		gitalypb.RegisterInternalGitalyServer(srv, internalService)
	}, testserver.WithDisablePraefect())
	conn, err := grpc.Dial(add, grpc.WithInsecure())
	require.NoError(t, err)
	t.Cleanup(func() { testhelper.MustClose(t, conn) })

	return gitalypb.NewInternalGitalyClient(conn)
}
