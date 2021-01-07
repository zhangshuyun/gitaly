package praefect

import (
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/nodes"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

func TestDialNodes(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	tmp, err := ioutil.TempDir("", "praefect")
	require.NoError(t, err)
	defer os.RemoveAll(tmp)

	type nodeAssertion struct {
		storage string
		token   string
		status  grpc_health_v1.HealthCheckResponse_ServingStatus
		error   error
	}

	expectedNodes := []nodeAssertion{
		{
			storage: "healthy",
			token:   "healthy-token",
			status:  grpc_health_v1.HealthCheckResponse_SERVING,
		},
		{
			storage: "unhealthy",
			token:   "unhealthy-token",
			status:  grpc_health_v1.HealthCheckResponse_NOT_SERVING,
		},
	}

	var cfgNodes []*config.Node
	for _, n := range expectedNodes {
		socket := filepath.Join(tmp, n.storage)
		ln, err := net.Listen("unix", socket)
		require.NoError(t, err)
		healthSrv := health.NewServer()
		healthSrv.SetServingStatus("", n.status)
		srv := grpc.NewServer()
		grpc_health_v1.RegisterHealthServer(srv, healthSrv)
		defer srv.Stop()
		go srv.Serve(ln)

		cfgNodes = append(cfgNodes, &config.Node{
			Storage: n.storage,
			Token:   n.token,
			Address: fmt.Sprintf("%s://%s", ln.Addr().Network(), ln.Addr().String()),
		})
	}

	expectedNodes = append(expectedNodes, nodeAssertion{
		storage: "invalid",
		error:   status.Error(codes.Unavailable, `connection error: desc = "transport: Error while dialing dial unix non-existent-socket: connect: no such file or directory"`),
	})

	nodeSet, err := DialNodes(ctx,
		[]*config.VirtualStorage{{
			Name: "virtual-storage",
			Nodes: append(cfgNodes, &config.Node{
				Storage: "invalid",
				Address: "unix:non-existent-socket",
			}),
		}}, nil, nil,
	)
	require.NoError(t, err)
	defer nodeSet.Close()

	conns := nodeSet.Connections()
	healthClients := nodeSet.HealthClients()

	var actualNodes []nodeAssertion
	for virtualStorage, nodes := range nodeSet {
		for _, node := range nodes {
			require.NotNil(t, conns[virtualStorage][node.Storage], "connection not found for storage %q", node.Storage)
			resp, err := healthClients[virtualStorage][node.Storage].Check(ctx, &grpc_health_v1.HealthCheckRequest{})

			assertion := nodeAssertion{
				storage: node.Storage,
				token:   node.Token,
				error:   err,
			}

			if resp != nil {
				assertion.status = resp.Status
			}

			actualNodes = append(actualNodes, assertion)

			delete(conns[virtualStorage], node.Storage)
			delete(healthClients[virtualStorage], node.Storage)
		}
	}

	require.ElementsMatch(t, expectedNodes, actualNodes)
	require.Equal(t, Connections{"virtual-storage": {}}, conns, "unexpected connections")
	require.Equal(t, nodes.HealthClients{"virtual-storage": {}}, healthClients, "unexpected health clients")
}
