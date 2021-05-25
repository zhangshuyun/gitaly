package chunk

import (
	"io"
	"net"
	"os"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/wrappers"
	"github.com/stretchr/testify/require"
	test "gitlab.com/gitlab-org/gitaly/v14/internal/helper/chunk/pb"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
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

type testSender struct {
	stream test.Test_StreamOutputServer
	output [][]byte
}

func (ts *testSender) Reset() { ts.output = nil }
func (ts *testSender) Append(m proto.Message) {
	ts.output = append(ts.output, m.(*wrappers.BytesValue).Value)
}

func (ts *testSender) Send() error {
	return ts.stream.Send(&test.StreamOutputResponse{
		Msg: ts.output,
	})
}

func TestChunker(t *testing.T) {
	s := &server{}
	srv, serverSocketPath := runServer(t, s)
	defer srv.Stop()

	client, conn := newClient(t, serverSocketPath)
	defer conn.Close()

	ctx, cancel := testhelper.Context()
	defer cancel()

	stream, err := client.StreamOutput(ctx, &test.StreamOutputRequest{BytesToReturn: 3.5 * maxMessageSize})
	require.NoError(t, err)

	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		require.Less(t, proto.Size(resp), maxMessageSize)
	}
}

type server struct{}

func (s *server) StreamOutput(req *test.StreamOutputRequest, srv test.Test_StreamOutputServer) error {
	const kilobyte = 1024

	c := New(&testSender{stream: srv})
	for numBytes := 0; numBytes < int(req.GetBytesToReturn()); numBytes += kilobyte {
		if err := c.Send(&wrappers.BytesValue{Value: make([]byte, kilobyte)}); err != nil {
			return err
		}
	}

	if err := c.Flush(); err != nil {
		return err
	}
	return nil
}

func runServer(t *testing.T, s *server, opt ...grpc.ServerOption) (*grpc.Server, string) {
	serverSocketPath := testhelper.GetTemporaryGitalySocketFileName(t)
	grpcServer := grpc.NewServer(opt...)
	test.RegisterTestServer(grpcServer, s)

	lis, err := net.Listen("unix", serverSocketPath)
	require.NoError(t, err)

	go grpcServer.Serve(lis)

	return grpcServer, "unix://" + serverSocketPath
}

func newClient(t *testing.T, serverSocketPath string) (test.TestClient, *grpc.ClientConn) {
	connOpts := []grpc.DialOption{
		grpc.WithInsecure(),
	}
	conn, err := grpc.Dial(serverSocketPath, connOpts...)
	if err != nil {
		t.Fatal(err)
	}

	return test.NewTestClient(conn), conn
}
