package commit

import (
	"os"
	"testing"

	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func TestMain(m *testing.M) {
	os.Exit(testMain(m))
}

func testMain(m *testing.M) int {
	defer testhelper.MustHaveNoChildProcess()

	return m.Run()
}

func startTestServices(t testing.TB) (func(), string) {
	srv := testhelper.NewServer(t, nil, nil)

	gitalypb.RegisterCommitServiceServer(srv.GrpcServer(), NewServer())
	reflection.Register(srv.GrpcServer())

	require.NoError(t, srv.Start())

	return srv.Stop, "unix://" + srv.Socket()
}

func newCommitServiceClient(t testing.TB, serviceSocketPath string) (gitalypb.CommitServiceClient, *grpc.ClientConn) {
	connOpts := []grpc.DialOption{
		grpc.WithInsecure(),
	}
	conn, err := grpc.Dial(serviceSocketPath, connOpts...)
	if err != nil {
		t.Fatal(err)
	}

	return gitalypb.NewCommitServiceClient(conn), conn
}

func dummyCommitAuthor(ts int64) *gitalypb.CommitAuthor {
	return &gitalypb.CommitAuthor{
		Name:     []byte("Ahmad Sherif"),
		Email:    []byte("ahmad+gitlab-test@gitlab.com"),
		Date:     &timestamp.Timestamp{Seconds: ts},
		Timezone: []byte("+0200"),
	}
}
