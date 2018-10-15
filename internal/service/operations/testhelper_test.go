package operations

import (
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"path"
	"testing"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly-proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/internal/rubyserver"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

var (
	gitlabPreHooks  = []string{"pre-receive", "update"}
	gitlabPostHooks = []string{"post-receive"}
	GitlabPreHooks  = gitlabPreHooks
	GitlabHooks     []string
	RubyServer      *rubyserver.Server
	user            = &gitalypb.User{
		Name:       []byte("Jane Doe"),
		Email:      []byte("janedoe@gitlab.com"),
		GlId:       "user-123",
		GlUsername: "janedoe",
	}
)

func init() {
	copy(GitlabHooks, gitlabPreHooks)
	GitlabHooks = append(GitlabHooks, gitlabPostHooks...)
}

func TestMain(m *testing.M) {
	os.Exit(testMain(m))
}

func testMain(m *testing.M) int {
	defer testhelper.MustHaveNoChildProcess()

	var err error

	testhelper.ConfigureGitalySSH()

	testhelper.ConfigureRuby()
	RubyServer, err = rubyserver.Start()
	if err != nil {
		log.Fatal(err)
	}
	defer RubyServer.Stop()

	return m.Run()
}

func runOperationServiceServer(t *testing.T) (*grpc.Server, string) {
	grpcServer := testhelper.NewTestGrpcServer(t, nil, nil)
	serverSocketPath := testhelper.GetTemporaryGitalySocketFileName()

	listener, err := net.Listen("unix", serverSocketPath)
	if err != nil {
		t.Fatal(err)
	}

	gitalypb.RegisterOperationServiceServer(grpcServer, &server{RubyServer})
	reflection.Register(grpcServer)

	go grpcServer.Serve(listener)

	return grpcServer, serverSocketPath
}

func newOperationClient(t *testing.T, serverSocketPath string) (gitalypb.OperationServiceClient, *grpc.ClientConn) {
	connOpts := []grpc.DialOption{
		grpc.WithInsecure(),
		grpc.WithDialer(func(addr string, timeout time.Duration) (net.Conn, error) {
			return net.DialTimeout("unix", addr, timeout)
		}),
	}
	conn, err := grpc.Dial(serverSocketPath, connOpts...)
	if err != nil {
		t.Fatal(err)
	}

	return gitalypb.NewOperationServiceClient(conn), conn
}

var NewOperationClient = newOperationClient

func WriteEnvToHook(t *testing.T, repoPath, hookName string) (string, string) {
	hookOutputTemp, err := ioutil.TempFile("", "")
	require.NoError(t, err)
	require.NoError(t, hookOutputTemp.Close())

	defer os.Remove(hookOutputTemp.Name())

	hookContent := fmt.Sprintf("#!/bin/sh\n/usr/bin/env > %s\n", hookOutputTemp.Name())

	hookPath := path.Join(repoPath, "hooks", hookName)
	ioutil.WriteFile(hookPath, []byte(hookContent), 0755)

	return hookPath, hookOutputTemp.Name()
}
