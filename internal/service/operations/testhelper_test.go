package operations

import (
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"path"
	"testing"

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly-proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/internal/git/hooks"
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

	hookDir, err := ioutil.TempDir("", "gitaly-tmp-hooks")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(hookDir)

	hooks.Override = hookDir

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

	return grpcServer, "unix://" + serverSocketPath
}

func newOperationClient(t *testing.T, serverSocketPath string) (gitalypb.OperationServiceClient, *grpc.ClientConn) {
	connOpts := []grpc.DialOption{
		grpc.WithInsecure(),
	}
	conn, err := grpc.Dial(serverSocketPath, connOpts...)
	if err != nil {
		t.Fatal(err)
	}

	return gitalypb.NewOperationServiceClient(conn), conn
}

var NewOperationClient = newOperationClient

// The callee is responsible for clean up of the specific hook, testMain removes
// the hook dir
func WriteEnvToHook(t *testing.T, repoPath, hookName string) string {
	hookOutputTemp, err := ioutil.TempFile("", "")
	require.NoError(t, err)
	require.NoError(t, hookOutputTemp.Close())

	hookContent := fmt.Sprintf("#!/bin/sh\n/usr/bin/env > %s\n", hookOutputTemp.Name())

	_, err = OverrideHooks(hookName, []byte(hookContent))
	require.NoError(t, err)

	return hookOutputTemp.Name()
}

// When testing hooks, the content for one specific hook can be defined, to simulate
// behaviours on different hook content
func OverrideHooks(name string, content []byte) (func(), error) {
	fullPath := path.Join(hooks.Path(), name)

	return func() { os.Remove(fullPath) }, ioutil.WriteFile(fullPath, content, 0755)
}
