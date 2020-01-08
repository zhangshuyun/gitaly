package operations

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/config"
	"gitlab.com/gitlab-org/gitaly/internal/rubyserver"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

var (
	gitlabPreHooks  = []string{"pre-receive", "update"}
	gitlabPostHooks = []string{"post-receive"}
	GitlabPreHooks  = gitlabPreHooks
	GitlabHooks     []string
	RubyServer      = &rubyserver.Server{}
	user            = &gitalypb.User{
		Name:       []byte("Jane Doe"),
		Email:      []byte("janedoe@gitlab.com"),
		GlId:       "user-123",
		GlUsername: "janedoe",
	}
)

func init() {
	GitlabHooks = append(GitlabHooks, append(gitlabPreHooks, gitlabPostHooks...)...)
}

func TestMain(m *testing.M) {
	os.Exit(testMain(m))
}

func testMain(m *testing.M) int {
	defer testhelper.MustHaveNoChildProcess()

	cwd, err := os.Getwd()
	if err != nil {
		log.Fatal(err)
	}
	gitlabShellDir := filepath.Join(cwd, "testdata", "gitlab-shell")
	os.RemoveAll(gitlabShellDir)

	if err := os.MkdirAll(gitlabShellDir, 0755); err != nil {
		log.Fatal(err)
	}

	config.Config.GitlabShell.Dir = filepath.Join(cwd, "testdata", "gitlab-shell")

	testhelper.ConfigureGitalySSH()
	testhelper.ConfigureGitalyHooksBinary()

	if err := RubyServer.Start(); err != nil {
		log.Fatal(err)
	}
	defer RubyServer.Stop()

	return m.Run()
}

func runOperationServiceServer(t *testing.T) (func(), string) {
	srv := testhelper.NewServer(t, nil, nil)

	gitalypb.RegisterOperationServiceServer(srv.GrpcServer(), &server{ruby: RubyServer})
	reflection.Register(srv.GrpcServer())

	require.NoError(t, srv.Start())

	return srv.Stop, "unix://" + srv.Socket()
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
func WriteEnvToCustomHook(t *testing.T, repoPath, hookName string) (string, func()) {
	hookOutputTemp, err := ioutil.TempFile("", "")
	require.NoError(t, err)
	require.NoError(t, hookOutputTemp.Close())

	hookContent := fmt.Sprintf("#!/bin/sh\n/usr/bin/env > %s\n", hookOutputTemp.Name())

	cleanupCustomHook, err := WriteCustomHook(repoPath, hookName, []byte(hookContent))
	require.NoError(t, err)

	return hookOutputTemp.Name(), func() {
		cleanupCustomHook()
		os.Remove(hookOutputTemp.Name())
	}
}

// write a hook in the repo/path.git/custom_hooks directory
func WriteCustomHook(repoPath, name string, content []byte) (func(), error) {
	fullPath := filepath.Join(repoPath, "custom_hooks", name)
	fullpathDir := filepath.Dir(fullPath)
	if err := os.MkdirAll(fullpathDir, 0755); err != nil {
		return func() {}, err
	}

	return func() {
		os.RemoveAll(fullpathDir)
	}, ioutil.WriteFile(fullPath, content, 0755)
}

func SetupAndStartGitlabServer(t *testing.T, glID, glRepository string, gitPushOptions ...string) func() {
	return testhelper.SetupAndStartGitlabServer(t, &testhelper.GitlabTestServerOptions{
		SecretToken:                 "secretToken",
		GLID:                        glID,
		GLRepository:                glRepository,
		PostReceiveCounterDecreased: true,
		Protocol:                    "web",
		GitPushOptions:              gitPushOptions,
	})
}
