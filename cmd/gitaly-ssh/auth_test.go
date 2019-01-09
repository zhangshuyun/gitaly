package main

import (
	"fmt"
	"net"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/golang/protobuf/jsonpb"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly-proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/internal/config"
	"gitlab.com/gitlab-org/gitaly/internal/rubyserver"
	"gitlab.com/gitlab-org/gitaly/internal/server"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"google.golang.org/grpc"
)

func buildGitalySSH(t *testing.T) {
	// Build the test-binary that we need
	os.Remove("gitaly-ssh")
	testhelper.MustRunCommand(nil, nil, "go", "build", "gitlab.com/gitlab-org/gitaly/cmd/gitaly-ssh")
}

func TestConnectivity(t *testing.T) {
	config.Config.TLS = config.TLS{
		CertPath: "testdata/certs/gitalycert.pem",
		KeyPath:  "testdata/gitalykey.pem",
	}

	cwd, err := os.Getwd()
	require.NoError(t, err)

	certPoolPath := path.Join(cwd, "testdata/certs")

	buildGitalySSH(t)
	testRepo := testhelper.TestRepository()

	gitalySSHPath := path.Join(cwd, "gitaly-ssh")

	socketPath := testhelper.GetTemporaryGitalySocketFileName()

	relativeSocketPath, err := filepath.Rel(cwd, socketPath)
	require.NoError(t, err, "relative path failure: %v. %s", err)

	tcpServer, tcpPort := runServer(t, server.NewInsecure, "tcp", "localhost:0")
	defer tcpServer.Stop()

	tlsServer, tlsPort := runServer(t, server.NewSecure, "tcp", "localhost:0")
	defer tlsServer.Stop()

	unixServer, _ := runServer(t, server.NewInsecure, "unix", socketPath)
	defer unixServer.Stop()

	testCases := []struct {
		name  string
		addr  string
		proxy bool
	}{
		{
			name: "tcp",
			addr: fmt.Sprintf("tcp://localhost:%d", tcpPort),
		},
		{
			name: "unix absolute",
			addr: fmt.Sprintf("unix:%s", socketPath),
		},
		{
			name:  "unix abs with proxy",
			addr:  fmt.Sprintf("unix:%s", socketPath),
			proxy: true,
		},
		{
			name: "unix relative",
			addr: fmt.Sprintf("unix:%s", relativeSocketPath),
		},
		{
			name:  "unix relative with proxy",
			addr:  fmt.Sprintf("unix:%s", relativeSocketPath),
			proxy: true,
		},

		{
			name: "tls",
			addr: fmt.Sprintf("tls://localhost:%d", tlsPort),
		},
	}

	pbMarshaler := &jsonpb.Marshaler{}
	payload, err := pbMarshaler.MarshalToString(&gitalypb.SSHUploadPackRequest{
		Repository: testRepo,
	})

	require.NoError(t, err)
	for _, testcase := range testCases {
		t.Run(testcase.name, func(t *testing.T) {
			cmd := exec.Command("git", "ls-remote", "git@localhost:test/test.git", "refs/heads/master")
			cmd.Stderr = os.Stderr
			cmd.Env = []string{
				fmt.Sprintf("GITALY_PAYLOAD=%s", payload),
				fmt.Sprintf("GITALY_ADDRESS=%s", testcase.addr),
				fmt.Sprintf("PATH=.:%s", os.Getenv("PATH")),
				fmt.Sprintf("GIT_SSH_COMMAND=%s upload-pack", gitalySSHPath),
				fmt.Sprintf("SSL_CERT_DIR=%s", certPoolPath),
			}

			if testcase.proxy {
				cmd.Env = append(cmd.Env,
					"http_proxy=http://invalid:1234",
					"https_proxy=https://invalid:1234",
				)
			}

			output, err := cmd.Output()

			require.NoError(t, err, "git ls-remote exit status")
			require.True(t, strings.HasSuffix(strings.TrimSpace(string(output)), "refs/heads/master"))
		})
	}
}

func runServer(t *testing.T, newServer func(rubyServer *rubyserver.Server) *grpc.Server, connectionType string, addr string) (*grpc.Server, int) {
	srv := newServer(nil)

	listener, err := net.Listen(connectionType, addr)
	require.NoError(t, err)

	go srv.Serve(listener)

	port := 0
	if connectionType != "unix" {
		addrSplit := strings.Split(listener.Addr().String(), ":")
		portString := addrSplit[len(addrSplit)-1]

		port, err = strconv.Atoi(portString)
		require.NoError(t, err)
	}

	return srv, port
}
