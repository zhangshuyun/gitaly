package main

import (
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/golang/protobuf/jsonpb"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/client"
	"gitlab.com/gitlab-org/gitaly/internal/backchannel"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/hook"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/server"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/service/setup"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/internal/gitlab"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

//go:generate openssl req -newkey rsa:4096 -new -nodes -x509 -days 3650 -out testdata/certs/gitalycert.pem -keyout testdata/gitalykey.pem -subj "/C=US/ST=California/L=San Francisco/O=GitLab/OU=GitLab-Shell/CN=localhost" -addext "subjectAltName = IP:127.0.0.1, DNS:localhost"
func TestConnectivity(t *testing.T) {
	cfg, repo, _ := testcfg.BuildWithRepo(t)

	testhelper.ConfigureGitalySSHBin(t, cfg)
	testhelper.ConfigureGitalyHooksBin(t, cfg)

	cwd, err := os.Getwd()
	require.NoError(t, err)

	certPoolPath := filepath.Join(cwd, "testdata", "certs")

	tempDir := testhelper.TempDir(t)

	relativeSocketPath, err := filepath.Rel(cwd, filepath.Join(tempDir, "gitaly.socket"))
	require.NoError(t, err)

	require.NoError(t, os.RemoveAll(relativeSocketPath))
	require.NoError(t, os.Symlink(cfg.SocketPath, relativeSocketPath))

	runGitaly := func(t testing.TB, cfg config.Cfg) string {
		t.Helper()
		return testserver.RunGitalyServer(t, cfg, nil, setup.RegisterAll, testserver.WithDisablePraefect())
	}

	testCases := []struct {
		name  string
		addr  func(t *testing.T, cfg config.Cfg) string
		proxy bool
	}{
		{
			name: "tcp",
			addr: func(t *testing.T, cfg config.Cfg) string {
				cfg.ListenAddr = "localhost:0"
				return runGitaly(t, cfg)
			},
		},
		{
			name: "unix absolute",
			addr: func(t *testing.T, cfg config.Cfg) string {
				return runGitaly(t, cfg)
			},
		},
		{
			name: "unix abs with proxy",
			addr: func(t *testing.T, cfg config.Cfg) string {
				return runGitaly(t, cfg)
			},
			proxy: true,
		},
		{
			name: "unix relative",
			addr: func(t *testing.T, cfg config.Cfg) string {
				cfg.SocketPath = fmt.Sprintf("unix:%s", relativeSocketPath)
				return runGitaly(t, cfg)
			},
		},
		{
			name: "unix relative with proxy",
			addr: func(t *testing.T, cfg config.Cfg) string {
				cfg.SocketPath = fmt.Sprintf("unix:%s", relativeSocketPath)
				return runGitaly(t, cfg)
			},
			proxy: true,
		},
		{
			name: "tls",
			addr: func(t *testing.T, cfg config.Cfg) string {
				cfg.TLSListenAddr = "localhost:0"
				cfg.TLS = config.TLS{
					// regenerate the test cert and key via `go generate`
					CertPath: "testdata/certs/gitalycert.pem",
					KeyPath:  "testdata/gitalykey.pem",
				}
				return runGitaly(t, cfg)
			},
		},
	}

	pbMarshaler := &jsonpb.Marshaler{}
	payload, err := pbMarshaler.MarshalToString(&gitalypb.SSHUploadPackRequest{
		Repository: repo,
	})

	require.NoError(t, err)
	for _, testcase := range testCases {
		t.Run(testcase.name, func(t *testing.T) {
			addr := testcase.addr(t, cfg)

			cmd := exec.Command(cfg.Git.BinPath, "ls-remote", "git@localhost:test/test.git", "refs/heads/master")
			cmd.Stderr = os.Stderr
			cmd.Env = []string{
				fmt.Sprintf("GITALY_PAYLOAD=%s", payload),
				fmt.Sprintf("GITALY_ADDRESS=%s", addr),
				fmt.Sprintf("GITALY_WD=%s", cwd),
				fmt.Sprintf("PATH=.:%s", os.Getenv("PATH")),
				fmt.Sprintf("GIT_SSH_COMMAND=%s upload-pack", filepath.Join(cfg.BinDir, "gitaly-ssh")),
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

func runServer(t *testing.T, secure bool, cfg config.Cfg, connectionType string, addr string) (int, func()) {
	conns := client.NewPool()
	locator := config.NewLocator(cfg)
	registry := backchannel.NewRegistry()
	txManager := transaction.NewManager(cfg, registry)
	hookManager := hook.NewManager(locator, txManager, gitlab.NewMockClient(), cfg)
	gitCmdFactory := git.NewExecCommandFactory(cfg)
	srv, err := server.New(secure, cfg, testhelper.DiscardTestEntry(t), registry)
	require.NoError(t, err)
	setup.RegisterAll(srv, &service.Dependencies{
		Cfg:                cfg,
		GitalyHookManager:  hookManager,
		TransactionManager: txManager,
		StorageLocator:     locator,
		ClientPool:         conns,
		GitCmdFactory:      gitCmdFactory,
	})

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

	return port, func() {
		conns.Close()
		srv.Stop()
	}
}
