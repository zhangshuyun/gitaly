package server

import (
	netctx "context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	gitalyauth "gitlab.com/gitlab-org/gitaly/auth"
	"gitlab.com/gitlab-org/gitaly/client"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config/auth"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/hook"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/rubyserver"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/streamio"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

func TestMain(m *testing.M) {
	os.Exit(testMain(m))
}

func testMain(m *testing.M) int {
	defer testhelper.MustHaveNoChildProcess()
	cleanup := testhelper.Configure()
	defer cleanup()
	testhelper.ConfigureGitalyHooksBinary()
	return m.Run()
}

func TestSanity(t *testing.T) {
	serverSocketPath, clean := runServer(t)
	defer clean()

	connOpts := []grpc.DialOption{
		grpc.WithInsecure(),
	}
	conn, err := dial(serverSocketPath, connOpts)
	require.NoError(t, err)
	defer conn.Close()

	require.NoError(t, healthCheck(conn))
}

func TestTLSSanity(t *testing.T) {
	addr, clean := runSecureServer(t)
	defer clean()

	certPool, err := x509.SystemCertPool()
	require.NoError(t, err)

	cert, err := ioutil.ReadFile("testdata/gitalycert.pem")
	require.NoError(t, err)

	ok := certPool.AppendCertsFromPEM(cert)
	require.True(t, ok)

	connOpts := []grpc.DialOption{
		grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{
			RootCAs:    certPool,
			MinVersion: tls.VersionTLS12,
		})),
	}

	conn, err := grpc.Dial(addr, connOpts...)
	require.NoError(t, err)
	defer conn.Close()

	require.NoError(t, healthCheck(conn))
}

func TestAuthFailures(t *testing.T) {
	defer func(oldAuth auth.Config) {
		config.Config.Auth = oldAuth
	}(config.Config.Auth)
	config.Config.Auth.Token = "quxbaz"

	testCases := []struct {
		desc string
		opts []grpc.DialOption
		code codes.Code
	}{
		{desc: "no auth", opts: nil, code: codes.Unauthenticated},
		{
			desc: "invalid auth",
			opts: []grpc.DialOption{grpc.WithPerRPCCredentials(brokenAuth{})},
			code: codes.Unauthenticated,
		},
		{
			desc: "wrong secret",
			opts: []grpc.DialOption{grpc.WithPerRPCCredentials(gitalyauth.RPCCredentialsV2("foobar"))},
			code: codes.PermissionDenied,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			serverSocketPath, clean := runServer(t)
			defer clean()

			connOpts := append(tc.opts, grpc.WithInsecure())
			conn, err := dial(serverSocketPath, connOpts)
			require.NoError(t, err, tc.desc)
			defer conn.Close()
			testhelper.RequireGrpcError(t, healthCheck(conn), tc.code)
		})
	}
}

func TestAuthSuccess(t *testing.T) {
	defer func(oldAuth auth.Config) {
		config.Config.Auth = oldAuth
	}(config.Config.Auth)

	token := "foobar"

	testCases := []struct {
		desc     string
		opts     []grpc.DialOption
		required bool
		token    string
	}{
		{desc: "no auth, not required"},
		{
			desc:  "v2 correct auth, not required",
			opts:  []grpc.DialOption{grpc.WithPerRPCCredentials(gitalyauth.RPCCredentialsV2(token))},
			token: token,
		},
		{
			desc:  "v2 incorrect auth, not required",
			opts:  []grpc.DialOption{grpc.WithPerRPCCredentials(gitalyauth.RPCCredentialsV2("incorrect"))},
			token: token,
		},
		{
			desc:     "v2 correct auth, required",
			opts:     []grpc.DialOption{grpc.WithPerRPCCredentials(gitalyauth.RPCCredentialsV2(token))},
			token:    token,
			required: true,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			config.Config.Auth.Token = tc.token
			config.Config.Auth.Transitioning = !tc.required

			serverSocketPath, clean := runServer(t)
			defer clean()

			connOpts := append(tc.opts, grpc.WithInsecure())
			conn, err := dial(serverSocketPath, connOpts)
			require.NoError(t, err, tc.desc)
			defer conn.Close()
			assert.NoError(t, healthCheck(conn), tc.desc)
		})
	}
}

type brokenAuth struct{}

func (brokenAuth) RequireTransportSecurity() bool { return false }
func (brokenAuth) GetRequestMetadata(netctx.Context, ...string) (map[string]string, error) {
	return map[string]string{"authorization": "Bearer blablabla"}, nil
}

func dial(serverSocketPath string, opts []grpc.DialOption) (*grpc.ClientConn, error) {
	return grpc.Dial(serverSocketPath, opts...)
}

func healthCheck(conn *grpc.ClientConn) error {
	ctx, cancel := testhelper.Context()
	defer cancel()

	client := healthpb.NewHealthClient(conn)
	_, err := client.Check(ctx, &healthpb.HealthCheckRequest{})
	return err
}

func newOperationClient(t *testing.T, serverSocketPath string) (gitalypb.OperationServiceClient, *grpc.ClientConn) {
	connOpts := []grpc.DialOption{
		grpc.WithInsecure(),
		grpc.WithPerRPCCredentials(gitalyauth.RPCCredentialsV2(config.Config.Auth.Token)),
	}
	conn, err := grpc.Dial(serverSocketPath, connOpts...)
	if err != nil {
		t.Fatal(err)
	}

	return gitalypb.NewOperationServiceClient(conn), conn
}

func runServerWithRuby(t *testing.T, ruby *rubyserver.Server) (string, func()) {
	conns := client.NewPool()
	cfg := config.Config
	locator := config.NewLocator(cfg)
	txManager := transaction.NewManager(cfg)
	hookManager := hook.NewManager(locator, txManager, hook.GitlabAPIStub, cfg)
	gitCmdFactory := git.NewExecCommandFactory(cfg)
	srv, err := New(false, ruby, hookManager, txManager, cfg, conns, locator, gitCmdFactory)
	require.NoError(t, err)

	serverSocketPath := testhelper.GetTemporaryGitalySocketFileName(t)

	listener, err := net.Listen("unix", serverSocketPath)
	require.NoError(t, err)
	go srv.Serve(listener)

	return "unix://" + serverSocketPath, func() {
		conns.Close()
		srv.Stop()
		ruby.Stop()
	}
}

func runServer(t *testing.T) (string, func()) {
	return runServerWithRuby(t, nil)
}

//go:generate openssl req -newkey rsa:4096 -new -nodes -x509 -days 3650 -out testdata/gitalycert.pem -keyout testdata/gitalykey.pem -subj "/C=US/ST=California/L=San Francisco/O=GitLab/OU=GitLab-Shell/CN=localhost" -addext "subjectAltName = IP:127.0.0.1, DNS:localhost"
func runSecureServer(t *testing.T) (string, func()) {
	t.Helper()

	config.Config.TLS = config.TLS{
		CertPath: "testdata/gitalycert.pem",
		KeyPath:  "testdata/gitalykey.pem",
	}

	conns := client.NewPool()
	srv, err := New(true, nil, nil, nil, config.Config, conns, nil, nil)
	require.NoError(t, err)

	listener, hostPort := testhelper.GetLocalhostListener(t)
	go srv.Serve(listener)

	return hostPort, func() {
		conns.Close()
		srv.Stop()
	}
}

func TestUnaryNoAuth(t *testing.T) {
	oldToken := config.Config.Auth.Token
	config.Config.Auth.Token = "testtoken"
	defer func() {
		config.Config.Auth.Token = oldToken
	}()

	path, clean := runServer(t)
	defer clean()

	connOpts := []grpc.DialOption{
		grpc.WithInsecure(),
	}

	conn, err := grpc.Dial(path, connOpts...)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := testhelper.Context()
	defer cancel()

	client := gitalypb.NewRepositoryServiceClient(conn)
	_, err = client.CreateRepository(ctx, &gitalypb.CreateRepositoryRequest{Repository: &gitalypb.Repository{StorageName: "default", RelativePath: "new/project/path"}})

	testhelper.RequireGrpcError(t, err, codes.Unauthenticated)
}

func TestStreamingNoAuth(t *testing.T) {
	oldToken := config.Config.Auth.Token
	config.Config.Auth.Token = "testtoken"
	defer func() {
		config.Config.Auth.Token = oldToken
	}()

	path, clean := runServer(t)
	defer clean()

	connOpts := []grpc.DialOption{
		grpc.WithInsecure(),
	}

	conn, err := grpc.Dial(path, connOpts...)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := testhelper.Context()
	defer cancel()

	client := gitalypb.NewRepositoryServiceClient(conn)
	stream, err := client.GetInfoAttributes(ctx, &gitalypb.GetInfoAttributesRequest{Repository: &gitalypb.Repository{StorageName: "default", RelativePath: "new/project/path"}})

	require.NoError(t, err)

	_, err = ioutil.ReadAll(streamio.NewReader(func() ([]byte, error) {
		_, err = stream.Recv()
		return nil, err
	}))

	testhelper.RequireGrpcError(t, err, codes.Unauthenticated)
}

func TestAuthBeforeLimit(t *testing.T) {
	defer func(cfg config.Cfg) {
		config.Config = cfg
	}(config.Config)

	config.Config.Auth.Token = "abc123"

	testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	gitlabShellDir, cleanup := testhelper.TempDir(t)
	defer cleanup()
	config.Config.GitlabShell.Dir = gitlabShellDir

	url, cleanup := testhelper.SetupAndStartGitlabServer(t, &testhelper.GitlabTestServerOptions{
		SecretToken:                 "secretToken",
		GLID:                        testhelper.GlID,
		GLRepository:                testRepo.GlRepository,
		PostReceiveCounterDecreased: true,
		Protocol:                    "web",
	})
	defer cleanup()

	config.Config.Concurrency = []config.Concurrency{{
		RPC:        "/gitaly.OperationService/UserCreateTag",
		MaxPerRepo: 1,
	}}
	config.ConfigureConcurrencyLimits(config.Config)

	config.Config.Gitlab.URL = url
	var RubyServer rubyserver.Server
	if err := RubyServer.Start(); err != nil {
		t.Fatal(err)
	}
	serverSocketPath, clean := runServerWithRuby(t, &RubyServer)
	defer clean()

	client, conn := newOperationClient(t, serverSocketPath)
	defer conn.Close()

	ctx, cancel := testhelper.Context()
	defer cancel()

	targetRevision := "c7fbe50c7c7419d9701eebe64b1fdacc3df5b9dd"
	inputTagName := "to-be-créated-soon"

	request := &gitalypb.UserCreateTagRequest{
		Repository:     testRepo,
		TagName:        []byte(inputTagName),
		TargetRevision: []byte(targetRevision),
		User:           testhelper.TestUser,
		Message:        []byte("a new tag!"),
	}

	defer func(d time.Duration) {
		gitalyauth.SetTokenValidityDuration(d)
	}(gitalyauth.TokenValidityDuration())
	gitalyauth.SetTokenValidityDuration(5 * time.Second)

	cleanupCustomHook := testhelper.WriteCustomHook(t, testRepoPath, "pre-receive", []byte(fmt.Sprintf(`#!/bin/bash
sleep %vs
`, gitalyauth.TokenValidityDuration().Seconds())))
	defer cleanupCustomHook()

	errChan := make(chan error)

	for i := 0; i < 2; i++ {
		go func() {
			_, err := client.UserCreateTag(ctx, request)
			errChan <- err
		}()
	}

	timer := time.NewTimer(1 * time.Minute)

	for i := 0; i < 2; i++ {
		select {
		case <-timer.C:
			require.Fail(t, "time limit reached waiting for calls to finish")
		case err := <-errChan:
			require.NoError(t, err)
		}
	}
}
