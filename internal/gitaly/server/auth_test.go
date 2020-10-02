package server

import (
	netctx "context"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	gitalyauth "gitlab.com/gitlab-org/gitaly/v13/auth"
	"gitlab.com/gitlab-org/gitaly/v13/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v13/internal/gitaly/config/auth"
	"gitlab.com/gitlab-org/gitaly/v13/internal/gitaly/rubyserver"
	"gitlab.com/gitlab-org/gitaly/v13/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v13/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/v13/streamio"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

func TestSanity(t *testing.T) {
	srv, serverSocketPath := runServer(t)
	defer srv.Stop()

	connOpts := []grpc.DialOption{
		grpc.WithInsecure(),
	}
	conn, err := dial(serverSocketPath, connOpts)
	require.NoError(t, err)
	defer conn.Close()

	require.NoError(t, healthCheck(conn))
}

func TestTLSSanity(t *testing.T) {
	srv, addr := runSecureServer(t)
	defer srv.Stop()

	certPool, err := x509.SystemCertPool()
	require.NoError(t, err)

	cert, err := ioutil.ReadFile("testdata/gitalycert.pem")
	require.NoError(t, err)

	ok := certPool.AppendCertsFromPEM(cert)
	require.True(t, ok)

	creds := credentials.NewClientTLSFromCert(certPool, "")
	connOpts := []grpc.DialOption{
		grpc.WithTransportCredentials(creds),
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
			srv, serverSocketPath := runServer(t)
			defer srv.Stop()

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

			srv, serverSocketPath := runServer(t)
			defer srv.Stop()

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

func runServerWithRuby(t *testing.T, ruby *rubyserver.Server) (*grpc.Server, string) {
	srv := NewInsecure(ruby, nil, config.Config)

	serverSocketPath := testhelper.GetTemporaryGitalySocketFileName()

	listener, err := net.Listen("unix", serverSocketPath)
	require.NoError(t, err)
	go srv.Serve(listener)

	return srv, "unix://" + serverSocketPath
}

func runServer(t *testing.T) (*grpc.Server, string) {
	return runServerWithRuby(t, nil)
}

func runSecureServer(t *testing.T) (*grpc.Server, string) {
	config.Config.TLS = config.TLS{
		CertPath: "testdata/gitalycert.pem",
		KeyPath:  "testdata/gitalykey.pem",
	}

	srv := NewSecure(nil, nil, config.Config)

	listener, err := net.Listen("tcp", "localhost:9999")
	require.NoError(t, err)

	go srv.Serve(listener)

	return srv, "localhost:9999"
}

func TestUnaryNoAuth(t *testing.T) {
	oldToken := config.Config.Auth.Token
	config.Config.Auth.Token = "testtoken"
	defer func() {
		config.Config.Auth.Token = oldToken
	}()

	srv, path := runServer(t)
	defer srv.Stop()

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

	srv, path := runServer(t)
	defer srv.Stop()

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

func TestMain(m *testing.M) {
	testhelper.Configure()
	os.Exit(testMain(m))
}

func testMain(m *testing.M) int {
	testhelper.ConfigureGitalyHooksBinary()

	return m.Run()
}

func TestAuthBeforeLimit(t *testing.T) {
	defer func(cfg config.Cfg) {
		config.Config = cfg
	}(config.Config)

	config.Config.Auth.Token = "abc123"

	testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	cwd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	gitlabShellDir := filepath.Join(cwd, "testdata", "gitlab-shell")
	os.RemoveAll(gitlabShellDir)

	if err := os.MkdirAll(gitlabShellDir, 0755); err != nil {
		t.Fatal(err)
	}

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
	config.ConfigureConcurrencyLimits()

	config.Config.Gitlab.URL = url
	var RubyServer rubyserver.Server
	if err := RubyServer.Start(); err != nil {
		t.Fatal(err)
	}
	server, serverSocketPath := runServerWithRuby(t, &RubyServer)
	defer server.Stop()

	client, conn := newOperationClient(t, serverSocketPath)
	defer conn.Close()

	ctx, cancel := testhelper.Context()
	defer cancel()

	targetRevision := "c7fbe50c7c7419d9701eebe64b1fdacc3df5b9dd"
	require.NoError(t, err)

	inputTagName := "to-be-créated-soon"

	request := &gitalypb.UserCreateTagRequest{
		Repository:     testRepo,
		TagName:        []byte(inputTagName),
		TargetRevision: []byte(targetRevision),
		User:           testhelper.TestUser,
		Message:        []byte("a new tag!"),
	}

	cleanupCustomHook, err := testhelper.WriteCustomHook(testRepoPath, "pre-receive", []byte(fmt.Sprintf(`#!/bin/bash
sleep %vs
`, gitalyauth.TimestampThreshold().Seconds())))

	require.NoError(t, err)
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
