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
	gitalyauth "gitlab.com/gitlab-org/gitaly/v14/auth"
	"gitlab.com/gitlab-org/gitaly/v14/client"
	"gitlab.com/gitlab-org/gitaly/v14/internal/backchannel"
	"gitlab.com/gitlab-org/gitaly/v14/internal/cache"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config/auth"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/hook"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/service/setup"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitlab"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/v14/streamio"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
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

func TestSanity(t *testing.T) {
	serverSocketPath := runServer(t, config.Cfg{})

	conn, err := dial(serverSocketPath, []grpc.DialOption{grpc.WithInsecure()})
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })

	require.NoError(t, healthCheck(conn))
}

func TestTLSSanity(t *testing.T) {
	cfg := testcfg.Build(t)
	addr := runSecureServer(t, cfg)

	certPool, err := x509.SystemCertPool()
	require.NoError(t, err)

	cert := testhelper.MustReadFile(t, "testdata/gitalycert.pem")
	ok := certPool.AppendCertsFromPEM(cert)
	require.True(t, ok)

	connOpts := []grpc.DialOption{
		grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{
			RootCAs:    certPool,
			MinVersion: tls.VersionTLS12,
		})),
	}

	conn, err := dial(addr, connOpts)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })

	require.NoError(t, healthCheck(conn))
}

func TestAuthFailures(t *testing.T) {
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
			serverSocketPath := runServer(t, config.Cfg{Auth: auth.Config{Token: "quxbaz"}})
			connOpts := append(tc.opts, grpc.WithInsecure())
			conn, err := dial(serverSocketPath, connOpts)
			require.NoError(t, err, tc.desc)
			t.Cleanup(func() { conn.Close() })
			testhelper.RequireGrpcError(t, healthCheck(conn), tc.code)
		})
	}
}

func TestAuthSuccess(t *testing.T) {
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
			cfg := testcfg.Build(t, testcfg.WithBase(config.Cfg{
				Auth: auth.Config{Token: tc.token, Transitioning: !tc.required},
			}))

			serverSocketPath := runServer(t, cfg)
			connOpts := append(tc.opts, grpc.WithInsecure())
			conn, err := dial(serverSocketPath, connOpts)
			require.NoError(t, err, tc.desc)
			t.Cleanup(func() { conn.Close() })
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

	_, err := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{})
	return err
}

func newOperationClient(t *testing.T, token, serverSocketPath string) (gitalypb.OperationServiceClient, *grpc.ClientConn) {
	t.Helper()

	connOpts := []grpc.DialOption{
		grpc.WithInsecure(),
		grpc.WithPerRPCCredentials(gitalyauth.RPCCredentialsV2(token)),
	}
	conn, err := grpc.Dial(serverSocketPath, connOpts...)
	require.NoError(t, err)

	return gitalypb.NewOperationServiceClient(conn), conn
}

func runServer(t *testing.T, cfg config.Cfg) string {
	t.Helper()

	registry := backchannel.NewRegistry()
	conns := client.NewPool()
	t.Cleanup(func() { conns.Close() })
	locator := config.NewLocator(cfg)
	txManager := transaction.NewManager(cfg, registry)
	hookManager := hook.NewManager(locator, txManager, gitlab.NewMockClient(), cfg)
	gitCmdFactory := git.NewExecCommandFactory(cfg)
	catfileCache := catfile.NewCache(cfg)
	diskCache := cache.New(cfg, locator)

	srv, err := New(false, cfg, testhelper.DiscardTestEntry(t), registry, diskCache)
	require.NoError(t, err)

	setup.RegisterAll(srv, &service.Dependencies{
		Cfg:                cfg,
		GitalyHookManager:  hookManager,
		TransactionManager: txManager,
		StorageLocator:     locator,
		ClientPool:         conns,
		GitCmdFactory:      gitCmdFactory,
		CatfileCache:       catfileCache,
	})
	serverSocketPath := testhelper.GetTemporaryGitalySocketFileName(t)

	listener, err := net.Listen("unix", serverSocketPath)
	require.NoError(t, err)
	t.Cleanup(srv.Stop)
	go srv.Serve(listener)

	return "unix://" + serverSocketPath
}

//go:generate openssl req -newkey rsa:4096 -new -nodes -x509 -days 3650 -out testdata/gitalycert.pem -keyout testdata/gitalykey.pem -subj "/C=US/ST=California/L=San Francisco/O=GitLab/OU=GitLab-Shell/CN=localhost" -addext "subjectAltName = IP:127.0.0.1, DNS:localhost"
func runSecureServer(t *testing.T, cfg config.Cfg) string {
	t.Helper()

	cfg.TLS = config.TLS{
		CertPath: "testdata/gitalycert.pem",
		KeyPath:  "testdata/gitalykey.pem",
	}

	conns := client.NewPool()
	t.Cleanup(func() { conns.Close() })

	srv, err := New(true, cfg, testhelper.DiscardTestEntry(t), backchannel.NewRegistry(), cache.New(cfg, config.NewLocator(cfg)))
	require.NoError(t, err)

	healthpb.RegisterHealthServer(srv, health.NewServer())

	listener, hostPort := testhelper.GetLocalhostListener(t)
	t.Cleanup(srv.Stop)
	go srv.Serve(listener)

	return hostPort
}

func TestUnaryNoAuth(t *testing.T) {
	cfg := testcfg.Build(t, testcfg.WithBase(config.Cfg{Auth: auth.Config{Token: "testtoken"}}))
	path := runServer(t, cfg)
	conn, err := grpc.Dial(path, grpc.WithInsecure())
	require.NoError(t, err)

	ctx, cancel := testhelper.Context()
	defer cancel()

	client := gitalypb.NewRepositoryServiceClient(conn)
	_, err = client.CreateRepository(ctx, &gitalypb.CreateRepositoryRequest{
		Repository: &gitalypb.Repository{
			StorageName:  cfg.Storages[0].Name,
			RelativePath: "new/project/path",
		}},
	)

	testhelper.RequireGrpcError(t, err, codes.Unauthenticated)
}

func TestStreamingNoAuth(t *testing.T) {
	cfg := testcfg.Build(t, testcfg.WithBase(config.Cfg{Auth: auth.Config{Token: "testtoken"}}))

	path := runServer(t, cfg)
	conn, err := dial(path, []grpc.DialOption{grpc.WithInsecure()})
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })

	ctx, cancel := testhelper.Context()
	defer cancel()

	client := gitalypb.NewRepositoryServiceClient(conn)
	stream, err := client.GetInfoAttributes(ctx, &gitalypb.GetInfoAttributesRequest{
		Repository: &gitalypb.Repository{
			StorageName:  cfg.Storages[0].Name,
			RelativePath: "new/project/path",
		}},
	)
	require.NoError(t, err)

	_, err = ioutil.ReadAll(streamio.NewReader(func() ([]byte, error) {
		_, err = stream.Recv()
		return nil, err
	}))
	testhelper.RequireGrpcError(t, err, codes.Unauthenticated)
}

func TestAuthBeforeLimit(t *testing.T) {
	cfg, repo, repoPath := testcfg.BuildWithRepo(t, testcfg.WithBase(config.Cfg{
		Auth: auth.Config{Token: "abc123"},
		Concurrency: []config.Concurrency{{
			RPC:        "/gitaly.OperationService/UserCreateTag",
			MaxPerRepo: 1,
		}}},
	))

	config.ConfigureConcurrencyLimits(cfg)

	gitlabURL, cleanup := testhelper.SetupAndStartGitlabServer(t, cfg.GitlabShell.Dir, &testhelper.GitlabTestServerOptions{
		SecretToken:                 "secretToken",
		GLID:                        gittest.GlID,
		GLRepository:                repo.GetGlRepository(),
		PostReceiveCounterDecreased: true,
		Protocol:                    "web",
	})
	t.Cleanup(cleanup)
	cfg.Gitlab.URL = gitlabURL

	serverSocketPath := runServer(t, cfg)
	client, conn := newOperationClient(t, cfg.Auth.Token, serverSocketPath)
	t.Cleanup(func() { conn.Close() })

	ctx, cancel := testhelper.Context()
	defer cancel()

	targetRevision := "c7fbe50c7c7419d9701eebe64b1fdacc3df5b9dd"
	inputTagName := "to-be-créated-soon"

	request := &gitalypb.UserCreateTagRequest{
		Repository:     repo,
		TagName:        []byte(inputTagName),
		TargetRevision: []byte(targetRevision),
		User:           gittest.TestUser,
		Message:        []byte("a new tag!"),
	}

	defer func(d time.Duration) {
		gitalyauth.SetTokenValidityDuration(d)
	}(gitalyauth.TokenValidityDuration())
	gitalyauth.SetTokenValidityDuration(5 * time.Second)

	gittest.WriteCustomHook(t, repoPath, "pre-receive", []byte(fmt.Sprintf(`#!/bin/bash
sleep %vs
`, gitalyauth.TokenValidityDuration().Seconds())))

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
