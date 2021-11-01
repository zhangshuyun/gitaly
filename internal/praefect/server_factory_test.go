package praefect

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"net"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/client"
	"gitlab.com/gitlab-org/gitaly/v14/internal/backchannel"
	"gitlab.com/gitlab-org/gitaly/v14/internal/bootstrap/starter"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gittest"
	gconfig "gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/service/setup"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/v14/internal/listenmux"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/datastore"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/datastore/glsql"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/nodes"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/protoregistry"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/service/transaction"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/transactions"
	"gitlab.com/gitlab-org/gitaly/v14/internal/sidechannel"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/promtest"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

func TestServerFactory(t *testing.T) {
	t.Parallel()
	cfg, repo, repoPath := testcfg.BuildWithRepo(t)
	gitalyAddr := testserver.RunGitalyServer(t, cfg, nil, setup.RegisterAll, testserver.WithDisablePraefect())

	certFile, keyFile := testhelper.GenerateCerts(t)

	conf := config.Config{
		TLS: gconfig.TLS{
			CertPath: certFile,
			KeyPath:  keyFile,
		},
		VirtualStorages: []*config.VirtualStorage{
			{
				Name: "praefect",
				Nodes: []*config.Node{
					{
						Storage: cfg.Storages[0].Name,
						Address: gitalyAddr,
						Token:   cfg.Auth.Token,
					},
				},
			},
		},
		Failover: config.Failover{Enabled: true},
	}

	repo.StorageName = conf.VirtualStorages[0].Name // storage must be re-written to virtual to be properly redirected by praefect
	revision := text.ChompBytes(gittest.Exec(t, cfg, "-C", repoPath, "rev-parse", "HEAD"))

	logger := testhelper.DiscardTestEntry(t)
	queue := datastore.NewPostgresReplicationEventQueue(glsql.NewDB(t))

	rs := datastore.MockRepositoryStore{}
	txMgr := transactions.NewManager(conf)
	sidechannelRegistry := sidechannel.NewRegistry()
	clientHandshaker := backchannel.NewClientHandshaker(logger, NewBackchannelServerFactory(logger, transaction.NewServer(txMgr), sidechannelRegistry))
	nodeMgr, err := nodes.NewManager(logger, conf, nil, rs, &promtest.MockHistogramVec{}, protoregistry.GitalyProtoPreregistered, nil, clientHandshaker, sidechannelRegistry)
	require.NoError(t, err)
	nodeMgr.Start(0, time.Second)
	defer nodeMgr.Stop()
	registry := protoregistry.GitalyProtoPreregistered

	coordinator := NewCoordinator(
		queue,
		rs,
		NewNodeManagerRouter(nodeMgr, rs),
		txMgr,
		conf,
		registry,
	)

	checkOwnRegisteredServices := func(ctx context.Context, t *testing.T, cc *grpc.ClientConn) healthpb.HealthClient {
		t.Helper()

		healthClient := healthpb.NewHealthClient(cc)
		resp, err := healthClient.Check(ctx, &healthpb.HealthCheckRequest{})
		require.NoError(t, err)
		require.Equal(t, healthpb.HealthCheckResponse_SERVING, resp.Status)
		return healthClient
	}

	checkProxyingOntoGitaly := func(ctx context.Context, t *testing.T, cc *grpc.ClientConn) {
		t.Helper()

		commitClient := gitalypb.NewCommitServiceClient(cc)
		resp, err := commitClient.FindCommit(ctx, &gitalypb.FindCommitRequest{
			Repository: repo,
			Revision:   []byte(revision),
		})
		require.NoError(t, err)
		require.Equal(t, revision, resp.Commit.Id)
	}

	checkSidechannelGitaly := func(ctx context.Context, t *testing.T, addr string, creds credentials.TransportCredentials) {
		t.Helper()

		// Client has its own sidechannel registry, don't reuse the one we plugged into Praefect.
		registry := sidechannel.NewRegistry()

		factory := func() backchannel.Server {
			lm := listenmux.New(insecure.NewCredentials())
			lm.Register(sidechannel.NewServerHandshaker(registry))
			return grpc.NewServer(grpc.Creds(lm))
		}

		clientHandshaker := backchannel.NewClientHandshaker(logger, factory)
		dialOpt := grpc.WithTransportCredentials(clientHandshaker.ClientHandshake(creds))

		cc, err := grpc.Dial(addr, dialOpt)
		require.NoError(t, err)
		defer func() { require.NoError(t, cc.Close()) }()

		var pack []byte
		ctx, waiter := sidechannel.RegisterSidechannel(ctx, registry, func(conn *sidechannel.ClientConn) error {
			// 1e292f8fedd741b75372e19097c76d327140c312 is refs/heads/master of the test repo
			const message = `003cwant 1e292f8fedd741b75372e19097c76d327140c312 ofs-delta
00000009done
`

			if _, err := io.WriteString(conn, message); err != nil {
				return err
			}
			if err := conn.CloseWrite(); err != nil {
				return err
			}

			buf := make([]byte, 8)
			if _, err := io.ReadFull(conn, buf); err != nil {
				return fmt.Errorf("read nak: %w", err)
			}
			if string(buf) != "0008NAK\n" {
				return fmt.Errorf("unexpected response: %q", buf)
			}

			var err error
			pack, err = io.ReadAll(conn)
			if err != nil {
				return err
			}

			return nil
		})
		defer waiter.Close()

		_, err = gitalypb.NewSmartHTTPServiceClient(cc).PostUploadPackWithSidechannel(ctx,
			&gitalypb.PostUploadPackWithSidechannelRequest{Repository: repo},
		)
		require.NoError(t, err)
		require.NoError(t, waiter.Close())

		gittest.ExecOpts(t, cfg, gittest.ExecConfig{Stdin: bytes.NewReader(pack)},
			"-C", repoPath, "index-pack", "--stdin", "--fix-thin",
		)
	}

	t.Run("insecure", func(t *testing.T) {
		praefectServerFactory := NewServerFactory(conf, logger, coordinator.StreamDirector, nodeMgr, txMgr, queue, rs, datastore.AssignmentStore{}, registry, nil, nil)
		defer praefectServerFactory.Stop()

		listener, err := net.Listen(starter.TCP, "localhost:0")
		require.NoError(t, err)
		defer func() { require.NoError(t, listener.Close()) }()

		go praefectServerFactory.Serve(listener, false)

		praefectAddr, err := starter.ComposeEndpoint(listener.Addr().Network(), listener.Addr().String())
		require.NoError(t, err)

		creds := insecure.NewCredentials()

		cc, err := client.Dial(praefectAddr, nil)
		require.NoError(t, err)
		defer func() { require.NoError(t, cc.Close()) }()

		ctx, cancel := testhelper.Context()
		defer cancel()

		t.Run("handles registered RPCs", func(t *testing.T) {
			checkOwnRegisteredServices(ctx, t, cc)
		})

		t.Run("proxies RPCs onto gitaly server", func(t *testing.T) {
			checkProxyingOntoGitaly(ctx, t, cc)
		})

		t.Run("proxies sidechannel RPCs onto gitaly server", func(t *testing.T) {
			checkSidechannelGitaly(ctx, t, listener.Addr().String(), creds)
		})
	})

	t.Run("secure", func(t *testing.T) {
		praefectServerFactory := NewServerFactory(conf, logger, coordinator.StreamDirector, nodeMgr, txMgr, queue, rs, datastore.AssignmentStore{}, registry, nil, nil)
		defer praefectServerFactory.Stop()

		listener, err := net.Listen(starter.TCP, "localhost:0")
		require.NoError(t, err)
		defer func() { require.NoError(t, listener.Close()) }()

		go praefectServerFactory.Serve(listener, true)

		ctx, cancel := testhelper.Context()
		defer cancel()

		certPool, err := x509.SystemCertPool()
		require.NoError(t, err)

		pem := testhelper.MustReadFile(t, conf.TLS.CertPath)

		require.True(t, certPool.AppendCertsFromPEM(pem))

		creds := credentials.NewTLS(&tls.Config{
			RootCAs:    certPool,
			MinVersion: tls.VersionTLS12,
		})

		cc, err := grpc.DialContext(ctx, listener.Addr().String(), grpc.WithTransportCredentials(creds))
		require.NoError(t, err)
		defer func() { require.NoError(t, cc.Close()) }()

		t.Run("handles registered RPCs", func(t *testing.T) {
			checkOwnRegisteredServices(ctx, t, cc)
		})

		t.Run("proxies RPCs onto gitaly server", func(t *testing.T) {
			checkProxyingOntoGitaly(ctx, t, cc)
		})

		t.Run("proxies sidechannel RPCs onto gitaly server", func(t *testing.T) {
			checkSidechannelGitaly(ctx, t, listener.Addr().String(), creds)
		})
	})

	t.Run("stops all listening servers", func(t *testing.T) {
		ctx, cancel := testhelper.Context()
		defer cancel()

		praefectServerFactory := NewServerFactory(conf, logger, coordinator.StreamDirector, nodeMgr, txMgr, queue, rs, datastore.AssignmentStore{}, registry, nil, nil)
		defer praefectServerFactory.Stop()

		// start with tcp address
		tcpListener, err := net.Listen(starter.TCP, "localhost:0")
		require.NoError(t, err)
		defer tcpListener.Close()

		go praefectServerFactory.Serve(tcpListener, false)

		praefectTCPAddr, err := starter.ComposeEndpoint(tcpListener.Addr().Network(), tcpListener.Addr().String())
		require.NoError(t, err)

		tcpCC, err := client.Dial(praefectTCPAddr, nil)
		require.NoError(t, err)
		defer func() { require.NoError(t, tcpCC.Close()) }()

		tcpHealthClient := checkOwnRegisteredServices(ctx, t, tcpCC)

		// start with tls address
		tlsListener, err := net.Listen(starter.TCP, "localhost:0")
		require.NoError(t, err)
		defer tlsListener.Close()

		go praefectServerFactory.Serve(tlsListener, true)

		praefectTLSAddr, err := starter.ComposeEndpoint(tcpListener.Addr().Network(), tcpListener.Addr().String())
		require.NoError(t, err)

		tlsCC, err := client.Dial(praefectTLSAddr, nil)
		require.NoError(t, err)
		defer func() { require.NoError(t, tlsCC.Close()) }()

		tlsHealthClient := checkOwnRegisteredServices(ctx, t, tlsCC)

		// start with socket address
		socketPath := testhelper.GetTemporaryGitalySocketFileName(t)
		defer func() { require.NoError(t, os.RemoveAll(socketPath)) }()
		socketListener, err := net.Listen(starter.Unix, socketPath)
		require.NoError(t, err)
		defer socketListener.Close()

		go praefectServerFactory.Serve(socketListener, false)

		praefectSocketAddr, err := starter.ComposeEndpoint(socketListener.Addr().Network(), socketListener.Addr().String())
		require.NoError(t, err)

		socketCC, err := client.Dial(praefectSocketAddr, nil)
		require.NoError(t, err)
		defer func() { require.NoError(t, socketCC.Close()) }()

		unixHealthClient := checkOwnRegisteredServices(ctx, t, socketCC)

		praefectServerFactory.GracefulStop()

		_, err = tcpHealthClient.Check(ctx, nil)
		require.Error(t, err)

		_, err = tlsHealthClient.Check(ctx, nil)
		require.Error(t, err)

		_, err = unixHealthClient.Check(ctx, nil)
		require.Error(t, err)
	})

	t.Run("tls key path invalid", func(t *testing.T) {
		badTLSKeyPath := conf
		badTLSKeyPath.TLS.KeyPath = "invalid"
		praefectServerFactory := NewServerFactory(badTLSKeyPath, logger, coordinator.StreamDirector, nodeMgr, txMgr, queue, rs, datastore.AssignmentStore{}, registry, nil, nil)

		err := praefectServerFactory.Serve(nil, true)
		require.EqualError(t, err, "load certificate key pair: open invalid: no such file or directory")
	})

	t.Run("tls cert path invalid", func(t *testing.T) {
		badTLSKeyPath := conf
		badTLSKeyPath.TLS.CertPath = "invalid"
		praefectServerFactory := NewServerFactory(badTLSKeyPath, logger, coordinator.StreamDirector, nodeMgr, txMgr, queue, rs, datastore.AssignmentStore{}, registry, nil, nil)

		err := praefectServerFactory.Serve(nil, true)
		require.EqualError(t, err, "load certificate key pair: open invalid: no such file or directory")
	})
}
