package testhelper

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_logrus "github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus"
	grpc_ctxtags "github.com/grpc-ecosystem/go-grpc-middleware/tags"
	"github.com/pelletier/go-toml"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	gitalyauth "gitlab.com/gitlab-org/gitaly/auth"
	"gitlab.com/gitlab-org/gitaly/internal/backchannel"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config/auth"
	gitalylog "gitlab.com/gitlab-org/gitaly/internal/gitaly/config/log"
	serverauth "gitlab.com/gitlab-org/gitaly/internal/gitaly/server/auth"
	"gitlab.com/gitlab-org/gitaly/internal/helper/fieldextractors"
	gitalyinternallog "gitlab.com/gitlab-org/gitaly/internal/log"
	praefectconfig "gitlab.com/gitlab-org/gitaly/internal/praefect/config"
	grpccorrelation "gitlab.com/gitlab-org/labkit/correlation/grpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

// TestServerOpt is an option for TestServer
type TestServerOpt func(t *TestServer)

// WithToken is a TestServerOpt that provides a security token
func WithToken(token string) TestServerOpt {
	return func(t *TestServer) {
		t.token = token
	}
}

// WithStorages is a TestServerOpt that sets the storages for a TestServer
func WithStorages(storages []string) TestServerOpt {
	return func(t *TestServer) {
		t.storages = storages
	}
}

// WithInternalSocket is a TestServerOpt that will cause the TestServer to
// listen on its internal socket.
func WithInternalSocket(cfg config.Cfg) TestServerOpt {
	return func(t *TestServer) {
		t.withInternalSocketPath = cfg.GitalyInternalSocketPath()
	}
}

// NewTestServer instantiates a new TestServer
func NewTestServer(srv *grpc.Server, opts ...TestServerOpt) *TestServer {
	ts := &TestServer{
		grpcServer: srv,
		storages:   []string{"default"},
	}

	for _, opt := range opts {
		opt(ts)
	}

	// the health service needs to be registered in order to support health checks on all
	// gitaly services that are under test.
	// The health check is executed by the praefect in case 'test-with-praefect' verification
	// job is running.
	healthpb.RegisterHealthServer(srv, health.NewServer())

	return ts
}

// NewServerWithAuth creates a new test server with authentication
func NewServerWithAuth(tb testing.TB, streamInterceptors []grpc.StreamServerInterceptor, unaryInterceptors []grpc.UnaryServerInterceptor, token string, registry *backchannel.Registry, opts ...TestServerOpt) *TestServer {
	if token != "" {
		opts = append(opts, WithToken(token))
		streamInterceptors = append(streamInterceptors, serverauth.StreamServerInterceptor(auth.Config{Token: token}))
		unaryInterceptors = append(unaryInterceptors, serverauth.UnaryServerInterceptor(auth.Config{Token: token}))
	}

	return newServerWithLogger(
		tb,
		NewTestLogger(tb),
		streamInterceptors,
		unaryInterceptors,
		registry,
		opts...,
	)
}

// TestServer wraps a grpc Server and handles automatically putting a praefect in front of a gitaly instance
// if necessary
type TestServer struct {
	grpcServer             *grpc.Server
	socket                 string
	process                *os.Process
	token                  string
	storages               []string
	waitCh                 chan struct{}
	withInternalSocketPath string
}

// GrpcServer returns the underlying grpc.Server
func (p *TestServer) GrpcServer() *grpc.Server {
	return p.grpcServer
}

// Stop will stop both the grpc server as well as the praefect process
func (p *TestServer) Stop() {
	p.grpcServer.Stop()
	if p.process != nil {
		p.process.Kill()
		<-p.waitCh
	}
}

// Socket returns the socket file the test server is listening on
func (p *TestServer) Socket() string {
	return p.socket
}

// Start will start the grpc server as well as spawn a praefect instance if GITALY_TEST_PRAEFECT_BIN is enabled
func (p *TestServer) Start(t testing.TB) {
	praefectBinPath, ok := os.LookupEnv("GITALY_TEST_PRAEFECT_BIN")
	if !ok {
		p.socket = p.listen(t)
		return
	}

	tempDir := TempDir(t)

	praefectServerSocketPath := GetTemporaryGitalySocketFileName(t)

	c := praefectconfig.Config{
		SocketPath: praefectServerSocketPath,
		Auth: auth.Config{
			Token: p.token,
		},
		MemoryQueueEnabled: true,
		Failover: praefectconfig.Failover{
			Enabled:           true,
			ElectionStrategy:  praefectconfig.ElectionStrategyLocal,
			BootstrapInterval: config.Duration(time.Microsecond),
			MonitorInterval:   config.Duration(time.Second),
		},
		Replication: praefectconfig.DefaultReplicationConfig(),
		Logging: gitalylog.Config{
			Format: "json",
			Level:  "panic",
		},
	}

	for _, storage := range p.storages {
		gitalyServerSocketPath := p.listen(t)

		c.VirtualStorages = append(c.VirtualStorages, &praefectconfig.VirtualStorage{
			Name: storage,
			Nodes: []*praefectconfig.Node{
				{
					Storage: storage,
					Address: "unix:/" + gitalyServerSocketPath,
					Token:   p.token,
				},
			},
		})
	}

	configFilePath := filepath.Join(tempDir, "config.toml")
	configFile, err := os.Create(configFilePath)
	require.NoError(t, err)
	defer MustClose(t, configFile)

	require.NoError(t, toml.NewEncoder(configFile).Encode(&c))
	require.NoError(t, configFile.Sync())

	cmd := exec.Command(praefectBinPath, "-config", configFilePath)
	cmd.Stderr = os.Stderr
	cmd.Stdout = os.Stdout

	p.socket = praefectServerSocketPath

	require.NoError(t, cmd.Start())

	p.waitCh = make(chan struct{})
	go func() {
		cmd.Wait()
		close(p.waitCh)
	}()

	opts := []grpc.DialOption{grpc.WithInsecure()}
	if p.token != "" {
		opts = append(opts, grpc.WithPerRPCCredentials(gitalyauth.RPCCredentialsV2(p.token)))
	}

	conn, err := grpc.Dial("unix://"+praefectServerSocketPath, opts...)
	require.NoError(t, err)
	defer MustClose(t, conn)

	waitHealthy(t, conn, 3, time.Second)

	p.process = cmd.Process
}

func (p *TestServer) listen(t testing.TB) string {
	gitalyServerSocketPath := GetTemporaryGitalySocketFileName(t)

	sockets := []string{
		gitalyServerSocketPath,
	}

	if p.withInternalSocketPath != "" {
		sockets = append(sockets, p.withInternalSocketPath)
	}

	for _, socket := range sockets {
		listener, err := net.Listen("unix", socket)
		require.NoError(t, err)

		go p.grpcServer.Serve(listener)

		opts := []grpc.DialOption{grpc.WithInsecure()}
		if p.token != "" {
			opts = append(opts, grpc.WithPerRPCCredentials(gitalyauth.RPCCredentialsV2(p.token)))
		}

		conn, err := grpc.Dial("unix://"+socket, opts...)
		require.NoError(t, err)
		defer MustClose(t, conn)

		waitHealthy(t, conn, 3, time.Second)
	}

	return gitalyServerSocketPath
}

// waitHealthy executes health check request `retries` times and awaits each `timeout` period to respond.
// After `retries` unsuccessful attempts it returns an error.
// Returns immediately without an error once get a successful health check response.
func waitHealthy(t testing.TB, conn *grpc.ClientConn, retries int, timeout time.Duration) {
	for i := 0; i < retries; i++ {
		if IsHealthy(conn, timeout) {
			return
		}
	}

	require.FailNow(t, "server not yet ready to serve")
}

// IsHealthy creates a health client to passed in connection and send `Check` request.
// It waits for `timeout` duration to get response back.
// It returns `true` only if remote responds with `SERVING` status.
func IsHealthy(conn *grpc.ClientConn, timeout time.Duration) bool {
	healthClient := healthpb.NewHealthClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	resp, err := healthClient.Check(ctx, &healthpb.HealthCheckRequest{}, grpc.WaitForReady(true))
	if err != nil {
		return false
	}

	if resp.Status != healthpb.HealthCheckResponse_SERVING {
		return false
	}

	return true
}

// NewServer creates a Server for testing purposes
func NewServer(tb testing.TB, streamInterceptors []grpc.StreamServerInterceptor, unaryInterceptors []grpc.UnaryServerInterceptor, opts ...TestServerOpt) *TestServer {
	logger := NewTestLogger(tb)

	return NewServerWithLogger(tb, logger, streamInterceptors, unaryInterceptors, opts...)
}

// NewServerWithLogger lets you inject a logger into a test server. You
// can use this to inspect log messages.
func NewServerWithLogger(tb testing.TB, logger *log.Logger, streamInterceptors []grpc.StreamServerInterceptor, unaryInterceptors []grpc.UnaryServerInterceptor, opts ...TestServerOpt) *TestServer {
	return newServerWithLogger(tb, logger, streamInterceptors, unaryInterceptors, backchannel.NewRegistry(), opts...)
}

func newServerWithLogger(tb testing.TB, logger *log.Logger, streamInterceptors []grpc.StreamServerInterceptor, unaryInterceptors []grpc.UnaryServerInterceptor, registry *backchannel.Registry, opts ...TestServerOpt) *TestServer {
	logrusEntry := log.NewEntry(logger).WithField("test", tb.Name())
	ctxTagger := grpc_ctxtags.WithFieldExtractorForInitialReq(fieldextractors.FieldExtractor)

	streamInterceptors = append([]grpc.StreamServerInterceptor{
		grpc_ctxtags.StreamServerInterceptor(ctxTagger),
		grpccorrelation.StreamServerCorrelationInterceptor(),
		grpc_logrus.StreamServerInterceptor(logrusEntry,
			grpc_logrus.WithTimestampFormat(gitalyinternallog.LogTimestampFormat)),
	}, streamInterceptors...)

	unaryInterceptors = append([]grpc.UnaryServerInterceptor{
		grpc_ctxtags.UnaryServerInterceptor(ctxTagger),
		grpccorrelation.UnaryServerCorrelationInterceptor(),
		grpc_logrus.UnaryServerInterceptor(logrusEntry,
			grpc_logrus.WithTimestampFormat(gitalyinternallog.LogTimestampFormat)),
	}, unaryInterceptors...)

	return NewTestServer(
		grpc.NewServer(
			grpc.Creds(backchannel.NewServerHandshaker(logrusEntry, backchannel.Insecure(), registry, nil)),
			grpc.StreamInterceptor(grpc_middleware.ChainStreamServer(streamInterceptors...)),
			grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(unaryInterceptors...)),
		),
		opts...,
	)
}

// NewTestGrpcServer creates a GRPC Server for testing purposes
func NewTestGrpcServer(tb testing.TB, streamInterceptors []grpc.StreamServerInterceptor, unaryInterceptors []grpc.UnaryServerInterceptor) *grpc.Server {
	logger := NewTestLogger(tb)
	logrusEntry := log.NewEntry(logger).WithField("test", tb.Name())

	ctxTagger := grpc_ctxtags.WithFieldExtractorForInitialReq(fieldextractors.FieldExtractor)
	ctxStreamTagger := grpc_ctxtags.StreamServerInterceptor(ctxTagger)
	ctxUnaryTagger := grpc_ctxtags.UnaryServerInterceptor(ctxTagger)

	streamInterceptors = append([]grpc.StreamServerInterceptor{ctxStreamTagger, grpc_logrus.StreamServerInterceptor(logrusEntry)}, streamInterceptors...)
	unaryInterceptors = append([]grpc.UnaryServerInterceptor{ctxUnaryTagger, grpc_logrus.UnaryServerInterceptor(logrusEntry)}, unaryInterceptors...)

	return grpc.NewServer(
		grpc.StreamInterceptor(grpc_middleware.ChainStreamServer(streamInterceptors...)),
		grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(unaryInterceptors...)),
	)
}

var changeLineRegex = regexp.MustCompile("^[a-f0-9]{40} [a-f0-9]{40} refs/[^ ]+$")

const secretHeaderName = "Gitlab-Shared-Secret"

func preReceiveFormToMap(u url.Values) map[string]string {
	return map[string]string{
		"action":        u.Get("action"),
		"gl_repository": u.Get("gl_repository"),
		"project":       u.Get("project"),
		"changes":       u.Get("changes"),
		"protocol":      u.Get("protocol"),
		"env":           u.Get("env"),
		"username":      u.Get("username"),
		"key_id":        u.Get("key_id"),
		"user_id":       u.Get("user_id"),
	}
}

type postReceiveForm struct {
	GLRepository   string   `json:"gl_repository"`
	SecretToken    string   `json:"secret_token"`
	Changes        string   `json:"changes"`
	Identifier     string   `json:"identifier"`
	GitPushOptions []string `json:"push_options"`
}

func parsePostReceiveForm(u url.Values) postReceiveForm {
	return postReceiveForm{
		GLRepository:   u.Get("gl_repository"),
		SecretToken:    u.Get("secret_token"),
		Changes:        u.Get("changes"),
		Identifier:     u.Get("identifier"),
		GitPushOptions: u["push_options[]"],
	}
}

func handleAllowed(t testing.TB, options GitlabTestServerOptions) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			http.Error(w, "could not parse form", http.StatusBadRequest)
			return
		}

		if r.Method != http.MethodPost {
			http.Error(w, "method not POST", http.StatusMethodNotAllowed)
			return
		}

		params := make(map[string]string)

		switch r.Header.Get("Content-Type") {
		case "application/x-www-form-urlencoded":
			params = preReceiveFormToMap(r.Form)
		case "application/json":
			if err := json.NewDecoder(r.Body).Decode(&params); err != nil {
				http.Error(w, "could not unmarshal json body", http.StatusBadRequest)
				return
			}
		}

		user, password, _ := r.BasicAuth()
		if user != options.User || password != options.Password {
			http.Error(w, "user or password incorrect", http.StatusUnauthorized)
			return
		}

		if options.GLID != "" {
			glidSplit := strings.SplitN(options.GLID, "-", 2)
			if len(glidSplit) != 2 {
				http.Error(w, "gl_id invalid", http.StatusUnauthorized)
				return
			}

			glKey, glVal := glidSplit[0], glidSplit[1]

			var glIDMatches bool
			switch glKey {
			case "user":
				glIDMatches = glVal == params["user_id"]
			case "key":
				glIDMatches = glVal == params["key_id"]
			case "username":
				glIDMatches = glVal == params["username"]
			default:
				http.Error(w, "gl_id invalid", http.StatusUnauthorized)
				return
			}

			if !glIDMatches {
				http.Error(w, "gl_id invalid", http.StatusUnauthorized)
				return
			}
		}

		if params["gl_repository"] == "" {
			http.Error(w, "gl_repository is empty", http.StatusUnauthorized)
			return
		}

		if options.GLRepository != "" {
			if params["gl_repository"] != options.GLRepository {
				http.Error(w, "gl_repository is invalid", http.StatusUnauthorized)
				return
			}
		}

		if params["protocol"] == "" {
			http.Error(w, "protocol is empty", http.StatusUnauthorized)
			return
		}

		if options.Protocol != "" {
			if params["protocol"] != options.Protocol {
				http.Error(w, "protocol is invalid", http.StatusUnauthorized)
				return
			}
		}

		if options.Changes != "" {
			if params["changes"] != options.Changes {
				http.Error(w, "changes is invalid", http.StatusUnauthorized)
				return
			}
		} else {
			changeLines := strings.Split(strings.TrimSuffix(params["changes"], "\n"), "\n")
			for _, line := range changeLines {
				if !changeLineRegex.MatchString(line) {
					http.Error(w, "changes is invalid", http.StatusUnauthorized)
					return
				}
			}
		}

		env := params["env"]
		if len(env) == 0 {
			http.Error(w, "env is empty", http.StatusUnauthorized)
			return
		}

		var gitVars struct {
			GitAlternateObjectDirsRel []string `json:"GIT_ALTERNATE_OBJECT_DIRECTORIES_RELATIVE"`
			GitObjectDirRel           string   `json:"GIT_OBJECT_DIRECTORY_RELATIVE"`
		}

		w.Header().Set("Content-Type", "application/json")

		if err := json.Unmarshal([]byte(env), &gitVars); err != nil {
			http.Error(w, "could not unmarshal env", http.StatusUnauthorized)
			return
		}

		if options.GitObjectDir != "" {
			relObjectDir, err := filepath.Rel(options.RepoPath, options.GitObjectDir)
			if err != nil {
				http.Error(w, "git object dirs is invalid", http.StatusUnauthorized)
				return
			}
			if relObjectDir != gitVars.GitObjectDirRel {
				_, err := w.Write([]byte(`{"status":false}`))
				require.NoError(t, err)
				return
			}
		}

		if len(options.GitAlternateObjectDirs) > 0 {
			if len(gitVars.GitAlternateObjectDirsRel) != len(options.GitAlternateObjectDirs) {
				http.Error(w, "git alternate object dirs is invalid", http.StatusUnauthorized)
				return
			}

			for i, gitAlterateObjectDir := range options.GitAlternateObjectDirs {
				relAltObjectDir, err := filepath.Rel(options.RepoPath, gitAlterateObjectDir)
				if err != nil {
					http.Error(w, "git alternate object dirs is invalid", http.StatusUnauthorized)
					return
				}

				if relAltObjectDir != gitVars.GitAlternateObjectDirsRel[i] {
					_, err := w.Write([]byte(`{"status":false}`))
					require.NoError(t, err)
					return
				}
			}
		}

		var authenticated bool
		if r.Form.Get("secret_token") == options.SecretToken {
			authenticated = true
		}

		secretHeader, err := base64.StdEncoding.DecodeString(r.Header.Get(secretHeaderName))
		if err == nil {
			if string(secretHeader) == options.SecretToken {
				authenticated = true
			}
		}

		if authenticated {
			_, err := w.Write([]byte(`{"status":true}`))
			require.NoError(t, err)
			return
		}
		w.WriteHeader(http.StatusUnauthorized)

		_, err = w.Write([]byte(`{"message":"401 Unauthorized\n"}`))
		require.NoError(t, err)
	}
}

func handlePreReceive(t testing.TB, options GitlabTestServerOptions) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			http.Error(w, "could not parse form", http.StatusBadRequest)
			return
		}

		params := make(map[string]string)

		switch r.Header.Get("Content-Type") {
		case "application/x-www-form-urlencoded":
			b, err := json.Marshal(r.Form)
			if err != nil {
				http.Error(w, "could not marshal form", http.StatusBadRequest)
				return
			}

			var reqForm struct {
				GLRepository []string `json:"gl_repository"`
			}

			if err = json.Unmarshal(b, &reqForm); err != nil {
				http.Error(w, "could not unmarshal form", http.StatusBadRequest)
				return
			}

			if len(reqForm.GLRepository) == 0 {
				http.Error(w, "gl_repository is missing", http.StatusBadRequest)
				return
			}

			params["gl_repository"] = reqForm.GLRepository[0]
		case "application/json":
			if err := json.NewDecoder(r.Body).Decode(&params); err != nil {
				http.Error(w, "error when unmarshalling json body", http.StatusBadRequest)
				return
			}
		}

		if r.Method != http.MethodPost {
			http.Error(w, "method not POST", http.StatusMethodNotAllowed)
			return
		}

		if params["gl_repository"] == "" {
			http.Error(w, "gl_repository is empty", http.StatusUnauthorized)
			return
		}

		if options.GLRepository != "" {
			if params["gl_repository"] != options.GLRepository {
				http.Error(w, "gl_repository is invalid", http.StatusUnauthorized)
				return
			}
		}

		var authenticated bool
		if r.Form.Get("secret_token") == options.SecretToken {
			authenticated = true
		}

		secretHeader, err := base64.StdEncoding.DecodeString(r.Header.Get(secretHeaderName))
		if err == nil {
			if string(secretHeader) == options.SecretToken {
				authenticated = true
			}
		}

		if !authenticated {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)

		_, err = w.Write([]byte(`{"reference_counter_increased": true}`))
		require.NoError(t, err)
	}
}

func handlePostReceive(options GitlabTestServerOptions) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			http.Error(w, "couldn't parse form", http.StatusBadRequest)
			return
		}

		if r.Method != http.MethodPost {
			http.Error(w, "method not POST", http.StatusMethodNotAllowed)
			return
		}

		var params postReceiveForm

		switch r.Header.Get("Content-Type") {
		case "application/x-www-form-urlencoded":
			params = parsePostReceiveForm(r.Form)
		case "application/json":
			if err := json.NewDecoder(r.Body).Decode(&params); err != nil {
				http.Error(w, "could not parse json body", http.StatusBadRequest)
				return
			}
		}

		if params.GLRepository == "" {
			http.Error(w, "gl_repository is empty", http.StatusUnauthorized)
			return
		}

		if options.GLRepository != "" {
			if params.GLRepository != options.GLRepository {
				http.Error(w, "gl_repository is invalid", http.StatusUnauthorized)
				return
			}
		}

		if params.SecretToken != options.SecretToken {
			decodedSecret, err := base64.StdEncoding.DecodeString(r.Header.Get("Gitlab-Shared-Secret"))
			if err != nil {
				http.Error(w, "secret_token is invalid", http.StatusUnauthorized)
				return
			}

			if string(decodedSecret) != options.SecretToken {
				http.Error(w, "secret_token is invalid", http.StatusUnauthorized)
				return
			}
		}

		if params.Identifier == "" {
			http.Error(w, "identifier is empty", http.StatusUnauthorized)
			return
		}

		if options.GLRepository != "" {
			if params.Identifier != options.GLID {
				http.Error(w, "identifier is invalid", http.StatusUnauthorized)
				return
			}
		}

		if params.Changes == "" {
			http.Error(w, "changes is empty", http.StatusUnauthorized)
			return
		}

		if options.Changes != "" {
			if params.Changes != options.Changes {
				http.Error(w, "changes is invalid", http.StatusUnauthorized)
				return
			}
		}

		if len(options.GitPushOptions) != len(params.GitPushOptions) {
			http.Error(w, "invalid push options", http.StatusUnauthorized)
			return
		}

		for i, opt := range options.GitPushOptions {
			if opt != params.GitPushOptions[i] {
				http.Error(w, "invalid push options", http.StatusUnauthorized)
				return
			}
		}

		response := postReceiveResponse{
			ReferenceCounterDecreased: options.PostReceiveCounterDecreased,
		}

		for _, basicMessage := range options.PostReceiveMessages {
			response.Messages = append(response.Messages, postReceiveMessage{
				Message: basicMessage,
				Type:    "basic",
			})
		}

		for _, alertMessage := range options.PostReceiveAlerts {
			response.Messages = append(response.Messages, postReceiveMessage{
				Message: alertMessage,
				Type:    "alert",
			})
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		if err := json.NewEncoder(w).Encode(&response); err != nil {
			panic(err)
		}
	}
}

type postReceiveResponse struct {
	ReferenceCounterDecreased bool                 `json:"reference_counter_decreased"`
	Messages                  []postReceiveMessage `json:"messages"`
}

type postReceiveMessage struct {
	Message string `json:"message"`
	Type    string `json:"type"`
}

func handleLfs(t testing.TB, options GitlabTestServerOptions) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			http.Error(w, "couldn't parse form", http.StatusBadRequest)
			return
		}

		if r.Method != http.MethodGet {
			http.Error(w, "method not GET", http.StatusMethodNotAllowed)
			return
		}

		if options.LfsOid != "" {
			if r.FormValue("oid") != options.LfsOid {
				http.Error(w, "oid parameter does not match", http.StatusBadRequest)
				return
			}
		}

		if options.GlRepository != "" {
			if r.FormValue("gl_repository") != options.GlRepository {
				http.Error(w, "gl_repository parameter does not match", http.StatusBadRequest)
				return
			}
		}

		w.Header().Set("Content-Type", "application/octet-stream")

		if options.LfsStatusCode != 0 {
			w.WriteHeader(options.LfsStatusCode)
		}

		if options.LfsBody != "" {
			_, err := w.Write([]byte(options.LfsBody))
			require.NoError(t, err)
		}
	}
}

func handleCheck(t testing.TB, options GitlabTestServerOptions) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		u, p, ok := r.BasicAuth()
		if !ok || u != options.User || p != options.Password {
			w.WriteHeader(http.StatusUnauthorized)
			require.NoError(t, json.NewEncoder(w).Encode(struct {
				Message string `json:"message"`
			}{Message: "authorization failed"}))
			return
		}

		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, `{"redis": true}`)
	}
}

// GitlabTestServerOptions is a config for a mock gitlab server containing expected values
type GitlabTestServerOptions struct {
	User, Password, SecretToken string
	GLID                        string
	GLRepository                string
	Changes                     string
	PostReceiveMessages         []string
	PostReceiveAlerts           []string
	PostReceiveCounterDecreased bool
	UnixSocket                  bool
	LfsStatusCode               int
	LfsOid                      string
	LfsBody                     string
	Protocol                    string
	GitPushOptions              []string
	GitObjectDir                string
	GitAlternateObjectDirs      []string
	RepoPath                    string
	RelativeURLRoot             string
	GlRepository                string
	ClientCACertPath            string // used to verify client certs are valid
	ServerCertPath              string
	ServerKeyPath               string
}

// NewGitlabTestServer returns a mock gitlab server that responds to the hook api endpoints
func NewGitlabTestServer(t testing.TB, options GitlabTestServerOptions) (url string, cleanup func()) {
	t.Helper()

	mux := http.NewServeMux()
	prefix := strings.TrimRight(options.RelativeURLRoot, "/") + "/api/v4/internal"
	mux.Handle(prefix+"/allowed", http.HandlerFunc(handleAllowed(t, options)))
	mux.Handle(prefix+"/pre_receive", http.HandlerFunc(handlePreReceive(t, options)))
	mux.Handle(prefix+"/post_receive", http.HandlerFunc(handlePostReceive(options)))
	mux.Handle(prefix+"/check", http.HandlerFunc(handleCheck(t, options)))
	mux.Handle(prefix+"/lfs", http.HandlerFunc(handleLfs(t, options)))

	var tlsCfg *tls.Config
	if options.ClientCACertPath != "" {
		caCertPEM, err := ioutil.ReadFile(options.ClientCACertPath)
		require.NoError(t, err)

		certPool := x509.NewCertPool()
		require.True(t, certPool.AppendCertsFromPEM(caCertPEM))

		serverCert, err := tls.LoadX509KeyPair(options.ServerCertPath, options.ServerKeyPath)
		require.NoError(t, err)

		tlsCfg = &tls.Config{
			ClientCAs:    certPool,
			ClientAuth:   tls.RequireAndVerifyClientCert,
			Certificates: []tls.Certificate{serverCert},
			MinVersion:   tls.VersionTLS12,
		}
	}

	if options.UnixSocket {
		return startSocketHTTPServer(t, mux, tlsCfg)
	}

	var server *httptest.Server
	if tlsCfg == nil {
		server = httptest.NewServer(mux)
	} else {
		server = httptest.NewUnstartedServer(mux)
		server.TLS = tlsCfg
		server.StartTLS()
	}
	return server.URL, server.Close
}

func startSocketHTTPServer(t testing.TB, mux *http.ServeMux, tlsCfg *tls.Config) (string, func()) {
	tempDir := TempDir(t)

	filename := filepath.Join(tempDir, "http-test-server")
	socketListener, err := net.Listen("unix", filename)
	require.NoError(t, err)

	server := http.Server{
		Handler:   mux,
		TLSConfig: tlsCfg,
	}

	go server.Serve(socketListener)

	url := "http+unix://" + filename
	cleanup := func() {
		require.NoError(t, server.Close())
	}

	return url, cleanup
}

// WriteTemporaryGitalyConfigFile writes a gitaly toml file into a temporary directory. It returns the path to
// the file as well as a cleanup function
func WriteTemporaryGitalyConfigFile(t testing.TB, tempDir, gitlabURL, user, password, secretFile string) (string, func()) {
	path := filepath.Join(tempDir, "config.toml")
	contents := fmt.Sprintf(`
[gitlab]
  url = "%s"
  secret_file = "%s"
  [gitlab.http-settings]
    user = %q
    password = %q
`, gitlabURL, secretFile, user, password)

	require.NoError(t, ioutil.WriteFile(path, []byte(contents), 0644))
	return path, func() {
		require.NoError(t, os.RemoveAll(path))
	}
}

// WriteShellSecretFile writes a .gitlab_shell_secret file in the specified directory
func WriteShellSecretFile(t testing.TB, dir, secretToken string) string {
	t.Helper()

	require.NoError(t, os.MkdirAll(dir, os.ModeDir))
	filePath := filepath.Join(dir, ".gitlab_shell_secret")
	require.NoError(t, ioutil.WriteFile(filePath, []byte(secretToken), 0644))
	return filePath
}

// HTTPSettings contains fields for http settings
type HTTPSettings struct {
	User     string `yaml:"user"`
	Password string `yaml:"password"`
}

// NewServerWithHealth creates a new GRPC server with the health server set up.
// It will listen on the socket identified by `socketName`.
func NewServerWithHealth(t testing.TB, socketName string) (*grpc.Server, *health.Server) {
	lis, err := net.Listen("unix", socketName)
	require.NoError(t, err)

	return NewHealthServerWithListener(t, lis)
}

// NewHealthServerWithListener creates a new GRPC server with the health server
// set up. It will listen on the given listener.
func NewHealthServerWithListener(t testing.TB, listener net.Listener) (*grpc.Server, *health.Server) {
	srv := NewTestGrpcServer(t, nil, nil)
	healthSrvr := health.NewServer()
	healthpb.RegisterHealthServer(srv, healthSrvr)
	healthSrvr.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)

	go srv.Serve(listener)

	return srv, healthSrvr
}

// SetupAndStartGitlabServer creates a new GitlabTestServer, starts it and sets
// up the gitlab-shell secret.
func SetupAndStartGitlabServer(t testing.TB, shellDir string, c *GitlabTestServerOptions) (string, func()) {
	url, cleanup := NewGitlabTestServer(t, *c)

	WriteShellSecretFile(t, shellDir, c.SecretToken)

	return url, cleanup
}
