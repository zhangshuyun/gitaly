package testhelper

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/BurntSushi/toml"
	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_logrus "github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus"
	grpc_ctxtags "github.com/grpc-ecosystem/go-grpc-middleware/tags"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	gitalyauth "gitlab.com/gitlab-org/gitaly/auth"
	"gitlab.com/gitlab-org/gitaly/internal/config"
	"gitlab.com/gitlab-org/gitaly/internal/config/auth"
	"gitlab.com/gitlab-org/gitaly/internal/git/hooks"
	"gitlab.com/gitlab-org/gitaly/internal/helper/fieldextractors"
	praefectconfig "gitlab.com/gitlab-org/gitaly/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/models"
	serverauth "gitlab.com/gitlab-org/gitaly/internal/server/auth"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"gopkg.in/yaml.v2"
)

// PraefectEnabled returns whether or not tests should use a praefect proxy
func PraefectEnabled() bool {
	_, ok := os.LookupEnv("GITALY_TEST_PRAEFECT_BIN")
	return ok
}

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

// NewTestServer instantiates a new TestServer
func NewTestServer(srv *grpc.Server, opts ...TestServerOpt) *TestServer {
	ts := &TestServer{
		grpcServer: srv,
	}

	for _, opt := range opts {
		opt(ts)
	}

	if len(ts.storages) == 0 {
		ts.storages = []string{"default"}
	}

	return ts
}

// NewServerWithAuth creates a new test server with authentication
func NewServerWithAuth(tb testing.TB, streamInterceptors []grpc.StreamServerInterceptor, unaryInterceptors []grpc.UnaryServerInterceptor, token string, opts ...TestServerOpt) *TestServer {
	if token != "" {
		if PraefectEnabled() {
			opts = append(opts, WithToken(token))
		}
		streamInterceptors = append(streamInterceptors, serverauth.StreamServerInterceptor(auth.Config{Token: token}))
		unaryInterceptors = append(unaryInterceptors, serverauth.UnaryServerInterceptor(auth.Config{Token: token}))
	}

	return NewServer(
		tb,
		streamInterceptors,
		unaryInterceptors,
		opts...,
	)
}

// TestServer wraps a grpc Server and handles automatically putting a praefect in front of a gitaly instance
// if necessary
type TestServer struct {
	grpcServer *grpc.Server
	socket     string
	process    *os.Process
	token      string
	storages   []string
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
	}
}

// Socket returns the socket file the test server is listening on
func (p *TestServer) Socket() string {
	return p.socket
}

// Start will start the grpc server as well as spawn a praefect instance if GITALY_TEST_PRAEFECT_BIN is enabled
func (p *TestServer) Start() error {
	gitalyServerSocketPath := GetTemporaryGitalySocketFileName()

	listener, err := net.Listen("unix", gitalyServerSocketPath)
	if err != nil {
		return err
	}

	go p.grpcServer.Serve(listener)

	praefectBinPath, ok := os.LookupEnv("GITALY_TEST_PRAEFECT_BIN")
	if !ok {
		p.socket = gitalyServerSocketPath
		return nil
	}

	tempDir, err := ioutil.TempDir("", "praefect-test-server")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tempDir)

	praefectServerSocketPath := GetTemporaryGitalySocketFileName()

	configFilePath := filepath.Join(tempDir, "config.toml")
	configFile, err := os.Create(configFilePath)
	if err != nil {
		return err
	}
	defer configFile.Close()

	c := praefectconfig.Config{
		SocketPath: praefectServerSocketPath,
		Auth: auth.Config{
			Token: p.token,
		},
	}

	for _, storage := range p.storages {
		c.VirtualStorages = append(c.VirtualStorages, &praefectconfig.VirtualStorage{
			Name: storage,
			Nodes: []*models.Node{
				{
					Storage:        storage,
					Address:        "unix:/" + gitalyServerSocketPath,
					DefaultPrimary: true,
					Token:          p.token,
				},
			},
		})
	}

	if err := toml.NewEncoder(configFile).Encode(&c); err != nil {
		return err
	}
	if err = configFile.Sync(); err != nil {
		return err
	}
	configFile.Close()

	cmd := exec.Command(praefectBinPath, "-config", configFilePath)
	cmd.Stderr = os.Stderr
	cmd.Stdout = os.Stdout

	p.socket = praefectServerSocketPath

	if err := cmd.Start(); err != nil {
		return err
	}
	go cmd.Wait()

	opts := []grpc.DialOption{grpc.WithInsecure()}
	if p.token != "" {
		opts = append(opts, grpc.WithPerRPCCredentials(gitalyauth.RPCCredentials(p.token)))
	}

	conn, err := grpc.Dial("unix://"+praefectServerSocketPath, opts...)

	if err != nil {
		return fmt.Errorf("dial: %v", err)
	}
	defer conn.Close()

	if err = waitForPraefectStartup(conn); err != nil {
		return err
	}

	p.process = cmd.Process

	return nil
}

func waitForPraefectStartup(conn *grpc.ClientConn) error {
	client := healthpb.NewHealthClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := client.Check(ctx, &healthpb.HealthCheckRequest{}, grpc.WaitForReady(true))
	if err != nil {
		return err
	}

	if resp.Status != healthpb.HealthCheckResponse_SERVING {
		return errors.New("server not yet ready to serve")
	}

	return nil
}

// NewServer creates a Server for testing purposes
func NewServer(tb testing.TB, streamInterceptors []grpc.StreamServerInterceptor, unaryInterceptors []grpc.UnaryServerInterceptor, opts ...TestServerOpt) *TestServer {
	logger := NewTestLogger(tb)
	logrusEntry := log.NewEntry(logger).WithField("test", tb.Name())

	ctxTagger := grpc_ctxtags.WithFieldExtractorForInitialReq(fieldextractors.FieldExtractor)
	ctxStreamTagger := grpc_ctxtags.StreamServerInterceptor(ctxTagger)
	ctxUnaryTagger := grpc_ctxtags.UnaryServerInterceptor(ctxTagger)

	streamInterceptors = append([]grpc.StreamServerInterceptor{ctxStreamTagger, grpc_logrus.StreamServerInterceptor(logrusEntry)}, streamInterceptors...)
	unaryInterceptors = append([]grpc.UnaryServerInterceptor{ctxUnaryTagger, grpc_logrus.UnaryServerInterceptor(logrusEntry)}, unaryInterceptors...)

	return NewTestServer(
		grpc.NewServer(
			grpc.StreamInterceptor(grpc_middleware.ChainStreamServer(streamInterceptors...)),
			grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(unaryInterceptors...)),
		),
		opts...,
	)
}

var changeLineRegex = regexp.MustCompile("^[a-f0-9]{40} [a-f0-9]{40} refs/[^ ]+$")

func handleAllowed(t *testing.T, options GitlabTestServerOptions) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		require.NoError(t, r.ParseForm())
		require.Equal(t, http.MethodPost, r.Method, "expected http post")
		require.Equal(t, "application/x-www-form-urlencoded", r.Header.Get("Content-Type"))

		if options.GLID != "" {
			glidSplit := strings.SplitN(options.GLID, "-", 2)
			require.Len(t, glidSplit, 2, "number of GLID components")

			switch glidSplit[0] {
			case "user":
				require.Equal(t, glidSplit[1], r.Form.Get("user_id"))
			case "key":
				require.Equal(t, glidSplit[1], r.Form.Get("key_id"))
			case "username":
				require.Equal(t, glidSplit[1], r.Form.Get("username"))
			default:
				t.Fatalf("invalid GLID: %q", options.GLID)
			}
		}

		require.NotEmpty(t, r.Form.Get("gl_repository"), "gl_repository should not be empty")
		if options.GLRepository != "" {
			require.Equal(t, options.GLRepository, r.Form.Get("gl_repository"), "expected value of gl_repository should match form")
		}
		require.NotEmpty(t, r.Form.Get("protocol"), "protocol should not be empty")
		if options.Protocol != "" {
			require.Equal(t, options.Protocol, r.Form.Get("protocol"), "expected value of options.Protocol should match form")
		}

		if options.Changes != "" {
			require.Equal(t, options.Changes, r.Form.Get("changes"), "expected value of options.Changes should match form")
		} else {
			changeLines := strings.Split(strings.TrimSuffix(r.Form.Get("changes"), "\n"), "\n")
			for _, line := range changeLines {
				require.Regexp(t, changeLineRegex, line)
			}
		}

		w.Header().Set("Content-Type", "application/json")
		if r.Form.Get("secret_token") == options.SecretToken {
			w.Write([]byte(`{"status":true}`))
			return
		}
		w.WriteHeader(http.StatusUnauthorized)
		w.Write([]byte(`{"message":"401 Unauthorized"}`))
	}
}

func handlePreReceive(t *testing.T, options GitlabTestServerOptions) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		require.NoError(t, r.ParseForm())
		require.Equal(t, http.MethodPost, r.Method)
		require.Equal(t, "application/x-www-form-urlencoded", r.Header.Get("Content-Type"))
		require.NotEmpty(t, r.Form.Get("gl_repository"), "gl_repository should not be empty")
		if options.GLRepository != "" {
			require.Equal(t, options.GLRepository, r.Form.Get("gl_repository"), "expected value of gl_repository should match form")
		}
		require.Equal(t, options.SecretToken, r.Form.Get("secret_token"), "expected value of secret_token should match form")

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"reference_counter_increased": true}`))
	}
}

func handlePostReceive(t *testing.T, options GitlabTestServerOptions) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		require.NoError(t, r.ParseForm())
		require.Equal(t, http.MethodPost, r.Method)
		require.Equal(t, "application/x-www-form-urlencoded", r.Header.Get("Content-Type"))
		require.NotEmpty(t, r.Form.Get("gl_repository"))
		if options.GLRepository != "" {
			require.Equal(t, options.GLRepository, r.Form.Get("gl_repository"), "expected value of gl_repository should match form")
		}
		require.Equal(t, options.SecretToken, r.Form.Get("secret_token"), "expected value of gl_repository should match form")

		require.NotEmpty(t, r.Form.Get("identifier"), "identifier should exist")
		if options.GLID != "" {
			require.Equal(t, options.GLID, r.Form.Get("identifier"), "identifier should be GLID")
		}

		require.NotEmpty(t, r.Form.Get("changes"), "changes should exist")
		if options.Changes != "" {
			require.Regexp(t, options.Changes, r.Form.Get("changes"), "expected value of changes should match form")
		}

		if len(options.GitPushOptions) > 0 {
			require.Equal(t, options.GitPushOptions, r.Form["push_options[]"], "expected value of push_options should match form")
		}

		w.WriteHeader(http.StatusOK)
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"reference_counter_decreased": %v}`, options.PostReceiveCounterDecreased)
	}
}

func handleCheck(options GitlabTestServerOptions) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		u, p, ok := r.BasicAuth()
		if !ok || u != options.User || p != options.Password {
			http.Error(w, "authorization failed", http.StatusUnauthorized)
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
	PostReceiveCounterDecreased bool
	Protocol                    string
	GitPushOptions              []string
}

// NewGitlabTestServer returns a mock gitlab server that responds to the hook api endpoints
func NewGitlabTestServer(t *testing.T, options GitlabTestServerOptions) *httptest.Server {
	mux := http.NewServeMux()
	mux.Handle("/api/v4/internal/allowed", http.HandlerFunc(handleAllowed(t, options)))
	mux.Handle("/api/v4/internal/pre_receive", http.HandlerFunc(handlePreReceive(t, options)))
	mux.Handle("/api/v4/internal/post_receive", http.HandlerFunc(handlePostReceive(t, options)))
	mux.Handle("/api/v4/internal/check", http.HandlerFunc(handleCheck(options)))

	return httptest.NewServer(mux)
}

// CreateTemporaryGitlabShellDir creates a temporary gitlab shell directory. It returns the path to the directory
// and a cleanup function
func CreateTemporaryGitlabShellDir(t *testing.T) (string, func()) {
	tempDir, err := ioutil.TempDir("", "gitlab-shell")
	require.NoError(t, err)
	return tempDir, func() {
		require.NoError(t, os.RemoveAll(tempDir))
	}
}

// WriteTemporaryGitlabShellConfigFile writes a gitlab shell config.yml in a temporary directory. It returns the path
// and a cleanup function
func WriteTemporaryGitlabShellConfigFile(t *testing.T, dir string, config GitlabShellConfig) (string, func()) {
	out, err := yaml.Marshal(&config)
	require.NoError(t, err)

	path := filepath.Join(dir, "config.yml")
	require.NoError(t, ioutil.WriteFile(path, out, 0644))

	return path, func() {
		os.RemoveAll(path)
	}
}

// WriteTemporaryGitalyConfigFile writes a gitaly toml file into a temporary directory. It returns the path to
// the file as well as a cleanup function
func WriteTemporaryGitalyConfigFile(t *testing.T, tempDir string) (string, func()) {
	path := filepath.Join(tempDir, "config.toml")
	contents := fmt.Sprintf(`
[gitlab-shell]
  dir = "%s/gitlab-shell"
`, tempDir)
	require.NoError(t, ioutil.WriteFile(path, []byte(contents), 0644))

	return path, func() {
		os.RemoveAll(path)
	}
}

// EnvForHooks generates a set of environment variables for gitaly hooks
func EnvForHooks(t *testing.T, glRepo, gitlabShellDir, glID string, gitPushOptions ...string) []string {
	rubyDir, err := filepath.Abs("../../ruby")
	require.NoError(t, err)

	return append(append(oldEnv(t, glRepo, gitlabShellDir, glID), []string{
		fmt.Sprintf("GITALY_BIN_DIR=%s", config.Config.BinDir),
		fmt.Sprintf("GITALY_RUBY_DIR=%s", rubyDir),
	}...), hooks.GitPushOptions(gitPushOptions)...)
}

func oldEnv(t *testing.T, glRepo, gitlabShellDir, glID string) []string {
	return append([]string{
		fmt.Sprintf("GL_ID=%s", glID),
		fmt.Sprintf("GL_REPOSITORY=%s", glRepo),
		"GL_PROTOCOL=ssh",
		fmt.Sprintf("GITALY_GITLAB_SHELL_DIR=%s", gitlabShellDir),
		fmt.Sprintf("GITALY_LOG_DIR=%s", gitlabShellDir),
		"GITALY_LOG_LEVEL=info",
		"GITALY_LOG_FORMAT=json",
	}, os.Environ()...)
}

// WriteShellSecretFile writes a .gitlab_shell_secret file in the specified directory
func WriteShellSecretFile(t *testing.T, dir, secretToken string) {
	require.NoError(t, ioutil.WriteFile(filepath.Join(dir, ".gitlab_shell_secret"), []byte(secretToken), 0644))
}

// GitlabShellConfig contains a subset of gitlabshell's config.yml
type GitlabShellConfig struct {
	GitlabURL    string       `yaml:"gitlab_url"`
	HTTPSettings HTTPSettings `yaml:"http_settings"`
}

// HTTPSettings contains fields for http settings
type HTTPSettings struct {
	User     string `yaml:"user"`
	Password string `yaml:"password"`
}

func NewServerWithHealth(t testing.TB, socketName string) (*grpc.Server, *health.Server) {
	srv := NewTestGrpcServer(t, nil, nil)
	healthSrvr := health.NewServer()
	grpc_health_v1.RegisterHealthServer(srv, healthSrvr)
	healthSrvr.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)

	lis, err := net.Listen("unix", socketName)
	require.NoError(t, err)

	go srv.Serve(lis)

	return srv, healthSrvr
}

func SetupAndStartGitlabServer(t *testing.T, c *GitlabTestServerOptions) func() {
	ts := NewGitlabTestServer(t, *c)

	WriteTemporaryGitlabShellConfigFile(t, config.Config.GitlabShell.Dir, GitlabShellConfig{GitlabURL: ts.URL})
	WriteShellSecretFile(t, config.Config.GitlabShell.Dir, c.SecretToken)

	return func() {
		ts.Close()
	}
}
