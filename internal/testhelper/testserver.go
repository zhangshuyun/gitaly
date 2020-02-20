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
	"strings"
	"testing"
	"time"

	"github.com/BurntSushi/toml"
	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_logrus "github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus"
	grpc_ctxtags "github.com/grpc-ecosystem/go-grpc-middleware/tags"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/config"
	"gitlab.com/gitlab-org/gitaly/internal/git/hooks"
	"gitlab.com/gitlab-org/gitaly/internal/helper/fieldextractors"
	praefectconfig "gitlab.com/gitlab-org/gitaly/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/models"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"gopkg.in/yaml.v2"
)

// NewTestServer instantiates a new TestServer
func NewTestServer(srv *grpc.Server) *TestServer {
	return &TestServer{
		grpcServer: srv,
	}
}

// TestServer wraps a grpc Server and handles automatically putting a praefect in front of a gitaly instance
// if necessary
type TestServer struct {
	grpcServer *grpc.Server
	socket     string
	process    *os.Process
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
		VirtualStorages: []*praefectconfig.VirtualStorage{
			{
				Name: "default",
				Nodes: []*models.Node{
					{
						Storage:        "default",
						Address:        "unix:/" + gitalyServerSocketPath,
						DefaultPrimary: true,
					},
				},
			},
		},
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

	conn, err := grpc.Dial("unix://"+praefectServerSocketPath, grpc.WithInsecure())

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
func NewServer(tb testing.TB, streamInterceptors []grpc.StreamServerInterceptor, unaryInterceptors []grpc.UnaryServerInterceptor) *TestServer {
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
		))
}

func handleAllowed(secretToken, glID, glRepository, changes, protocol string) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		if http.MethodPost != r.Method {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		if r.Header.Get("Content-Type") != "application/x-www-form-urlencoded" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}

		var idFound bool
		var id string
		for _, v := range []string{r.Form.Get("key_id"), r.Form.Get("user_id"), r.Form.Get("username")} {
			if v != "" {
				idFound = true
				id = v
				break
			}
		}

		switch {
		case strings.HasPrefix(glID, "user-"):
			glID = strings.TrimPrefix(glID, "user-")
		case strings.HasPrefix(glID, "key-"):
			glID = strings.TrimPrefix(glID, "key-")
		case strings.HasPrefix(glID, "username-"):
			glID = strings.TrimPrefix(glID, "username-")
		}

		if !idFound || (glID != "" && glID != id) {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}

		if r.Form.Get("gl_repository") == "" || glRepository != "" && glRepository != r.Form.Get("gl_repository") {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}

		if r.Form.Get("protocol") == "" || protocol != "" && protocol != r.Form.Get("protocol") {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}

		if r.Form.Get("changes") == "" || changes != "" && changes != r.Form.Get("changes") {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		if r.Form.Get("secret_token") == secretToken {
			w.Write([]byte(`{"status":true}`))
			return
		}

		w.WriteHeader(http.StatusUnauthorized)
		w.Write([]byte(`{"message":"401 Unauthorized"}`))
	}
}

func handlePreReceive(secretToken, glRepository string) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		if r.Header.Get("Content-Type") != "application/x-www-form-urlencoded" {
			w.WriteHeader(http.StatusNotAcceptable)
			return
		}
		if r.Form.Get("gl_repository") == "" || (glRepository != "" && r.Form.Get("gl_repository") != glRepository) {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		if r.Form.Get("secret_token") != secretToken {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"reference_counter_increased": true}`))
	}
}

func handlePostReceive(secretToken, userID, glRepository, changes string, counterDecreased bool, gitPushOptions ...string) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		if r.Header.Get("Content-Type") != "application/x-www-form-urlencoded" {
			w.WriteHeader(http.StatusNotAcceptable)
			return
		}
		if r.Form.Get("gl_repository") == "" || (glRepository != "" && r.Form.Get("gl_repository") != glRepository) {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		if r.Form.Get("secret_token") != secretToken {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}

		if r.Form.Get("identifier") == "" || (r.Form.Get("identifier") != userID) {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		if r.Form.Get("changes") == "" || changes != "" && changes != r.Form.Get("changes") {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		if len(gitPushOptions) > 0 {
			options := r.Form["push_options[]"]
			for i, option := range options {
				if gitPushOptions[i] != option {
					w.WriteHeader(http.StatusInternalServerError)
					return
				}
			}
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(fmt.Sprintf(`{"reference_counter_decreased": %v}`, counterDecreased)))
	}
}

func handleCheck(user, password string) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		u, p, ok := r.BasicAuth()
		if !ok || u != user || p != password {
			http.Error(w, "authorization failed", http.StatusUnauthorized)
			return
		}

		w.Write([]byte(`{"redis": true}`))
		w.WriteHeader(http.StatusOK)
	}
}

// GitlabServerConfig is a config for a mock gitlab server
type GitlabServerConfig struct {
	User, Password, SecretToken string
	GLID                        string
	GLRepository                string
	Changes                     string
	PostReceiveCounterDecreased bool
	Protocol                    string
	GitPushOptions              []string
}

// NewGitlabTestServer returns a mock gitlab server that responds to the hook api endpoints
func NewGitlabTestServer(c GitlabServerConfig) *httptest.Server {
	mux := http.NewServeMux()
	mux.Handle("/api/v4/internal/allowed", http.HandlerFunc(handleAllowed(c.SecretToken, c.GLID, c.GLRepository, c.Changes, c.Protocol)))
	mux.Handle("/api/v4/internal/pre_receive", http.HandlerFunc(handlePreReceive(c.SecretToken, c.GLRepository)))
	mux.Handle("/api/v4/internal/post_receive", http.HandlerFunc(handlePostReceive(c.SecretToken, c.GLID, c.GLRepository, c.Changes, c.PostReceiveCounterDecreased, c.GitPushOptions...)))
	mux.Handle("/api/v4/internal/check", http.HandlerFunc(handleCheck(c.User, c.Password)))

	return httptest.NewServer(mux)
}

// CreateTemporaryGitlabShellDir creates a temporary gitlab shell directory. It returns the path to the directory
// and a cleanup function
func CreateTemporaryGitlabShellDir() (string, func()) {
	tempDir, err := ioutil.TempDir("", "gitlab-shell")
	if err != nil {
		log.Fatal(err)
	}
	return tempDir, func() {
		os.RemoveAll(tempDir)
	}
}

// WriteTemporaryGitlabShellConfigFile writes a gitlab shell config.yml in a temporary directory. It returns the path
// and a cleanup function
func WriteTemporaryGitlabShellConfigFile(dir string, config GitlabShellConfig) (string, func()) {
	out, err := yaml.Marshal(&config)
	if err != nil {
		log.Fatal(err)
	}

	path := filepath.Join(dir, "config.yml")
	if err := ioutil.WriteFile(path, out, 0644); err != nil {
		log.Fatal(err)
	}

	return path, func() {
		os.RemoveAll(path)
	}
}

// WriteTemporaryGitalyConfigFile writes a gitaly toml file into a temporary directory. It returns the path to
// the file as well as a cleanup function
func WriteTemporaryGitalyConfigFile(tempDir string) (string, func()) {
	path := filepath.Join(tempDir, "config.toml")
	contents := fmt.Sprintf(`
[gitlab-shell]
  dir = "%s/gitlab-shell"
`, tempDir)

	if err := ioutil.WriteFile(path, []byte(contents), 0644); err != nil {
		log.Fatal(err)
	}

	return path, func() {
		os.RemoveAll(path)
	}
}

// EnvForHooks generates a set of environment variables for gitaly hooks
func EnvForHooks(glRepo, gitlabShellDir, glID string, gitPushOptions ...string) []string {
	rubyDir, err := filepath.Abs("../../ruby")
	if err != nil {
		log.Fatal(err)
	}

	return append(append(oldEnv(glRepo, gitlabShellDir, glID), []string{
		fmt.Sprintf("GITALY_BIN_DIR=%s", config.Config.BinDir),
		fmt.Sprintf("GITALY_RUBY_DIR=%s", rubyDir),
	}...), hooks.GitPushOptions(gitPushOptions)...)
}

func oldEnv(glRepo, gitlabShellDir, glID string) []string {
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
func WriteShellSecretFile(dir, secretToken string) {
	if err := ioutil.WriteFile(filepath.Join(dir, ".gitlab_shell_secret"), []byte(secretToken), 0644); err != nil {
		log.Fatal(err)
	}
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
