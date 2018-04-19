package rubyserver

import (
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"gitlab.com/gitlab-org/gitaly/internal/command"
	"gitlab.com/gitlab-org/gitaly/internal/config"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/rubyserver/balancer"
	"gitlab.com/gitlab-org/gitaly/internal/supervisor"
	"gitlab.com/gitlab-org/gitaly/internal/version"
	"gitlab.com/gitlab-org/gitaly/streamio"

	pb "gitlab.com/gitlab-org/gitaly-proto/go"

	"github.com/grpc-ecosystem/go-grpc-prometheus"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

var (
	socketDir string

	lazyInit sync.Once

	// ConnectTimeout is the timeout for establishing a connection to the gitaly-ruby process.
	ConnectTimeout = 40 * time.Second
)

func init() {
	timeout64, err := strconv.ParseInt(os.Getenv("GITALY_RUBY_CONNECT_TIMEOUT"), 10, 32)
	if err == nil && timeout64 > 0 {
		ConnectTimeout = time.Duration(timeout64) * time.Second
	}
}

func prepareSocketPath() {
	// The socket path must be short-ish because listen(2) fails on long
	// socket paths. We hope/expect that ioutil.TempDir creates a directory
	// that is not too deep. We need a directory, not a tempfile, because we
	// will later want to set its permissions to 0700. The permission change
	// is done in the Ruby child process.
	var err error
	socketDir, err = ioutil.TempDir("", "gitaly-ruby")
	if err != nil {
		log.Fatalf("create ruby server socket directory: %v", err)
	}
}

func socketPath(id int) string {
	if socketDir == "" {
		panic("socketDir is not set")
	}

	return path.Join(filepath.Clean(socketDir), fmt.Sprintf("socket.%d", id))
}

// Server represents a gitaly-ruby helper process.
type Server struct {
	workers      []*worker
	clientConnMu sync.RWMutex
	clientConn   *grpc.ClientConn
}

// Stop shuts down the gitaly-ruby helper process and cleans up resources.
func (s *Server) Stop() {
	if s != nil {
		s.clientConnMu.RLock()
		defer s.clientConnMu.RUnlock()
		if s.clientConn != nil {
			s.clientConn.Close()
		}

		for _, w := range s.workers {
			w.stopMonitor()
			w.Process.Stop()
		}
	}

	if socketDir != "" {
		os.RemoveAll(socketDir)
	}
}

// Start spawns the Ruby server.
func Start() (*Server, error) {
	lazyInit.Do(prepareSocketPath)

	wd, err := os.Getwd()
	if err != nil {
		return nil, err
	}

	cfg := config.Config
	env := append(
		os.Environ(),
		"GITALY_RUBY_GIT_BIN_PATH="+command.GitPath(),
		fmt.Sprintf("GITALY_RUBY_WRITE_BUFFER_SIZE=%d", streamio.WriteBufferSize),
		fmt.Sprintf("GITALY_RUBY_MAX_COMMIT_OR_TAG_MESSAGE_SIZE=%d", helper.MaxCommitOrTagMessageSize),
		"GITALY_RUBY_GITLAB_SHELL_PATH="+cfg.GitlabShell.Dir,
		"GITALY_RUBY_GITALY_BIN_DIR="+cfg.BinDir,
		"GITALY_VERSION="+version.GetVersion(),
	)
	if dsn := cfg.Logging.RubySentryDSN; dsn != "" {
		env = append(env, "SENTRY_DSN="+dsn)
	}

	gitalyRuby := path.Join(cfg.Ruby.Dir, "bin/gitaly-ruby")

	s := &Server{}
	for i := 0; i < cfg.Ruby.NumWorkers; i++ {
		name := fmt.Sprintf("gitaly-ruby.%d", i)
		socketPath := socketPath(i)

		// Use 'ruby-cd' to make sure gitaly-ruby has the same working directory
		// as the current process. This is a hack to sort-of support relative
		// Unix socket paths.
		args := []string{"bundle", "exec", "bin/ruby-cd", wd, gitalyRuby, strconv.Itoa(os.Getpid()), socketPath}

		events := make(chan supervisor.Event)
		check := func() error { return ping(socketPath) }
		p, err := supervisor.New(name, env, args, cfg.Ruby.Dir, cfg.Ruby.MaxRSS, events, check)
		if err != nil {
			return nil, err
		}

		s.workers = append(s.workers, newWorker(p, socketPath, events, false))
	}

	return s, nil
}

// CommitServiceClient returns a CommitServiceClient instance that is
// configured to connect to the running Ruby server. This assumes Start()
// has been called already.
func (s *Server) CommitServiceClient(ctx context.Context) (pb.CommitServiceClient, error) {
	conn, err := s.getConnection(ctx)
	return pb.NewCommitServiceClient(conn), err
}

// DiffServiceClient returns a DiffServiceClient instance that is
// configured to connect to the running Ruby server. This assumes Start()
// has been called already.
func (s *Server) DiffServiceClient(ctx context.Context) (pb.DiffServiceClient, error) {
	conn, err := s.getConnection(ctx)
	return pb.NewDiffServiceClient(conn), err
}

// RefServiceClient returns a RefServiceClient instance that is
// configured to connect to the running Ruby server. This assumes Start()
// has been called already.
func (s *Server) RefServiceClient(ctx context.Context) (pb.RefServiceClient, error) {
	conn, err := s.getConnection(ctx)
	return pb.NewRefServiceClient(conn), err
}

// OperationServiceClient returns a OperationServiceClient instance that is
// configured to connect to the running Ruby server. This assumes Start()
// has been called already.
func (s *Server) OperationServiceClient(ctx context.Context) (pb.OperationServiceClient, error) {
	conn, err := s.getConnection(ctx)
	return pb.NewOperationServiceClient(conn), err
}

// RepositoryServiceClient returns a RefServiceClient instance that is
// configured to connect to the running Ruby server. This assumes Start()
// has been called already.
func (s *Server) RepositoryServiceClient(ctx context.Context) (pb.RepositoryServiceClient, error) {
	conn, err := s.getConnection(ctx)
	return pb.NewRepositoryServiceClient(conn), err
}

// WikiServiceClient returns a WikiServiceClient instance that is
// configured to connect to the running Ruby server. This assumes Start()
// has been called already.
func (s *Server) WikiServiceClient(ctx context.Context) (pb.WikiServiceClient, error) {
	conn, err := s.getConnection(ctx)
	return pb.NewWikiServiceClient(conn), err
}

// ConflictsServiceClient returns a ConflictsServiceClient instance that is
// configured to connect to the running Ruby server. This assumes Start()
// has been called already.
func (s *Server) ConflictsServiceClient(ctx context.Context) (pb.ConflictsServiceClient, error) {
	conn, err := s.getConnection(ctx)
	return pb.NewConflictsServiceClient(conn), err
}

// RemoteServiceClient returns a RemoteServiceClient instance that is
// configured to connect to the running Ruby server. This assumes Start()
// has been called already.
func (s *Server) RemoteServiceClient(ctx context.Context) (pb.RemoteServiceClient, error) {
	conn, err := s.getConnection(ctx)
	return pb.NewRemoteServiceClient(conn), err
}

// BlobServiceClient returns a BlobServiceClient instance that is
// configured to connect to the running Ruby server. This assumes Start()
// has been called already.
func (s *Server) BlobServiceClient(ctx context.Context) (pb.BlobServiceClient, error) {
	conn, err := s.getConnection(ctx)
	return pb.NewBlobServiceClient(conn), err
}

func (s *Server) getConnection(ctx context.Context) (*grpc.ClientConn, error) {
	s.clientConnMu.RLock()
	conn := s.clientConn
	s.clientConnMu.RUnlock()

	if conn != nil {
		return conn, nil
	}

	return s.createConnection(ctx)
}

func (s *Server) createConnection(ctx context.Context) (*grpc.ClientConn, error) {
	s.clientConnMu.Lock()
	defer s.clientConnMu.Unlock()

	if conn := s.clientConn; conn != nil {
		return conn, nil
	}

	dialCtx, cancel := context.WithTimeout(ctx, ConnectTimeout)
	defer cancel()

	conn, err := grpc.DialContext(dialCtx, balancer.Scheme+"://gitaly-ruby", dialOptions()...)
	if err != nil {
		return nil, err
	}

	s.clientConn = conn
	return s.clientConn, nil
}

func dialOptions() []grpc.DialOption {
	return []grpc.DialOption{
		grpc.WithBlock(), // With this we get retries. Without, connections fail fast.
		grpc.WithInsecure(),
		grpc.WithDialer(func(addr string, timeout time.Duration) (net.Conn, error) {
			return net.DialTimeout("unix", addr, timeout)
		}),
		grpc.WithUnaryInterceptor(grpc_prometheus.UnaryClientInterceptor),
		grpc.WithStreamInterceptor(grpc_prometheus.StreamClientInterceptor),
	}
}
