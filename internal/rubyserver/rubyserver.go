package rubyserver

import (
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"gitlab.com/gitlab-org/gitaly/internal/config"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/supervisor"

	pb "gitlab.com/gitlab-org/gitaly-proto/go"

	log "github.com/Sirupsen/logrus"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

const (
	repoPathHeader = "gitaly-repo-path"
)

var (
	socketPath string

	lazyInit sync.Once

	// ConnectTimeout is the timeout for establishing a connection to the gitaly-ruby process.
	ConnectTimeout = 20 * time.Second
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
	socketDir, err := ioutil.TempDir("", "gitaly-ruby")
	if err != nil {
		log.Fatalf("create ruby server socket directory: %v", err)
	}
	socketPath = path.Join(filepath.Clean(socketDir), "socket")
}

// Start spawns the Ruby server.
func Start() (*supervisor.Process, error) {
	lazyInit.Do(prepareSocketPath)

	args := []string{"bundle", "exec", "bin/gitaly-ruby", fmt.Sprintf("%d", os.Getpid()), socketPath}
	env := append(os.Environ(), "GITALY_RUBY_GIT_BIN_PATH="+helper.GitPath())
	return supervisor.New(env, args, config.Config.Ruby.Dir)
}

// CommitServiceClient returns a CommitServiceClient instance that is
// configured to connect to the running Ruby server. This assumes Start()
// has been called already.
func CommitServiceClient(ctx context.Context) (pb.CommitServiceClient, error) {
	conn, err := newConnection(ctx)
	return pb.NewCommitServiceClient(conn), err
}

func newConnection(ctx context.Context) (*grpc.ClientConn, error) {
	dialCtx, cancel := context.WithTimeout(ctx, ConnectTimeout)
	defer cancel()
	return grpc.DialContext(dialCtx, socketPath, dialOptions()...)
}

func dialOptions() []grpc.DialOption {
	return []grpc.DialOption{
		grpc.WithBlock(), // With this we get retries. Without, connections fail fast.
		grpc.WithInsecure(),
		grpc.WithDialer(func(addr string, timeout time.Duration) (net.Conn, error) {
			return net.DialTimeout("unix", addr, timeout)
		}),
	}
}

// SetHeaders adds headers that tell gitaly-ruby the full path to the repository.
func SetHeaders(ctx context.Context, repo *pb.Repository) (context.Context, error) {
	repoPath, err := helper.GetRepoPath(repo)
	if err != nil {
		return nil, err
	}

	newCtx := metadata.NewOutgoingContext(ctx, metadata.Pairs(repoPathHeader, repoPath))
	return newCtx, nil
}

// Proxy calls recvSend until it receives an error. The error is returned
// to the caller unless it is io.EOF.
func Proxy(recvSend func() error) (err error) {
	for err == nil {
		err = recvSend()
	}

	if err == io.EOF {
		err = nil
	}
	return err
}
