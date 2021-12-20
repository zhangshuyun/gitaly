package testserver

import (
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/pelletier/go-toml"
	"github.com/stretchr/testify/require"
	gitalycfg "gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testcfg"
)

// praefectSpawnTokens limits the number of concurrent Praefect instances we spawn. With parallel
// tests, it can happen that we otherwise would spawn so many Praefect executables, with two
// consequences: first, they eat up all the hosts' memory. Second, they start to saturate Postgres
// such that new connections start to fail becaue of too many clients. The limit of concurrent
// instances is not scientifically chosen, but is picked such that tests do not fail on my machine
// anymore.
//
// Note that this only limits concurrency for a single package. If you test multiple packages at
// once, then these would also run concurrently, leading to `16 * len(packages)` concurrent Praefect
// instances. To limit this, you can run `go test -p $n` to test at most `$n` concurrent packages.
var praefectSpawnTokens = make(chan struct{}, 16)

// PraefectServer encapsulates information of a running Praefect server.
type PraefectServer struct {
	address  string
	shutdown func()
}

// Address is the address of the Praefect server.
func (ps PraefectServer) Address() string {
	return ps.address
}

// Shutdown shuts the Praefect server down. This function is synchronous and waits for the server to
// exit.
func (ps PraefectServer) Shutdown() {
	ps.shutdown()
}

// StartPraefect creates and runs a Praefect proxy. This server is created by running the external
// Praefect executable.
func StartPraefect(t testing.TB, cfg config.Config) PraefectServer {
	t.Helper()

	praefectSpawnTokens <- struct{}{}
	t.Cleanup(func() {
		<-praefectSpawnTokens
	})

	// We're precreating the Unix socket which we pass to Praefect. This closes a race where
	// the Unix socket didn't yet exist when we tried to dial the Praefect server.
	praefectServerSocket, err := net.Listen("unix", cfg.SocketPath)
	require.NoError(t, err)
	testhelper.MustClose(t, praefectServerSocket)
	t.Cleanup(func() { require.NoError(t, os.RemoveAll(praefectServerSocket.Addr().String())) })

	tempDir := testhelper.TempDir(t)

	configFilePath := filepath.Join(tempDir, "config.toml")
	configFile, err := os.Create(configFilePath)
	require.NoError(t, err)
	defer testhelper.MustClose(t, configFile)

	require.NoError(t, toml.NewEncoder(configFile).Encode(&cfg))
	require.NoError(t, configFile.Sync())

	binaryPath := testcfg.BuildPraefect(t, gitalycfg.Cfg{
		BinDir: tempDir,
	})

	cmd := exec.Command(binaryPath, "-config", configFilePath)
	cmd.Stderr = os.Stderr
	cmd.Stdout = os.Stdout

	require.NoError(t, cmd.Start())

	praefectServer := PraefectServer{
		address: "unix://" + praefectServerSocket.Addr().String(),
		shutdown: func() {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		},
	}
	t.Cleanup(praefectServer.Shutdown)

	waitHealthy(t, praefectServer.Address(), cfg.Auth.Token)

	return praefectServer
}
