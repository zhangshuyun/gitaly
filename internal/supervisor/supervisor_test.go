package supervisor

import (
	"context"
	"io/ioutil"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
)

var (
	testDir    string
	testExe    string
	socketPath string
)

func TestMain(m *testing.M) {
	os.Exit(testMain(m))
}

func testMain(m *testing.M) int {
	defer testhelper.MustHaveNoChildProcess()
	cleanup := testhelper.Configure()
	defer cleanup()

	var err error
	testDir, err = ioutil.TempDir("", "gitaly-supervisor-test")
	if err != nil {
		log.Error(err)
		return 1
	}
	defer os.RemoveAll(testDir)

	scriptPath, err := filepath.Abs("test-scripts/pid-server.go")
	if err != nil {
		log.Error(err)
		return 1
	}

	testExe = filepath.Join(testDir, "pid-server")
	buildCmd := exec.Command("go", "build", "-o", testExe, scriptPath)
	buildCmd.Dir = filepath.Dir(scriptPath)
	buildCmd.Stderr = os.Stderr
	buildCmd.Stdout = os.Stdout
	if err := buildCmd.Run(); err != nil {
		log.Error(err)
		return 1
	}

	socketPath = filepath.Join(testDir, "socket")

	return m.Run()
}

func TestRespawnAfterCrashWithoutCircuitBreaker(t *testing.T) {
	process, err := New(t.Name(), nil, []string{testExe}, testDir, 0, nil, nil)
	require.NoError(t, err)
	defer process.Stop()

	attempts := config.CrashThreshold
	require.True(t, attempts > 2, "config.CrashThreshold sanity check")

	pids, err := tryConnect(socketPath, attempts, 1*time.Second)
	require.NoError(t, err)

	require.Equal(t, attempts, len(pids), "number of pids should equal number of attempts")

	previous := 0
	for _, pid := range pids {
		require.True(t, pid > 0, "pid > 0")
		require.NotEqual(t, previous, pid, "pid sanity check")
		previous = pid
	}
}

func TestTooManyCrashes(t *testing.T) {
	process, err := New(t.Name(), nil, []string{testExe}, testDir, 0, nil, nil)
	require.NoError(t, err)
	defer process.Stop()

	attempts := config.CrashThreshold + 1
	require.True(t, attempts > 2, "config.CrashThreshold sanity check")

	pids, err := tryConnect(socketPath, attempts, 1*time.Second)
	require.Error(t, err, "circuit breaker should cause a connection error / timeout")

	require.Equal(t, config.CrashThreshold, len(pids), "number of pids should equal circuit breaker threshold")
}

func TestSpawnFailure(t *testing.T) {
	defer func(waitTime time.Duration) {
		config.CrashWaitTime = waitTime
	}(config.CrashWaitTime)

	config.CrashWaitTime = 2 * time.Second

	notFoundExe := filepath.Join(testDir, "not-found")
	require.NoError(t, os.RemoveAll(notFoundExe))
	defer os.Remove(notFoundExe)

	process, err := New(t.Name(), nil, []string{notFoundExe}, testDir, 0, nil, nil)
	require.NoError(t, err)
	defer process.Stop()

	time.Sleep(1 * time.Second)

	pids, err := tryConnect(socketPath, 1, 1*time.Millisecond)
	require.Error(t, err, "connection must fail because executable cannot be spawned")
	require.Equal(t, 0, len(pids))

	// 'Fix' the spawning problem of our process
	require.NoError(t, os.Symlink(testExe, notFoundExe))

	// After CrashWaitTime, the circuit breaker should have closed
	pids, err = tryConnect(socketPath, 1, config.CrashWaitTime)

	require.NoError(t, err, "process should be accepting connections now")
	require.Equal(t, 1, len(pids), "we should have received the pid of the new process")
	require.True(t, pids[0] > 0, "pid sanity check")
}

func tryConnect(socketPath string, attempts int, timeout time.Duration) (pids []int, err error) {
	ctx, cancel := testhelper.Context(testhelper.ContextWithTimeout(timeout))
	defer cancel()

	for j := 0; j < attempts; j++ {
		var curPid int
		for {
			curPid, err = getPid(ctx, socketPath)
			if err == nil {
				break
			}

			select {
			case <-ctx.Done():
				return pids, ctx.Err()
			case <-time.After(5 * time.Millisecond):
				// sleep
			}
		}
		if err != nil {
			return pids, err
		}

		pids = append(pids, curPid)
		if curPid > 0 {
			syscall.Kill(curPid, syscall.SIGKILL)
		}
	}

	return pids, err
}

func getPid(ctx context.Context, socket string) (int, error) {
	var err error
	var conn net.Conn

	for {
		conn, err = net.DialTimeout("unix", socket, 1*time.Millisecond)
		if err == nil {
			break
		}

		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(5 * time.Millisecond):
			// sleep
		}
	}
	if err != nil {
		return 0, err
	}
	defer conn.Close()

	response, err := ioutil.ReadAll(conn)
	if err != nil {
		return 0, err
	}

	return strconv.Atoi(string(response))
}
