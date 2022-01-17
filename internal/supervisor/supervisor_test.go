package supervisor

import (
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/kelseyhightower/envconfig"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testcfg"
)

func TestMain(m *testing.M) {
	testhelper.Run(m)
}

func TestSupervisor_RespawnAfterCrashWithoutCircuitBreaker(t *testing.T) {
	t.Parallel()

	pidServer := buildPidServer(t)
	tempDir := testhelper.TempDir(t)

	config := Config{
		CrashThreshold: 3,
		CrashWaitTime:  time.Hour,
		CrashResetTime: time.Hour,
	}

	eventCh := make(chan Event, 1)
	process, err := New(config, t.Name(), nil, []string{pidServer}, tempDir, 0, eventCh, nil)
	require.NoError(t, err)
	defer process.Stop()

	// We're trying to connect to the service as fast as possible, killing it each time after we
	// have successfully connected to it. We should see it coming up fast again because it needs
	// to respawn less often than the CrashThreshold.
	var pids []int
	for i := 0; i < config.CrashThreshold; i++ {
		pid := waitForProcess(t, eventCh, filepath.Join(tempDir, "socket"))
		require.NoError(t, syscall.Kill(pid, syscall.SIGKILL))
		pids = append(pids, pid)
	}

	previous := 0
	for _, pid := range pids {
		require.NotEqual(t, previous, pid, "pid sanity check")
		previous = pid
	}
}

func TestSupervisor_TooManyCrashes(t *testing.T) {
	t.Parallel()

	pidServer := buildPidServer(t)
	tempDir := testhelper.TempDir(t)

	config := Config{
		CrashThreshold: 3,
		CrashWaitTime:  time.Second,
		CrashResetTime: time.Hour,
	}

	eventCh := make(chan Event, 1)
	process, err := New(config, t.Name(), nil, []string{pidServer}, tempDir, 0, eventCh, nil)
	require.NoError(t, err)
	defer process.Stop()

	// Kill the service `CrashThreshold` times, which will cause it to not come up after the
	// last iteration.
	var killTime time.Time
	for i := 0; i < config.CrashThreshold; i++ {
		pid := waitForProcess(t, eventCh, filepath.Join(tempDir, "socket"))

		// Record the kill time such that we can assert that the process spawned after the
		// circuit breaker has triggered waited for at least `CrashWaitTime`.
		killTime = time.Now()

		require.NoError(t, syscall.Kill(pid, syscall.SIGKILL))
	}

	// Connecting to the process should work after the circuit breaker has kicked in, but it
	// should take at least `CrashWaitTime` time.
	waitForProcess(t, eventCh, filepath.Join(tempDir, "socket"))

	require.True(t, time.Now().After(killTime.Add(config.CrashWaitTime)), "circuit breaker should have delayed start")
}

func TestSupervisor_SpawnFailure(t *testing.T) {
	t.Parallel()

	pidServer := buildPidServer(t)
	tempDir := testhelper.TempDir(t)

	config := Config{
		CrashThreshold: 3,
		CrashWaitTime:  time.Second,
		CrashResetTime: time.Hour,
	}

	notFoundExe := filepath.Join(tempDir, "not-found")

	// Spawn the supervisor with an executable that doesn't exist.
	eventCh := make(chan Event, config.CrashThreshold+1)
	process, err := New(config, t.Name(), nil, []string{notFoundExe}, tempDir, 0, eventCh, nil)
	require.NoError(t, err)
	defer process.Stop()

	// We should observe multiple crashes now given that the executable cannot be found.
	crashes := 0
	for event := range eventCh {
		if event.Type == Crash {
			crashes++
		}

		if crashes == config.CrashThreshold {
			break
		}
	}

	// 'Fix' the spawning problem of our process by symlinking the PID server into place.
	require.NoError(t, os.Symlink(pidServer, notFoundExe))

	// So that we should now see the server to come up again.
	waitForProcess(t, eventCh, filepath.Join(tempDir, "socket"))
}

func TestNewConfigFromEnv(t *testing.T) {
	for _, tc := range []struct {
		desc           string
		envvars        map[string]string
		expectedErr    error
		expectedConfig Config
	}{
		{
			desc: "default value",
			expectedConfig: Config{
				CrashThreshold: 5,
				CrashWaitTime:  time.Minute,
				CrashResetTime: time.Minute,
			},
		},
		{
			desc: "valid configuration",
			envvars: map[string]string{
				"GITALY_SUPERVISOR_CRASH_THRESHOLD":  "9000",
				"GITALY_SUPERVISOR_CRASH_WAIT_TIME":  "12s",
				"GITALY_SUPERVISOR_CRASH_RESET_TIME": "12h",
			},
			expectedConfig: Config{
				CrashThreshold: 9000,
				CrashWaitTime:  12 * time.Second,
				CrashResetTime: 12 * time.Hour,
			},
		},
		{
			desc: "invalid configuration",
			envvars: map[string]string{
				"GITALY_SUPERVISOR_CRASH_THRESHOLD": "something",
			},
			expectedErr: &envconfig.ParseError{
				KeyName:   "GITALY_SUPERVISOR_CRASH_THRESHOLD",
				FieldName: "CrashThreshold",
				TypeName:  "int",
				Value:     "something",
				Err: &strconv.NumError{
					Func: "ParseInt",
					Num:  "something",
					Err:  errors.New("invalid syntax"),
				},
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			for key, value := range tc.envvars {
				testhelper.ModifyEnvironment(t, key, value)
			}

			config, err := NewConfigFromEnv()
			require.Equal(t, tc.expectedErr, err)
			require.Equal(t, tc.expectedConfig, config)
		})
	}
}

// waitForProcess will wait for the process to come up. If it does, then it connects to its service
// and retrieves the PID. Returns the PID if successful, fails the test if not.
func waitForProcess(t *testing.T, eventCh <-chan Event, socketPath string) int {
	t.Helper()

	// We first wait for the process to come back up by waiting for its event notifcation.
	for {
		event := <-eventCh
		if event.Type == Up {
			break
		}
	}

	// Even if the service is up, we still need to wait for the socket to be created.
	var conn net.Conn
	require.Eventually(t, func() bool {
		var err error
		conn, err = net.Dial("unix", socketPath)
		return err == nil
	}, time.Minute, 10*time.Millisecond)

	defer testhelper.MustClose(t, conn)

	response, err := io.ReadAll(conn)
	require.NoError(t, err)

	pid, err := strconv.Atoi(string(response))
	require.NoError(t, err)
	require.NotZero(t, pid, "we should have received the pid of the new process")

	return pid
}

func buildPidServer(t *testing.T) string {
	t.Helper()

	sourcePath, err := filepath.Abs("test-scripts/pid-server.go")
	require.NoError(t, err)

	return testcfg.BuildBinary(t, testhelper.TempDir(t), sourcePath)
}
