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
		CrashWaitTime:  time.Minute,
		CrashResetTime: time.Minute,
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
		pid, ok := tryGetPid(t, eventCh, filepath.Join(tempDir, "socket"), 5*time.Second)
		require.True(t, ok)
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
		CrashWaitTime:  time.Minute,
		CrashResetTime: time.Minute,
	}

	eventCh := make(chan Event, 1)
	process, err := New(config, t.Name(), nil, []string{pidServer}, tempDir, 0, eventCh, nil)
	require.NoError(t, err)
	defer process.Stop()

	// Kill the service `CrashThreshold` times, which will cause it to not come up after the
	// last iteration.
	for i := 0; i < config.CrashThreshold; i++ {
		pid, ok := tryGetPid(t, eventCh, filepath.Join(tempDir, "socket"), 1*time.Second)
		require.True(t, ok)
		require.NoError(t, syscall.Kill(pid, syscall.SIGKILL))
	}

	// We should thus see the process not coming up here again given that there is a larger
	// timeout after we have reached the threshold.
	_, ok := tryGetPid(t, eventCh, filepath.Join(tempDir, "socket"), 1*time.Second)
	require.False(t, ok, "circuit breaker should cause a connection error / timeout")
}

func TestSupervisor_SpawnFailure(t *testing.T) {
	t.Parallel()

	pidServer := buildPidServer(t)
	tempDir := testhelper.TempDir(t)

	config := Config{
		CrashThreshold: 3,
		CrashWaitTime:  2 * time.Second,
		CrashResetTime: time.Minute,
	}

	notFoundExe := filepath.Join(tempDir, "not-found")

	// Spawn the supervisor with an executable that doesn't exist.
	eventCh := make(chan Event, 1)
	process, err := New(config, t.Name(), nil, []string{notFoundExe}, tempDir, 0, eventCh, nil)
	require.NoError(t, err)
	defer process.Stop()

	// Connecting to the service should thus obviously fail: there is nothing that the
	// supervisor could have spawned.
	_, ok := tryGetPid(t, eventCh, filepath.Join(tempDir, "socket"), 1*time.Second)
	require.False(t, ok, "connection must fail because executable cannot be spawned")

	// 'Fix' the spawning problem of our process by symlinking the PID server into place.
	require.NoError(t, os.Symlink(pidServer, notFoundExe))

	// So that we should now see the server to come up again after CrashWaitTime.
	_, ok = tryGetPid(t, eventCh, filepath.Join(tempDir, "socket"), config.CrashWaitTime)
	require.True(t, ok)
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
				cleanup := testhelper.ModifyEnvironment(t, key,
					value)
				defer cleanup()
			}

			config, err := NewConfigFromEnv()
			require.Equal(t, tc.expectedErr, err)
			require.Equal(t, tc.expectedConfig, config)
		})
	}
}

// tryGetPid will wait for the process to come up. If it does, then it connects to its service and
// retrieves the PID. Returns the PID and `true` in case the process came up, `false` otherwise.
func tryGetPid(t *testing.T, eventCh <-chan Event, socketPath string, timeout time.Duration) (int, bool) {
	t.Helper()

	ctx, cancel := testhelper.Context(testhelper.ContextWithTimeout(timeout))
	defer cancel()

	// We first wait for the process to come back up by waiting for its event notifcation. If
	// we do not receive an Up event, then we gracefully fail and notify the caller that the
	// supervisor did not respawn the executable.
	for {
		var up bool
		select {
		case event := <-eventCh:
			up = event.Type == Up
		case <-ctx.Done():
			return 0, false
		}

		if up {
			break
		}
	}

	// If we got here, then we know that the process must've come up. We thus require the
	// connection to it to succeed given that otherwise it would indicate that the running
	// process is somehow unable to serve requests, which is unexpected. Given that there is
	// still a race between the process coming up and it creating its socket, we must loop
	// around the connection.
	for {
		conn, err := net.DialTimeout("unix", socketPath, 1*time.Millisecond)
		if err != nil {
			select {
			case <-ctx.Done():
				require.FailNow(t, "process did not start to serve")
			case <-time.After(5 * time.Millisecond):
				continue
			}
		}
		defer testhelper.MustClose(t, conn)

		response, err := io.ReadAll(conn)
		require.NoError(t, err)

		pid, err := strconv.Atoi(string(response))
		require.NoError(t, err)
		require.NotZero(t, pid, "we should have received the pid of the new process")

		return pid, true
	}
}

func buildPidServer(t *testing.T) string {
	t.Helper()

	sourcePath, err := filepath.Abs("test-scripts/pid-server.go")
	require.NoError(t, err)

	return testhelper.BuildBinary(t, testhelper.TempDir(t), sourcePath)
}
