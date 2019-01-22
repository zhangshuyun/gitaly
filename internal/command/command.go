package command

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"sync"
	"syscall"
	"time"

	grpc_logrus "github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus"
	log "github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/internal/config"
)

// GitEnv contains the ENV variables for git commands
var GitEnv = []string{
	// Force english locale for consistency on the output messages
	"LANG=en_US.UTF-8",
}

// exportedEnvVars contains a list of environment variables
// that are always exported to child processes on spawn
var exportedEnvVars = []string{
	"HOME",
	"PATH",
	"LD_LIBRARY_PATH",
	"TZ",

	// Export git tracing variables for easier debugging
	"GIT_TRACE",
	"GIT_TRACE_PACK_ACCESS",
	"GIT_TRACE_PACKET",
	"GIT_TRACE_PERFORMANCE",
	"GIT_TRACE_SETUP",

	// Git HTTP proxy settings: https://git-scm.com/docs/git-config#git-config-httpproxy
	"all_proxy",
	"http_proxy",
	"HTTP_PROXY",
	"https_proxy",
	"HTTPS_PROXY",
	// libcurl settings: https://curl.haxx.se/libcurl/c/CURLOPT_NOPROXY.html
	"no_proxy",
}

// Command encapsulates a running exec.Cmd. The embedded exec.Cmd is
// terminated and reaped automatically when the context.Context that
// created it is canceled.
type Command struct {
	reader       io.Reader
	logrusWriter io.WriteCloser
	cmd          *exec.Cmd
	context      context.Context
	startTime    time.Time

	waitError error
	waitOnce  sync.Once
}

// Read calls Read() on the stdout pipe of the command.
func (c *Command) Read(p []byte) (int, error) {
	if c.reader == nil {
		panic("command has no reader")
	}

	return c.reader.Read(p)
}

// Wait calls Wait() on the exec.Cmd instance inside the command. This
// blocks until the command has finished and reports the command exit
// status via the error return value. Use ExitStatus to get the integer
// exit status from the error returned by Wait().
func (c *Command) Wait() error {
	c.waitOnce.Do(c.wait)

	return c.waitError
}

// GitPath returns the path to the `git` binary. See `SetGitPath` for details
// on how this is set
func GitPath() string {
	if config.Config.Git.BinPath == "" {
		// This shouldn't happen outside of testing, SetGitPath should be called by
		// main.go to ensure correctness of the configuration on start-up.
		if err := config.SetGitPath(); err != nil {
			log.Fatal(err) // Bail out.
		}
	}

	return config.Config.Git.BinPath
}

var wg = &sync.WaitGroup{}

// WaitAllDone waits for all commands started by the command package to
// finish. This can only be called once in the lifecycle of the current
// Go process.
func WaitAllDone() {
	wg.Wait()
}

type spawnTimeoutError error
type contextWithoutDonePanic string

// New creates a Command from an exec.Cmd. On success, the Command
// contains a running subprocess. When ctx is canceled the embedded
// process will be terminated and reaped automatically.
func New(ctx context.Context, cmd *exec.Cmd, stdin io.Reader, stdout, stderr io.Writer, env ...string) (*Command, error) {
	if ctx.Done() == nil {
		panic(contextWithoutDonePanic("command spawned with context without Done() channel"))
	}

	putToken, err := getSpawnToken(ctx)
	if err != nil {
		return nil, err
	}
	defer putToken()

	logPid := -1
	defer func() {
		grpc_logrus.Extract(ctx).WithFields(log.Fields{
			"pid":  logPid,
			"path": cmd.Path,
			"args": cmd.Args,
		}).Debug("spawn")
	}()

	command := &Command{
		cmd:       cmd,
		startTime: time.Now(),
		context:   ctx,
	}

	// Explicitly set the environment for the command
	env = append(env, "GIT_TERMINAL_PROMPT=0")

	// Export env vars
	cmd.Env = exportEnvironment(env)

	// Start the command in its own process group (nice for signalling)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	if stdin != nil {
		cmd.Stdin = stdin
	}

	if stdout != nil {
		// We don't assign a reader if an stdout override was passed. We assume
		// output is going to be directly handled by the caller.
		cmd.Stdout = stdout
	} else {
		pipe, err := cmd.StdoutPipe()
		if err != nil {
			return nil, fmt.Errorf("GitCommand: stdout: %v", err)
		}
		command.reader = pipe
	}

	if stderr != nil {
		cmd.Stderr = stderr
	} else {
		// If we don't do something with cmd.Stderr, Git errors will be lost
		command.logrusWriter = grpc_logrus.Extract(ctx).WriterLevel(log.InfoLevel)
		cmd.Stderr = command.logrusWriter
	}

	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("GitCommand: start %v: %v", cmd.Args, err)
	}

	// The goroutine below is responsible for terminating and reaping the
	// process when ctx is canceled.
	wg.Add(1)
	go func() {
		<-ctx.Done()

		if process := cmd.Process; process != nil && process.Pid > 0 {
			// Send SIGTERM to the process group of cmd
			syscall.Kill(-process.Pid, syscall.SIGTERM)
		}
		command.Wait()
		wg.Done()
	}()

	logPid = cmd.Process.Pid

	return command, nil
}

func exportEnvironment(env []string) []string {
	for _, envVarName := range exportedEnvVars {
		if val, ok := os.LookupEnv(envVarName); ok {
			env = append(env, fmt.Sprintf("%s=%s", envVarName, val))
		}
	}

	return env
}

// This function should never be called directly, use Wait().
func (c *Command) wait() {
	if c.reader != nil {
		// Prevent the command from blocking on writing to its stdout.
		io.Copy(ioutil.Discard, c.reader)
	}

	c.waitError = c.cmd.Wait()

	exitCode := 0
	if c.waitError != nil {
		if exitStatus, ok := ExitStatus(c.waitError); ok {
			exitCode = exitStatus
		}
	}

	c.logProcessComplete(c.context, exitCode)

	if w := c.logrusWriter; w != nil {
		// Closing this writer lets a logrus goroutine finish early
		w.Close()
	}
}

// ExitStatus will return the exit-code from an error returned by Wait().
func ExitStatus(err error) (int, bool) {
	exitError, ok := err.(*exec.ExitError)
	if !ok {
		return 0, false
	}

	waitStatus, ok := exitError.Sys().(syscall.WaitStatus)
	if !ok {
		return 0, false
	}

	return waitStatus.ExitStatus(), true
}

func (c *Command) logProcessComplete(ctx context.Context, exitCode int) {
	cmd := c.cmd

	systemTime := cmd.ProcessState.SystemTime()
	userTime := cmd.ProcessState.UserTime()
	realTime := time.Since(c.startTime)

	entry := grpc_logrus.Extract(ctx).WithFields(log.Fields{
		"pid":                    cmd.ProcessState.Pid(),
		"path":                   cmd.Path,
		"args":                   cmd.Args,
		"command.exitCode":       exitCode,
		"command.system_time_ms": systemTime.Seconds() * 1000,
		"command.user_time_ms":   userTime.Seconds() * 1000,
		"command.real_time_ms":   realTime.Seconds() * 1000,
	})

	if rusage, ok := cmd.ProcessState.SysUsage().(*syscall.Rusage); ok {
		entry = entry.WithFields(log.Fields{
			"command.maxrss":  rusage.Maxrss,
			"command.inblock": rusage.Inblock,
			"command.oublock": rusage.Oublock,
		})
	}

	entry.Debug("spawn complete")
}
