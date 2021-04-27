package command

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"github.com/opentracing/opentracing-go"
	"github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/labkit/tracing"
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
	"NO_PROXY",
}

var envInjector = tracing.NewEnvInjector()

const (
	// maxStderrBytes is at most how many bytes will be written to stderr
	maxStderrBytes = 10000 // 10kb
	// maxStderrLineLength is at most how many bytes a single line will be
	// written to stderr. Lines exceeding this limit should be truncated
	maxStderrLineLength = 4096
)

// Command encapsulates a running exec.Cmd. The embedded exec.Cmd is
// terminated and reaped automatically when the context.Context that
// created it is canceled.
type Command struct {
	reader       io.Reader
	writer       io.WriteCloser
	stderrBuffer *stderrBuffer
	cmd          *exec.Cmd
	context      context.Context
	startTime    time.Time

	waitError error
	waitOnce  sync.Once

	span opentracing.Span
}

type stdinSentinel struct{}

func (stdinSentinel) Read([]byte) (int, error) {
	return 0, errors.New("stdin sentinel should not be read from")
}

// SetupStdin instructs New() to configure the stdin pipe of the command it is
// creating. This allows you call Write() on the command as if it is an ordinary
// io.Writer, sending data directly to the stdin of the process.
//
// You should not call Read() on this value - it is strictly for configuration!
var SetupStdin io.Reader = stdinSentinel{}

// Read calls Read() on the stdout pipe of the command.
func (c *Command) Read(p []byte) (int, error) {
	if c.reader == nil {
		panic("command has no reader")
	}

	return c.reader.Read(p)
}

// Write calls Write() on the stdin pipe of the command.
func (c *Command) Write(p []byte) (int, error) {
	if c.writer == nil {
		panic("command has no writer")
	}

	return c.writer.Write(p)
}

// Wait calls Wait() on the exec.Cmd instance inside the command. This
// blocks until the command has finished and reports the command exit
// status via the error return value. Use ExitStatus to get the integer
// exit status from the error returned by Wait().
func (c *Command) Wait() error {
	c.waitOnce.Do(c.wait)

	return c.waitError
}

var wg = &sync.WaitGroup{}

// WaitAllDone waits for all commands started by the command package to
// finish. This can only be called once in the lifecycle of the current
// Go process.
func WaitAllDone() {
	wg.Wait()
}

type contextWithoutDonePanic string

// New creates a Command from an exec.Cmd. On success, the Command
// contains a running subprocess. When ctx is canceled the embedded
// process will be terminated and reaped automatically.
//
// If stdin is specified as SetupStdin, you will be able to write to the stdin
// of the subprocess by calling Write() on the returned Command.
func New(ctx context.Context, cmd *exec.Cmd, stdin io.Reader, stdout, stderr io.Writer, env ...string) (*Command, error) {
	if ctx.Done() == nil {
		panic(contextWithoutDonePanic("command spawned with context without Done() channel"))
	}

	if err := checkNullArgv(cmd); err != nil {
		return nil, err
	}

	span, ctx := opentracing.StartSpanFromContext(
		ctx,
		cmd.Path,
		opentracing.Tag{Key: "args", Value: strings.Join(cmd.Args, " ")},
	)

	putToken, err := getSpawnToken(ctx)
	if err != nil {
		return nil, err
	}
	defer putToken()

	logPid := -1
	defer func() {
		ctxlogrus.Extract(ctx).WithFields(logrus.Fields{
			"pid":  logPid,
			"path": cmd.Path,
			"args": cmd.Args,
		}).Debug("spawn")
	}()

	command := &Command{
		cmd:       cmd,
		startTime: time.Now(),
		context:   ctx,
		span:      span,
	}

	// Explicitly set the environment for the command
	env = append(env, "GIT_TERMINAL_PROMPT=0")

	// Export env vars
	cmd.Env = append(env, AllowedEnvironment(os.Environ())...)
	cmd.Env = envInjector(ctx, cmd.Env)

	// Start the command in its own process group (nice for signalling)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	// Three possible values for stdin:
	//   * nil - Go implicitly uses /dev/null
	//   * SetupStdin - configure with cmd.StdinPipe(), allowing Write() to work
	//   * Another io.Reader - becomes cmd.Stdin. Write() will not work
	if stdin == SetupStdin {
		pipe, err := cmd.StdinPipe()
		if err != nil {
			return nil, fmt.Errorf("GitCommand: stdin: %v", err)
		}
		command.writer = pipe
	} else if stdin != nil {
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
		command.stderrBuffer, err = newStderrBuffer(maxStderrBytes, maxStderrLineLength, []byte("\n"))
		if err != nil {
			return nil, fmt.Errorf("GitCommand: failed to create stderr buffer: %v", err)
		}
		cmd.Stderr = command.stderrBuffer
	}

	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("GitCommand: start %v: %v", cmd.Args, err)
	}
	inFlightCommandGauge.Inc()

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

// AllowedEnvironment filters the given slice of environment variables and
// returns all variables which are allowed per the variables defined above.
// This is useful for constructing a base environment in which a command can be
// run.
func AllowedEnvironment(envs []string) []string {
	var filtered []string

	for _, env := range envs {
		for _, exportedEnv := range exportedEnvVars {
			if strings.HasPrefix(env, exportedEnv+"=") {
				filtered = append(filtered, env)
			}
		}
	}

	return filtered
}

// This function should never be called directly, use Wait().
func (c *Command) wait() {
	if c.writer != nil {
		// Prevent the command from blocking on waiting for stdin to be closed
		c.writer.Close()
	}

	if c.reader != nil {
		// Prevent the command from blocking on writing to its stdout.
		io.Copy(ioutil.Discard, c.reader)
	}

	c.waitError = c.cmd.Wait()

	inFlightCommandGauge.Dec()

	c.logProcessComplete()
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

func (c *Command) logProcessComplete() {
	exitCode := 0
	if c.waitError != nil {
		if exitStatus, ok := ExitStatus(c.waitError); ok {
			exitCode = exitStatus
		}
	}

	ctx := c.context
	cmd := c.cmd

	systemTime := cmd.ProcessState.SystemTime()
	userTime := cmd.ProcessState.UserTime()
	realTime := time.Since(c.startTime)

	entry := ctxlogrus.Extract(ctx).WithFields(logrus.Fields{
		"pid":                    cmd.ProcessState.Pid(),
		"path":                   cmd.Path,
		"args":                   cmd.Args,
		"command.exitCode":       exitCode,
		"command.system_time_ms": systemTime.Seconds() * 1000,
		"command.user_time_ms":   userTime.Seconds() * 1000,
		"command.real_time_ms":   realTime.Seconds() * 1000,
	})

	rusage, ok := cmd.ProcessState.SysUsage().(*syscall.Rusage)
	if ok {
		entry = entry.WithFields(logrus.Fields{
			"command.maxrss":  rusage.Maxrss,
			"command.inblock": rusage.Inblock,
			"command.oublock": rusage.Oublock,
		})
	}

	entry.Debug("spawn complete")
	if c.stderrBuffer != nil && c.stderrBuffer.Len() > 0 {
		entry.Error(c.stderrBuffer.String())
	}

	if stats := StatsFromContext(ctx); stats != nil && featureflag.IsEnabled(ctx, featureflag.LogCommandStats) {
		stats.RecordSum("command.count", 1)
		stats.RecordSum("command.system_time_ms", int(systemTime.Seconds()*1000))
		stats.RecordSum("command.user_time_ms", int(userTime.Seconds()*1000))
		stats.RecordSum("command.real_time_ms", int(realTime.Seconds()*1000))

		if ok {
			stats.RecordMax("command.maxrss", int(rusage.Maxrss))
			stats.RecordSum("command.inblock", int(rusage.Inblock))
			stats.RecordSum("command.oublock", int(rusage.Oublock))
			stats.RecordSum("command.minflt", int(rusage.Minflt))
			stats.RecordSum("command.majflt", int(rusage.Majflt))
		}
	}

	c.span.LogKV(
		"pid", cmd.ProcessState.Pid(),
		"exit_code", exitCode,
		"system_time_ms", int(systemTime.Seconds()*1000),
		"user_time_ms", int(userTime.Seconds()*1000),
		"real_time_ms", int(realTime.Seconds()*1000),
	)
	if ok {
		c.span.LogKV(
			"maxrss", rusage.Maxrss,
			"inblock", rusage.Inblock,
			"oublock", rusage.Oublock,
			"minflt", rusage.Minflt,
			"majflt", rusage.Majflt,
		)
	}
	c.span.Finish()
}

// Command arguments will be passed to the exec syscall as
// null-terminated C strings. That means the arguments themselves may not
// contain a null byte. The go stdlib checks for null bytes but it
// returns a cryptic error. This function returns a more explicit error.
func checkNullArgv(cmd *exec.Cmd) error {
	for _, arg := range cmd.Args {
		if strings.IndexByte(arg, 0) > -1 {
			// Use %q so that the null byte gets printed as \x00
			return fmt.Errorf("detected null byte in command argument %q", arg)
		}
	}

	return nil
}

// Args is an accessor for the command arguments
func (c *Command) Args() []string {
	return c.cmd.Args
}

// Env is an accessor for the environment variables
func (c *Command) Env() []string {
	return c.cmd.Env
}

// Pid is an accessor for the pid
func (c *Command) Pid() int {
	return c.cmd.Process.Pid
}

// suppressedContext suppresses cancellation or expiration of the context.
type suppressedContext struct{ context.Context }

func (suppressedContext) Deadline() (deadline time.Time, ok bool) { return time.Time{}, false }

func (suppressedContext) Done() <-chan struct{} { return nil }

func (suppressedContext) Err() error { return nil }

// SuppressCancellation returns a context that suppresses cancellation or expiration of the parent context.
func SuppressCancellation(ctx context.Context) context.Context { return suppressedContext{ctx} }
