package command

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestNewCommandTZEnv(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	oldTZ := os.Getenv("TZ")
	defer func() {
		require.NoError(t, os.Setenv("TZ", oldTZ))
	}()

	require.NoError(t, os.Setenv("TZ", "foobar"))

	buff := &bytes.Buffer{}
	cmd, err := New(ctx, exec.Command("env"), nil, buff, nil)

	require.NoError(t, err)
	require.NoError(t, cmd.Wait())

	require.Contains(t, strings.Split(buff.String(), "\n"), "TZ=foobar")
}

func TestNewCommandExtraEnv(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	extraVar := "FOOBAR=123456"
	buff := &bytes.Buffer{}
	cmd, err := New(ctx, exec.Command("/usr/bin/env"), nil, buff, nil, extraVar)

	require.NoError(t, err)
	require.NoError(t, cmd.Wait())

	require.Contains(t, strings.Split(buff.String(), "\n"), extraVar)
}

func TestNewCommandProxyEnv(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	testCases := []struct {
		key   string
		value string
	}{
		{
			key:   "all_proxy",
			value: "http://localhost:4000",
		},
		{
			key:   "http_proxy",
			value: "http://localhost:5000",
		},
		{
			key:   "HTTP_PROXY",
			value: "http://localhost:6000",
		},
		{
			key:   "https_proxy",
			value: "https://localhost:5000",
		},
		{
			key:   "HTTPS_PROXY",
			value: "https://localhost:6000",
		},
		{
			key:   "no_proxy",
			value: "https://excluded:5000",
		},
		{
			key:   "NO_PROXY",
			value: "https://excluded:5000",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.key, func(t *testing.T) {
			extraVar := fmt.Sprintf("%s=%s", tc.key, tc.value)
			buff := &bytes.Buffer{}
			cmd, err := New(ctx, exec.Command("/usr/bin/env"), nil, buff, nil, extraVar)

			require.NoError(t, err)
			require.NoError(t, cmd.Wait())

			require.Contains(t, strings.Split(buff.String(), "\n"), extraVar)
		})
	}
}

func TestRejectEmptyContextDone(t *testing.T) {
	defer func() {
		p := recover()
		if p == nil {
			t.Error("expected panic, got none")
			return
		}

		if _, ok := p.(contextWithoutDonePanic); !ok {
			panic(p)
		}
	}()

	_, err := New(context.Background(), exec.Command("true"), nil, nil, nil)
	require.NoError(t, err)
}

func TestNewCommandTimeout(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	defer func(ch chan struct{}, t time.Duration) {
		spawnTokens = ch
		spawnConfig.Timeout = t
	}(spawnTokens, spawnConfig.Timeout)

	// This unbuffered channel will behave like a full/blocked buffered channel.
	spawnTokens = make(chan struct{})
	// Speed up the test by lowering the timeout
	spawnTimeout := 200 * time.Millisecond
	spawnConfig.Timeout = spawnTimeout

	testDeadline := time.After(1 * time.Second)
	tick := time.After(spawnTimeout / 2)

	errCh := make(chan error)
	go func() {
		_, err := New(ctx, exec.Command("true"), nil, nil, nil)
		errCh <- err
	}()

	var err error
	timePassed := false

wait:
	for {
		select {
		case err = <-errCh:
			break wait
		case <-tick:
			timePassed = true
		case <-testDeadline:
			t.Fatal("test timed out")
		}
	}

	require.True(t, timePassed, "time must have passed")
	require.Error(t, err)
	require.Contains(t, err.Error(), "process spawn timed out after")
}

func TestCommand_Wait_interrupts_after_context_timeout(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ctx, timeout := context.WithTimeout(ctx, time.Second)
	defer timeout()

	cmd, err := New(ctx, exec.CommandContext(ctx, "sleep", "3"), nil, nil, nil)
	require.NoError(t, err)

	completed := make(chan error, 1)
	go func() { completed <- cmd.Wait() }()

	select {
	case err := <-completed:
		require.Error(t, err)
		s, ok := ExitStatus(err)
		require.True(t, ok)
		require.Equal(t, -1, s)
	case <-time.After(2 * time.Second):
		require.FailNow(t, "process is running too long")
	}
}

func TestNewCommandWithSetupStdin(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	value := "Test value"
	output := bytes.NewBuffer(nil)

	cmd, err := New(ctx, exec.Command("cat"), SetupStdin, nil, nil)
	require.NoError(t, err)

	_, err = fmt.Fprintf(cmd, "%s", value)
	require.NoError(t, err)

	// The output of the `cat` subprocess should exactly match its input
	_, err = io.CopyN(output, cmd, int64(len(value)))
	require.NoError(t, err)
	require.Equal(t, value, output.String())

	require.NoError(t, cmd.Wait())
}

func TestNewCommandNullInArg(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, err := New(ctx, exec.Command("sh", "-c", "hello\x00world"), nil, nil, nil)
	require.Error(t, err)
	require.EqualError(t, err, `detected null byte in command argument "hello\x00world"`)
}

func TestNewNonExistent(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cmd, err := New(ctx, exec.Command("command-non-existent"), nil, nil, nil)
	require.Nil(t, cmd)
	require.Error(t, err)
}

func TestCommandStdErr(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var stdout, stderr bytes.Buffer
	expectedMessage := `hello world\nhello world\nhello world\nhello world\nhello world\n`

	logger := logrus.New()
	logger.SetOutput(&stderr)

	ctx = ctxlogrus.ToContext(ctx, logrus.NewEntry(logger))

	cmd, err := New(ctx, exec.Command("./testdata/stderr_script.sh"), nil, &stdout, nil)
	require.NoError(t, err)
	require.Error(t, cmd.Wait())

	assert.Empty(t, stdout.Bytes())
	require.Equal(t, expectedMessage, extractMessage(stderr.String()))
}

func TestCommandStdErrLargeOutput(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var stdout, stderr bytes.Buffer

	logger := logrus.New()
	logger.SetOutput(&stderr)

	ctx = ctxlogrus.ToContext(ctx, logrus.NewEntry(logger))

	cmd, err := New(ctx, exec.Command("./testdata/stderr_many_lines.sh"), nil, &stdout, nil)
	require.NoError(t, err)
	require.Error(t, cmd.Wait())

	assert.Empty(t, stdout.Bytes())
	msg := strings.ReplaceAll(extractMessage(stderr.String()), "\\n", "\n")
	require.LessOrEqual(t, len(msg), maxStderrBytes)
}

func TestCommandStdErrBinaryNullBytes(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var stdout, stderr bytes.Buffer

	logger := logrus.New()
	logger.SetOutput(&stderr)

	ctx = ctxlogrus.ToContext(ctx, logrus.NewEntry(logger))

	cmd, err := New(ctx, exec.Command("./testdata/stderr_binary_null.sh"), nil, &stdout, nil)
	require.NoError(t, err)
	require.Error(t, cmd.Wait())

	assert.Empty(t, stdout.Bytes())
	msg := strings.SplitN(extractMessage(stderr.String()), "\\n", 2)[0]
	require.Equal(t, strings.Repeat("\\x00", maxStderrLineLength), msg)
}

func TestCommandStdErrLongLine(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var stdout, stderr bytes.Buffer

	logger := logrus.New()
	logger.SetOutput(&stderr)

	ctx = ctxlogrus.ToContext(ctx, logrus.NewEntry(logger))

	cmd, err := New(ctx, exec.Command("./testdata/stderr_repeat_a.sh"), nil, &stdout, nil)
	require.NoError(t, err)
	require.Error(t, cmd.Wait())

	assert.Empty(t, stdout.Bytes())
	require.Contains(t, stderr.String(), fmt.Sprintf("%s\\n%s", strings.Repeat("a", maxStderrLineLength), strings.Repeat("b", maxStderrLineLength)))
}

func TestCommandStdErrMaxBytes(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var stdout, stderr bytes.Buffer

	logger := logrus.New()
	logger.SetOutput(&stderr)

	ctx = ctxlogrus.ToContext(ctx, logrus.NewEntry(logger))

	cmd, err := New(ctx, exec.Command("./testdata/stderr_max_bytes_edge_case.sh"), nil, &stdout, nil)
	require.NoError(t, err)
	require.Error(t, cmd.Wait())

	assert.Empty(t, stdout.Bytes())
	require.Equal(t, maxStderrBytes, len(strings.ReplaceAll(extractMessage(stderr.String()), "\\n", "\n")))
}

var logMsgRegex = regexp.MustCompile(`msg="(.+?)"`)

func extractMessage(logMessage string) string {
	subMatches := logMsgRegex.FindStringSubmatch(logMessage)
	if len(subMatches) != 2 {
		return ""
	}

	return subMatches[1]
}

func TestUncancellableContext(t *testing.T) {
	t.Run("cancellation", func(t *testing.T) {
		parent, cancel := context.WithCancel(context.Background())
		ctx := SuppressCancellation(parent)

		cancel()
		require.Equal(t, context.Canceled, parent.Err(), "sanity check: context should be cancelled")

		require.Nil(t, ctx.Err(), "cancellation of the parent shouldn't propagate via Err")
		select {
		case <-ctx.Done():
			require.FailNow(t, "cancellation of the parent shouldn't propagate via Done")
		default:
		}
	})

	t.Run("timeout", func(t *testing.T) {
		parent, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
		defer cancel()

		ctx := SuppressCancellation(parent)

		time.Sleep(time.Millisecond)
		require.Equal(t, context.DeadlineExceeded, parent.Err(), "sanity check: context should be expired after awaiting")

		require.Nil(t, ctx.Err(), "timeout on the parent shouldn't propagate via Err")
		select {
		case <-ctx.Done():
			require.FailNow(t, "timeout on the parent shouldn't propagate via Done")
		default:
		}
		_, ok := ctx.Deadline()
		require.False(t, ok, "no deadline should be set")
	})

	t.Run("re-cancellation", func(t *testing.T) {
		parent, cancelParent := context.WithCancel(context.Background())
		ctx := SuppressCancellation(parent)
		child, cancelChild := context.WithCancel(ctx)
		defer cancelChild()

		cancelParent()
		select {
		case <-child.Done():
			require.FailNow(t, "uncancellable context should suppress cancellation on the parent")
		default:
			// all good
		}

		cancelChild()
		require.Equal(t, context.Canceled, child.Err(), "context derived from cancellable could be cancelled")

		select {
		case <-child.Done():
			// all good
		default:
			require.FailNow(t, "child context should be canceled despite if parent is uncancellable")
		}
	})

	t.Run("context values are preserved", func(t *testing.T) {
		type ctxKey string
		k1 := ctxKey("1")
		k2 := ctxKey("2")

		parent, cancel := context.WithCancel(context.Background())
		defer cancel()

		parent = context.WithValue(parent, k1, 1)
		parent = context.WithValue(parent, k2, "two")

		ctx := SuppressCancellation(parent)

		require.Equal(t, 1, ctx.Value(k1))
		require.Equal(t, "two", ctx.Value(k2))

		cancel()
		require.Equal(t, context.Canceled, parent.Err(), "sanity check: context should be cancelled")

		require.Equal(t, 1, ctx.Value(k1), "should be accessible after parent context cancellation")
		require.Equal(t, "two", ctx.Value(k2))
	})
}
