package command

import (
	"bufio"
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
)

func TestNewCommandTZEnv(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	oldTZ := os.Getenv("TZ")
	defer os.Setenv("TZ", oldTZ)

	os.Setenv("TZ", "foobar")

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

	New(context.Background(), exec.Command("true"), nil, nil, nil)
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

func TestCommandStdErr(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var stdout bytes.Buffer

	expectedMessage := `hello world\\nhello world\\nhello world\\nhello world\\nhello world\\n`

	r, w := io.Pipe()
	defer r.Close()
	defer w.Close()

	logger := logrus.New()
	logger.SetOutput(w)

	ctx = ctxlogrus.ToContext(ctx, logrus.NewEntry(logger))

	cmd, err := New(ctx, exec.Command("./testdata/stderr_script.sh"), nil, &stdout, nil)
	require.NoError(t, err)

	require.Error(t, cmd.Wait())
	assert.Empty(t, stdout.Bytes())

	b := bufio.NewReader(r)
	line, err := b.ReadString('\n')
	require.NoError(t, err)
	require.Equal(t, expectedMessage, extractMessage(line))
}

func TestCommandStdErrLargeOutput(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var stdout bytes.Buffer
	r, w := io.Pipe()
	defer r.Close()
	defer w.Close()

	logger := logrus.New()
	logger.SetOutput(w)

	ctx = ctxlogrus.ToContext(ctx, logrus.NewEntry(logger))

	cmd, err := New(ctx, exec.Command("./testdata/stderr_many_lines.sh"), nil, &stdout, nil)
	require.NoError(t, err)

	require.Error(t, cmd.Wait())
	assert.Empty(t, stdout.Bytes())

	b := bufio.NewReader(r)
	line, err := b.ReadString('\n')
	require.NoError(t, err)

	// the logrus printer prints with %q, so with an escaped newline it will add an extra \ escape to the
	// output. So for the test we can take out the extra \ since it was logrus that added it, not the command
	// https://github.com/sirupsen/logrus/blob/master/text_formatter.go#L324
	msg := strings.Replace(extractMessage(line), `\\n`, `\n`, -1)
	require.LessOrEqual(t, len(msg), MaxStderrBytes)
}

func TestCommandStdErrBinaryNullBytes(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var stdout bytes.Buffer

	r, w := io.Pipe()
	defer r.Close()
	defer w.Close()

	logger := logrus.New()
	logger.SetOutput(w)

	ctx = ctxlogrus.ToContext(ctx, logrus.NewEntry(logger))

	cmd, err := New(ctx, exec.Command("./testdata/stderr_binary_null.sh"), nil, &stdout, nil)
	require.NoError(t, err)

	require.Error(t, cmd.Wait())
	assert.Empty(t, stdout.Bytes())

	b := bufio.NewReader(r)
	line, err := b.ReadString('\n')
	require.NoError(t, err)
	require.NotEmpty(t, extractMessage(line))
}

func TestCommandStdErrLongLine(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var stdout bytes.Buffer
	r, w := io.Pipe()
	defer r.Close()
	defer w.Close()

	logger := logrus.New()
	logger.SetOutput(w)

	ctx = ctxlogrus.ToContext(ctx, logrus.NewEntry(logger))

	cmd, err := New(ctx, exec.Command("./testdata/stderr_repeat_a.sh"), nil, &stdout, nil)
	require.NoError(t, err)

	require.Error(t, cmd.Wait())
	assert.Empty(t, stdout.Bytes())

	b := bufio.NewReader(r)
	line, err := b.ReadString('\n')
	require.NoError(t, err)
	require.Contains(t, line, fmt.Sprintf(`%s\\n%s`, strings.Repeat("a", StderrBufferSize), strings.Repeat("b", StderrBufferSize)))
}

func TestCommandStdErrMaxBytes(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var stdout bytes.Buffer
	r, w := io.Pipe()
	defer r.Close()
	defer w.Close()

	logger := logrus.New()
	logger.SetOutput(w)

	ctx = ctxlogrus.ToContext(ctx, logrus.NewEntry(logger))

	cmd, err := New(ctx, exec.Command("./testdata/stderr_max_bytes_edge_case.sh"), nil, &stdout, nil)
	require.NoError(t, err)

	require.Error(t, cmd.Wait())
	assert.Empty(t, stdout.Bytes())

	b := bufio.NewReader(r)
	line, err := b.ReadString('\n')
	require.NoError(t, err)
	require.NotEmpty(t, extractMessage(line))
}

var logMsgRegex = regexp.MustCompile(`msg="(.+)"`)

func extractMessage(logMessage string) string {
	subMatches := logMsgRegex.FindStringSubmatch(logMessage)
	if len(subMatches) != 2 {
		return ""
	}

	return subMatches[1]
}
