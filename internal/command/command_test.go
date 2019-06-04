package command

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

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

	_, ok := err.(spawnTimeoutError)
	require.True(t, ok, "type of error should be spawnTimeoutError")
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

	_, ok := err.(nullInArgvError)
	require.True(t, ok, "expected %+v to be nullInArgvError", err)
}

func TestCommandStdErr(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var stdout, stderr bytes.Buffer

	cmd, err := New(ctx, exec.Command("./testdata/stderr_script.sh"), nil, &stdout, &stderr)
	require.NoError(t, err)

	require.Error(t, cmd.Wait())
	assert.Empty(t, stdout.Bytes())
	assert.Equal(t, `hello world\nhello world\nhello world\nhello world\nhello world\n`, stderr.String())
}

func TestCommandStdErrLargeOutput(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var stdout, stderr bytes.Buffer

	cmd, err := New(ctx, exec.Command("./testdata/stderr_many_lines.sh"), nil, &stdout, &stderr)
	require.NoError(t, err)

	require.Error(t, cmd.Wait())
	assert.Empty(t, stdout.Bytes())
	assert.True(t, stderr.Len() <= MaxStderrBytes)
}

func TestCommandStdErrBinaryNullBytes(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var stdout, stderr bytes.Buffer

	cmd, err := New(ctx, exec.Command("./testdata/stderr_binary_null.sh"), nil, &stdout, &stderr)
	require.NoError(t, err)

	require.Error(t, cmd.Wait())
	assert.Empty(t, stdout.Bytes())
	assert.NotEmpty(t, stderr.Bytes())
}

func TestCommandStdErrLongLine(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var stdout, stderr bytes.Buffer

	cmd, err := New(ctx, exec.Command("./testdata/stderr_repeat_a.sh"), nil, &stdout, &stderr)
	require.NoError(t, err)

	require.Error(t, cmd.Wait())
	assert.Empty(t, stdout.Bytes())
	assert.NotEmpty(t, stderr.Bytes())
	assert.Equal(t, fmt.Sprintf("%s\\n%s", strings.Repeat("a", StderrBufferSize), strings.Repeat("b", StderrBufferSize)), stderr.String())
}

func TestCommandStdErrMaxBytes(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var stdout, stderr bytes.Buffer

	cmd, err := New(ctx, exec.Command("./testdata/stderr_max_bytes_edge_case.sh"), nil, &stdout, &stderr)
	require.NoError(t, err)

	require.Error(t, cmd.Wait())
	assert.Empty(t, stdout.Bytes())
	assert.NotEmpty(t, stderr.Bytes())
}
