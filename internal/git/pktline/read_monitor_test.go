package pktline

import (
	"bytes"
	"context"
	"io"
	"io/ioutil"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
)

func TestReadMonitorTimeout(t *testing.T) {
	waitPipeR, waitPipeW := io.Pipe()
	defer waitPipeW.Close()

	ctx, cancel := testhelper.Context()
	defer cancel()

	in := io.MultiReader(
		strings.NewReader("000ftest string"),
		waitPipeR, // this pipe reader lets us block the multi reader
	)

	r, monitor, err := NewReadMonitor(ctx, in)
	require.NoError(t, err)

	startTime := time.Now()
	go monitor.Monitor(PktDone(), 10*time.Millisecond, cancel)

	// We should be done quickly
	<-ctx.Done()

	elapsed := time.Since(startTime)
	require.Error(t, ctx.Err())
	require.Equal(t, ctx.Err(), context.Canceled)
	require.True(t, elapsed < time.Second, "Expected context to be cancelled quickly, but it was not")

	// Verify that pipe is closed
	_, err = ioutil.ReadAll(r)
	require.Error(t, err)
	require.IsType(t, &os.PathError{}, err)
}

func TestReadMonitorSuccess(t *testing.T) {
	waitPipeR, waitPipeW := io.Pipe()

	ctx, cancel := testhelper.Context()
	defer cancel()

	preTimeoutPayload := "000ftest string"
	postTimeoutPayload := "0017post-timeout string"

	in := io.MultiReader(
		strings.NewReader(preTimeoutPayload),
		bytes.NewReader(PktFlush()),
		waitPipeR, // this pipe reader lets us block the multi reader
		strings.NewReader(postTimeoutPayload),
	)

	r, monitor, err := NewReadMonitor(ctx, in)
	require.NoError(t, err)

	go monitor.Monitor(PktFlush(), 10*time.Millisecond, cancel)

	// Verify the data is passed through correctly
	scanner := NewScanner(r)
	require.True(t, scanner.Scan())
	require.Equal(t, preTimeoutPayload, scanner.Text())
	require.True(t, scanner.Scan())
	require.Equal(t, PktFlush(), scanner.Bytes())

	// If the timeout *has* been stopped, a wait this long won't break it
	time.Sleep(100 * time.Millisecond)

	// Close the pipe to skip to next reader
	require.NoError(t, waitPipeW.Close())

	// Verify that more data can be sent through the pipe
	require.True(t, scanner.Scan())
	require.Equal(t, postTimeoutPayload, scanner.Text())

	require.NoError(t, ctx.Err())
}
