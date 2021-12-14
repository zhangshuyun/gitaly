package pktline

import (
	"bytes"
	"context"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper"
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

	timeoutTicker := helper.NewManualTicker()

	startTime := time.Now()
	go monitor.Monitor(ctx, PktDone(), timeoutTicker, cancel)

	// Trigger the timeout, which should cause the context to be cancelled.
	timeoutTicker.Tick()
	<-ctx.Done()

	elapsed := time.Since(startTime)
	require.Error(t, ctx.Err())
	require.Equal(t, ctx.Err(), context.Canceled)
	require.True(t, elapsed < time.Second, "Expected context to be cancelled quickly, but it was not")

	// Verify that pipe is closed
	_, err = io.ReadAll(r)
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

	timeoutTicker := helper.NewManualTicker()

	stopCh := make(chan interface{})
	timeoutTicker.StopFunc = func() {
		close(stopCh)
	}

	go monitor.Monitor(ctx, PktFlush(), timeoutTicker, cancel)

	// Verify the data is passed through correctly
	scanner := NewScanner(r)
	require.True(t, scanner.Scan())
	require.Equal(t, preTimeoutPayload, scanner.Text())
	require.True(t, scanner.Scan())
	require.Equal(t, PktFlush(), scanner.Bytes())

	// We have observed the flush packet, so the timer should've been stopped.
	<-stopCh

	// Close the pipe to skip to next reader
	require.NoError(t, waitPipeW.Close())

	// Verify that more data can be sent through the pipe
	require.True(t, scanner.Scan())
	require.Equal(t, postTimeoutPayload, scanner.Text())

	require.NoError(t, ctx.Err())
}
