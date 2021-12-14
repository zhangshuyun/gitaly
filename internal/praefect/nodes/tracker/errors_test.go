package tracker

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
)

var (
	alwaysInErrorWindow = func(time.Time, time.Time) bool { return true }
	neverInErrorWindow  = func(time.Time, time.Time) bool { return false }
)

func TestErrorTracker_IncrErrors(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	writeThreshold, readThreshold := 10, 10

	errors, err := newErrors(ctx, alwaysInErrorWindow, uint32(readThreshold), uint32(writeThreshold))
	require.NoError(t, err)

	node := "backend-node-1"

	assert.False(t, errors.WriteThresholdReached(node))
	assert.False(t, errors.ReadThresholdReached(node))

	for i := 0; i < writeThreshold; i++ {
		errors.IncrWriteErr(node)
	}

	assert.True(t, errors.WriteThresholdReached(node))

	for i := 0; i < readThreshold; i++ {
		errors.IncrReadErr(node)
	}

	assert.True(t, errors.ReadThresholdReached(node))

	// Use `neverInErrorWindow()` to make sure that all errors in the queue are cleared.
	errors, err = newErrors(ctx, neverInErrorWindow, uint32(readThreshold), uint32(writeThreshold))
	require.NoError(t, err)

	errors.clear()

	assert.False(t, errors.WriteThresholdReached(node))
	assert.False(t, errors.ReadThresholdReached(node))
}

func TestErrorTracker_Concurrency(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	readAndWriteThreshold := 10
	errors, err := newErrors(ctx, alwaysInErrorWindow, uint32(readAndWriteThreshold), uint32(readAndWriteThreshold))
	require.NoError(t, err)

	node := "backend-node-1"

	assert.False(t, errors.WriteThresholdReached(node))
	assert.False(t, errors.ReadThresholdReached(node))

	var g sync.WaitGroup
	for i := 0; i < readAndWriteThreshold; i++ {
		g.Add(1)
		go func() {
			errors.IncrWriteErr(node)
			errors.IncrReadErr(node)
			errors.ReadThresholdReached(node)
			errors.WriteThresholdReached(node)

			g.Done()
		}()
	}

	g.Wait()
}

func TestErrorTracker_ClearErrors(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	writeThreshold, readThreshold := 10, 10
	errors, err := newErrors(ctx, alwaysInErrorWindow, uint32(readThreshold), uint32(writeThreshold))
	require.NoError(t, err)

	node := "backend-node-1"

	errors.IncrWriteErr(node)
	errors.IncrReadErr(node)

	now := time.Now()
	errors.isInErrorWindow = func(_ time.Time, errTime time.Time) bool {
		// Consider all errors which have been added until now to not be part of the error
		// window anymore. All new events will be considered part of it though.
		return errTime.After(now)
	}

	errors.IncrWriteErr(node)
	errors.IncrReadErr(node)

	errors.clear()
	assert.Len(t, errors.readErrors[node], 1, "clear should only have cleared the read error older than the time specifiied")
	assert.Len(t, errors.writeErrors[node], 1, "clear should only have cleared the write error older than the time specifiied")
}

func TestErrorTracker_Expired(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	threshold := 10
	errors, err := newErrors(ctx, alwaysInErrorWindow, uint32(threshold), uint32(threshold))
	require.NoError(t, err)

	node := "node"
	for i := 0; i < threshold; i++ {
		errors.IncrWriteErr(node)
		errors.IncrReadErr(node)
	}

	assert.True(t, errors.ReadThresholdReached(node))
	assert.True(t, errors.WriteThresholdReached(node))

	cancel()

	assert.False(t, errors.ReadThresholdReached(node))
	assert.False(t, errors.WriteThresholdReached(node))

	for i := 0; i < threshold; i++ {
		errors.IncrWriteErr(node)
		errors.IncrReadErr(node)
	}

	assert.False(t, errors.ReadThresholdReached(node))
	assert.False(t, errors.WriteThresholdReached(node))
}
