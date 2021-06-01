package tracker

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
)

func TestErrorTracker_IncrErrors(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	writeThreshold, readThreshold := 10, 10

	errors, err := newErrors(ctx, time.Second, uint32(readThreshold), uint32(writeThreshold))
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

	// use negative value for window so we are ensured to clear all of the errors in the queue
	errors, err = newErrors(ctx, -time.Second, uint32(readThreshold), uint32(writeThreshold))
	require.NoError(t, err)

	errors.clear()

	assert.False(t, errors.WriteThresholdReached(node))
	assert.False(t, errors.ReadThresholdReached(node))
}

func TestErrorTracker_Concurrency(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	readAndWriteThreshold := 10
	errors, err := newErrors(ctx, 1*time.Second, uint32(readAndWriteThreshold), uint32(readAndWriteThreshold))
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
	errors, err := newErrors(ctx, time.Second, uint32(readThreshold), uint32(writeThreshold))
	require.NoError(t, err)

	node := "backend-node-1"

	errors.IncrWriteErr(node)
	errors.IncrReadErr(node)

	clearBeforeNow := time.Now()

	errors.olderThan = func() time.Time {
		return clearBeforeNow
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
	errors, err := newErrors(ctx, 10*time.Second, uint32(threshold), uint32(threshold))
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
