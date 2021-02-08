package praefect

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestNewLockedRandom tests that n is correctly passed down to the random
// implementation and that the return value is correctly passed back. To test
// that access to the random is actually synchronized, we launch 50 goroutines
// to call the random func concurrently to increment actual. If the calls to
// random are not correctly synchronized, actual might be not match expected
// at the end and the race detector should detect racy accesses even in the
// cases where the values match.
func TestNewLockedRandom(t *testing.T) {
	expected := 50
	actual := 0

	random := NewLockedRandom(mockRandom{
		intnFunc: func(n int) int {
			assert.Equal(t, 1, n)
			actual++
			return 2
		},
	})

	var wg sync.WaitGroup
	wg.Add(expected)

	for i := 0; i < expected; i++ {
		go func() {
			defer wg.Done()
			assert.Equal(t, 2, random.Intn(1))
		}()
	}

	wg.Wait()

	require.Equal(t, expected, actual)
}
