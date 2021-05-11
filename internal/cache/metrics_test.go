package cache

import "sync"

var (
	ExportMockRemovalCounter = &MockCounter{}
	ExportMockCheckCounter   = &MockCounter{}
	ExportMockLoserBytes     = &MockCounter{}

	ExportDisableMoveAndClear = &disableMoveAndClear
	ExportDisableWalker       = &disableWalker
)

// MockCounter is a mocked counter used for the testing.
type MockCounter struct {
	sync.RWMutex
	count int
}

// Add increments counter on the n.
func (mc *MockCounter) Add(n int) {
	mc.Lock()
	mc.count += n
	mc.Unlock()
}

// Count returns total value of the increments.
func (mc *MockCounter) Count() int {
	mc.RLock()
	defer mc.RUnlock()
	return mc.count
}

// Reset resets the counter to zero.
func (mc *MockCounter) Reset() {
	mc.Lock()
	mc.count = 0
	mc.Unlock()
}

func init() {
	// override counter functions with our mocked version
	countWalkRemoval = func() { ExportMockRemovalCounter.Add(1) }
	countWalkCheck = func() { ExportMockCheckCounter.Add(1) }
	countLoserBytes = func(n float64) { ExportMockLoserBytes.Add(int(n)) }
}
