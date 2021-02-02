package praefect

import (
	"sync"
)

// Random is the interface of the Go random number generator.
type Random interface {
	// Intn returns a random integer in the range [0,n).
	Intn(n int) int
	// Shuffle pseudo-randomizes the order of elements. n is the number of elements.
	// Shuffle panics if n < 0. swap swaps the elements with indexes i and j.
	Shuffle(n int, swap func(i, j int))
}

type lockedRandom struct {
	m sync.Mutex
	r Random
}

// NewLockedRandom wraps the passed in Random to make it safe for concurrent use.
func NewLockedRandom(r Random) Random {
	return &lockedRandom{r: r}
}

func (lr *lockedRandom) Intn(n int) int {
	lr.m.Lock()
	defer lr.m.Unlock()
	return lr.r.Intn(n)
}

func (lr *lockedRandom) Shuffle(n int, swap func(i, j int)) {
	lr.m.Lock()
	defer lr.m.Unlock()
	lr.r.Shuffle(n, swap)
}

type mockRandom struct {
	intnFunc    func(int) int
	shuffleFunc func(int, func(int, int))
}

func (r mockRandom) Intn(n int) int {
	return r.intnFunc(n)
}

func (r mockRandom) Shuffle(n int, swap func(i, j int)) {
	r.shuffleFunc(n, swap)
}
