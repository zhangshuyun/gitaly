package dontpanic_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/dontpanic"
)

func TestTry(t *testing.T) {
	dontpanic.Try(func() { panic("don't panic") })
}

func TestTryNoPanic(t *testing.T) {
	invoked := false
	dontpanic.Try(func() { invoked = true })
	require.True(t, invoked)
}

func TestGo(t *testing.T) {
	done := make(chan struct{})
	dontpanic.Go(func() {
		defer close(done)
		panic("don't panic")
	})
	<-done
}

func TestGoNoPanic(t *testing.T) {
	done := make(chan struct{})
	dontpanic.Go(func() { close(done) })
	<-done
}

func TestGoForever(t *testing.T) {
	var i int
	recoveredQ := make(chan struct{})
	expectPanics := 5

	fn := func() {
		defer func() { recoveredQ <- struct{}{} }()
		i++

		if i > expectPanics {
			close(recoveredQ)
		}

		panic("don't panic")
	}

	dontpanic.GoForever(time.Microsecond, fn)

	var actualPanics int
	for range recoveredQ {
		actualPanics++
	}
	require.Equal(t, expectPanics, actualPanics)
}
