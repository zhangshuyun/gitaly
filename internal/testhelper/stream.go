package testhelper

import (
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// ReceiveEOFWithTimeout reads to the end of the stream and will throw an
// error if a deadlock is suspected
func ReceiveEOFWithTimeout(t testing.TB, errorFunc func() error) {
	errCh := make(chan error, 1)
	go func() {
		errCh <- errorFunc()
	}()

	var err error
	select {
	case err = <-errCh:
	case <-time.After(1 * time.Second):
		err = fmt.Errorf("timed out waiting for EOF")
	}

	if err != nil {
		require.Equal(t, io.EOF, err)
	}
}
