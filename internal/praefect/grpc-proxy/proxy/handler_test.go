package proxy

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFailDestinationWithError(t *testing.T) {
	expectedErr := errors.New("some error")

	t.Run("works with nil ErrHandlers", func(t *testing.T) {
		require.NotPanics(t, func() {
			failDestinationsWithError(&StreamParameters{
				primary:     Destination{},
				secondaries: []Destination{{}},
			}, expectedErr)
		})
	})

	t.Run("fails both primary and secondaries", func(t *testing.T) {
		var primaryErr, secondaryErr error

		failDestinationsWithError(&StreamParameters{
			primary: Destination{ErrHandler: func(err error) error {
				primaryErr = err
				return nil
			}},
			secondaries: []Destination{{ErrHandler: func(err error) error {
				secondaryErr = err
				return nil
			}}},
		}, expectedErr)

		require.Equal(t, expectedErr, primaryErr)
		require.Equal(t, expectedErr, secondaryErr)
	})
}
