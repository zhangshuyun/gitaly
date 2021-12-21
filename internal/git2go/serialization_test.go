package git2go

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSerializableError(t *testing.T) {
	for _, tc := range []struct {
		desc          string
		input         error
		output        error
		containsTyped bool
	}{
		{
			desc:   "plain error",
			input:  errors.New("plain error"),
			output: wrapError{Message: "plain error"},
		},
		{
			desc:   "wrapped plain error",
			input:  fmt.Errorf("error wrapper: %w", errors.New("plain error")),
			output: wrapError{Message: "error wrapper: plain error", Err: wrapError{Message: "plain error"}},
		},
		{
			desc:          "wrapped typed error",
			containsTyped: true,
			input:         fmt.Errorf("error wrapper: %w", InvalidArgumentError("typed error")),
			output:        wrapError{Message: "error wrapper: typed error", Err: InvalidArgumentError("typed error")},
		},
		{
			desc:          "typed wrapper",
			containsTyped: true,
			input: wrapError{
				Message: "error wrapper: typed error 1: typed error 2",
				Err: wrapError{
					Message: "typed error 1: typed error 2",
					Err:     InvalidArgumentError("typed error 2"),
				},
			},
			output: wrapError{
				Message: "error wrapper: typed error 1: typed error 2",
				Err: wrapError{
					Message: "typed error 1: typed error 2",
					Err:     InvalidArgumentError("typed error 2"),
				},
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			encoded := &bytes.Buffer{}
			require.NoError(t, gob.NewEncoder(encoded).Encode(SerializableError(tc.input)))
			var err wrapError
			require.NoError(t, gob.NewDecoder(encoded).Decode(&err))
			require.Equal(t, tc.output, err)

			var typedErr InvalidArgumentError
			require.Equal(t, tc.containsTyped, errors.As(err, &typedErr))
		})
	}
}
