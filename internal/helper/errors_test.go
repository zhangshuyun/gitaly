package helper

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestError(t *testing.T) {
	errorFormat := "expected %s"
	errorMessage := "sentinel error"
	input := errors.New(errorMessage)
	inputGRPC := status.Error(codes.Unauthenticated, errorMessage)
	inputGRPCFmt := status.Errorf(codes.Unauthenticated, errorFormat, errorMessage)

	for _, tc := range []struct {
		desc      string
		function  func(err error) error
		functionf func(format string, a ...interface{}) error
		code      codes.Code
		format    bool
		wrapped   bool
	}{
		// Format-less functions
		{
			desc:     "Internal",
			function: ErrInternal,
			code:     codes.Internal,
			format:   false,
			wrapped:  true,
		},
		{
			desc:     "InvalidArgument",
			function: ErrInvalidArgument,
			code:     codes.InvalidArgument,
			format:   false,
			wrapped:  true,
		},
		{
			desc:     "PreconditionFailed",
			function: ErrPreconditionFailed,
			code:     codes.FailedPrecondition,
			format:   false,
			wrapped:  true,
		},
		{
			desc:     "NotFound",
			function: ErrNotFound,
			code:     codes.NotFound,
			format:   false,
			wrapped:  true,
		},

		// Format-y functions
		{
			desc:      "Internalf",
			functionf: ErrInternalf,
			code:      codes.Internal,
			format:    true,
			wrapped:   false,
		},
		{
			desc:      "InvalidArgumentf",
			functionf: ErrInvalidArgumentf,
			code:      codes.InvalidArgument,
			format:    true,
			wrapped:   false,
		},
		{
			desc:      "PreconditionFailedf",
			functionf: ErrPreconditionFailedf,
			code:      codes.FailedPrecondition,
			format:    true,
			wrapped:   false,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			var err error
			if tc.format {
				err = tc.functionf(errorFormat, input)
				require.EqualError(t, err, fmt.Sprintf(errorFormat, errorMessage))
				require.False(t, errors.Is(err, input))
			} else {
				err = tc.function(input)
				require.EqualError(t, err, errorMessage)
				require.True(t, errors.Is(err, input))
			}
			require.Equal(t, tc.code, status.Code(err))

			// Does an existing GRPC's error's code get
			// preserved?
			if tc.format {
				err = tc.functionf(errorFormat, inputGRPCFmt)
				require.Equal(t, tc.code, status.Code(err))
			} else {
				err = tc.function(inputGRPC)
				require.True(t, errors.Is(err, inputGRPC))
				require.NotEqual(t, tc.code, status.Code(inputGRPC))
				require.Equal(t, status.Code(inputGRPC), status.Code(err))
			}
		})
	}
}
