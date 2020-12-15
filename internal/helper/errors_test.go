package helper

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// MockErrNotFoundf wraps error with codes.NotFound, unless err is already a grpc error
func MockErrNotFoundf(format string, a ...interface{}) error {
	return DecorateError(codes.NotFound, fmt.Errorf(format, a...))
}

func TestError(t *testing.T) {
	errorFormat := "expected %v"
	errorFormatW := "expected %v"
	errorMessage := "sentinel error"
	input := errors.New(errorMessage)
	inputGRPCCode := codes.Unauthenticated
	inputGRPC := status.Error(inputGRPCCode, errorMessage)
	inputGRPCFmt := status.Errorf(inputGRPCCode, errorFormat, errorMessage)
	inputGRPCFmtW := status.Errorf(inputGRPCCode, errorFormatW, errorMessage)

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
		{
			desc:      "MockErrNotFoundf",
			functionf: MockErrNotFoundf,
			code:      codes.NotFound,
			format:    true,
			wrapped:   true,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			// assert: tc.code and our test code must not
			// clash.
			require.NotEqual(t, tc.code, inputGRPCCode)

			var err, errW error
			errorMessageFormatted := errorMessage
			errorMessageFormattedW := errorMessage
			if tc.format {
				errorMessageFormatted = fmt.Sprintf(errorFormat, errorMessage)
				errorMessageFormattedW = fmt.Sprintf(errorFormatW, errorMessage)
				err = tc.functionf(errorFormat, input)
				errW = tc.functionf(errorFormatW, input)
				require.EqualError(t, errW, errorMessageFormatted)
				require.EqualError(t, errW, errorMessageFormattedW)
			} else {
				err = tc.function(input)
				require.True(t, tc.wrapped)
			}
			require.EqualError(t, err, errorMessageFormatted)
			require.EqualError(t, err, errorMessageFormattedW)

			if tc.wrapped {
				require.False(t, errors.Is(err, inputGRPC))
			} else {
				require.Equal(t, errors.Is(err, input), tc.wrapped)
			}
			require.Equal(t, tc.code, status.Code(err))

			// Does an existing GRPC's error's code get
			// preserved?
			expectedCode := inputGRPCCode
			if tc.format {
				err = tc.functionf(errorFormat, inputGRPCFmt)
				errW = tc.functionf(errorFormat, inputGRPCFmtW)
				expectedCode = tc.code
			} else {
				err = tc.function(inputGRPC)
				errW = nil
				require.True(t, errors.Is(err, inputGRPC))
				require.True(t, tc.wrapped)
			}
			if tc.wrapped {
				require.NotEqual(t, errors.Is(err, input), tc.wrapped)
				require.NotEqual(t, errors.Is(errW, input), tc.wrapped)
			} else {
				require.Equal(t, errors.Is(err, input), tc.wrapped)
				require.Equal(t, errors.Is(errW, input), tc.wrapped)
			}
			require.Equal(t, expectedCode, status.Code(err))
			require.NotEqual(t, tc.code, status.Code(inputGRPC))
		})
	}
}
