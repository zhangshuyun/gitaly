package helper

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestError(t *testing.T) {
	errorMessage := "sentinel error"
	input := errors.New(errorMessage)
	inputGRPCCode := codes.Unauthenticated
	inputGRPC := status.Error(inputGRPCCode, errorMessage)

	for _, tc := range []struct {
		desc   string
		errorf func(err error) error
		code   codes.Code
	}{
		{
			desc:   "Internal",
			errorf: ErrInternal,
			code:   codes.Internal,
		},
		{
			desc:   "InvalidArgument",
			errorf: ErrInvalidArgument,
			code:   codes.InvalidArgument,
		},
		{
			desc:   "PreconditionFailed",
			errorf: ErrPreconditionFailed,
			code:   codes.FailedPrecondition,
		},
		{
			desc:   "NotFound",
			errorf: ErrNotFound,
			code:   codes.NotFound,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			// tc.code and our canary test code must not
			// clash!
			require.NotEqual(t, tc.code, inputGRPCCode)

			// When not re-throwing an error we get the
			// GRPC error code corresponding to the
			// function's name.
			err := tc.errorf(input)
			require.EqualError(t, err, errorMessage)
			require.False(t, errors.Is(err, inputGRPC))
			require.Equal(t, tc.code, status.Code(err))

			// When re-throwing an error an existing GRPC
			// error code will get preserved, instead of
			// the one corresponding to the function's
			// name.
			err = tc.errorf(inputGRPC)
			require.True(t, errors.Is(err, inputGRPC))
			require.False(t, errors.Is(err, input))
			require.Equal(t, inputGRPCCode, status.Code(err))
			require.NotEqual(t, tc.code, status.Code(inputGRPC))
		})
	}
}

func TestErrorF_withVFormat(t *testing.T) {
	testErrorfFormat(t, "expected %v", "expected %v")
}

func TestErrorF_withWFormat(t *testing.T) {
	testErrorfFormat(t, "expected %w", "expected %s")
}

// oldPreconditionFailedf shows ErrPreconditionFailedf looked like
// before 777a12cfd. We're testing the nature of a regression in that
// change.
func oldPreconditionFailedf(format string, a ...interface{}) error {
	return DecorateError(codes.FailedPrecondition, fmt.Errorf(format, a...))
}

func testErrorfFormat(t *testing.T, errorFormat, errorFormatEqual string) {
	isFormatW := strings.Contains(errorFormat, "%w")
	errorMessage := "sentinel error"
	input := errors.New(errorMessage)
	inputGRPCCode := codes.Unauthenticated
	inputGRPC := status.Error(inputGRPCCode, errorMessage)
	inputGRPCFmt := status.Errorf(inputGRPCCode, errorFormat, errorMessage)

	for _, tc := range []struct {
		desc   string
		errorf func(format string, a ...interface{}) error
		code   codes.Code
	}{
		{
			desc:   "Internalf",
			errorf: ErrInternalf,
			code:   codes.Internal,
		},
		{
			desc:   "InvalidArgumentf",
			errorf: ErrInvalidArgumentf,
			code:   codes.InvalidArgument,
		},
		{
			desc:   "PreconditionFailedf",
			errorf: ErrPreconditionFailedf,
			code:   codes.FailedPrecondition,
		},
		{
			desc:   "oldPreconditionFailedf",
			errorf: oldPreconditionFailedf,
			code:   codes.FailedPrecondition,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			// tc.code and our canary test code must not
			// clash!
			require.NotEqual(t, tc.code, inputGRPCCode)

			// When not re-throwing an error we get the
			// GRPC error code corresponding to the
			// function's name. Just like the non-f functions.
			err := tc.errorf(errorFormat, input)
			require.EqualError(t, err, fmt.Sprintf(errorFormatEqual, errorMessage))
			require.False(t, errors.Is(err, inputGRPC))
			require.Equal(t, tc.code, status.Code(err))

			// When re-throwing an error an existing GRPC
			// error code will get clobbered, not the one
			// corresponding to the function's name. This
			// is unlike the non-f functions.
			err = tc.errorf(errorFormat, inputGRPCFmt)
			require.False(t, errors.Is(err, input))
			require.Equal(t, tc.code, status.Code(err))
			require.NotEqual(t, tc.code, status.Code(inputGRPC))

			errX := tc.errorf(errorFormat, inputGRPC)
			require.Equal(t, errors.Is(errX, inputGRPC), isFormatW) // .True() for non-f
			require.False(t, errors.Is(errX, input))
			require.NotEqual(t, inputGRPCCode, status.Code(errX)) // .Equal() for non-f
			require.NotEqual(t, tc.code, status.Code(inputGRPC))
		})
	}
}
