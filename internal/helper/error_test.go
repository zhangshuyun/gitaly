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
			desc:   "Canceled",
			errorf: ErrCanceled,
			code:   codes.Canceled,
		},
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
			desc:   "FailedPrecondition",
			errorf: ErrFailedPrecondition,
			code:   codes.FailedPrecondition,
		},
		{
			desc:   "NotFound",
			errorf: ErrNotFound,
			code:   codes.NotFound,
		},
		{
			desc:   "Unavailable",
			errorf: ErrUnavailable,
			code:   codes.Unavailable,
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
			desc:   "FailedPreconditionf",
			errorf: ErrFailedPreconditionf,
			code:   codes.FailedPrecondition,
		},
		{
			desc:   "NotFoundf",
			errorf: ErrNotFoundf,
			code:   codes.NotFound,
		},
		{
			desc:   "ErrUnavailablef",
			errorf: ErrUnavailablef,
			code:   codes.Unavailable,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			require.NotEqual(t, tc.code, inputGRPCCode, "canary test code and tc.code may not be the same")

			// When not re-throwing an error we get the GRPC error code corresponding to
			// the function's name. Just like the non-f functions.
			err := tc.errorf(errorFormat, input)
			require.EqualError(t, err, fmt.Sprintf(errorFormatEqual, errorMessage))
			require.False(t, errors.Is(err, inputGRPC))
			require.Equal(t, tc.code, status.Code(err))

			// When wrapping an existing gRPC error, then the error code will stay the
			// same.
			err = tc.errorf(errorFormat, inputGRPCFmt)
			require.False(t, errors.Is(err, input))
			if isFormatW {
				require.Equal(t, inputGRPCCode, status.Code(err))
			} else {
				require.Equal(t, tc.code, status.Code(err))
			}
			require.NotEqual(t, tc.code, status.Code(inputGRPC))

			// The same as above, except that we test with an error returned by
			// `status.Error()`.
			errX := tc.errorf(errorFormat, inputGRPC)
			require.Equal(t, errors.Is(errX, inputGRPC), isFormatW) // .True() for non-f
			require.False(t, errors.Is(errX, input))
			if isFormatW {
				require.Equal(t, inputGRPCCode, status.Code(errX))
			} else {
				require.Equal(t, tc.code, status.Code(errX))
			}
			require.Equal(t, inputGRPCCode, status.Code(inputGRPC))
		})
	}
}
