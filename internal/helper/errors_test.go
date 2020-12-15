package helper

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestError(t *testing.T) {
	input := errors.New("sentinel error")
	for _, tc := range []struct {
		desc     string
		decorate func(err error) error
		code     codes.Code
	}{
		{
			desc:     "Internal",
			decorate: ErrInternal,
			code:     codes.Internal,
		},
		{
			desc:     "InvalidArgument",
			decorate: ErrInvalidArgument,
			code:     codes.InvalidArgument,
		},
		{
			desc:     "PreconditionFailed",
			decorate: ErrPreconditionFailed,
			code:     codes.FailedPrecondition,
		},
		{
			desc:     "NotFound",
			decorate: ErrNotFound,
			code:     codes.NotFound,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			err := tc.decorate(input)
			require.True(t, errors.Is(err, input))
			require.Equal(t, tc.code, status.Code(err))
		})
	}
}

func TestErrorf(t *testing.T) {
	input := errors.New("sentinel error")
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
	} {
		t.Run(tc.desc, func(t *testing.T) {
			err := tc.errorf("expected %s", input)
			require.EqualError(t, err, "expected sentinel error")
			require.Equal(t, tc.code, status.Code(err))
		})
	}
}
