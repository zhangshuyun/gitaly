package helper

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testassert"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
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

func TestErrWithDetails(t *testing.T) {
	for _, tc := range []struct {
		desc            string
		err             error
		details         []proto.Message
		expectedErr     error
		expectedMessage string
		expectedDetails []proto.Message
		expectedCode    codes.Code
	}{
		{
			desc:        "no error",
			expectedErr: errors.New("no error given"),
		},
		{
			desc:        "status with OK code",
			err:         status.Error(codes.OK, "message"),
			expectedErr: errors.New("no error given"),
		},
		{
			desc:        "normal error",
			err:         errors.New("message"),
			expectedErr: errors.New("error is not a gRPC status"),
		},
		{
			desc:            "status",
			err:             status.Error(codes.FailedPrecondition, "message"),
			expectedMessage: "rpc error: code = FailedPrecondition desc = message",
			expectedCode:    codes.FailedPrecondition,
		},
		{
			desc: "status with details",
			err:  status.Error(codes.FailedPrecondition, "message"),
			details: []proto.Message{
				&gitalypb.Repository{},
			},
			expectedMessage: "rpc error: code = FailedPrecondition desc = message",
			expectedCode:    codes.FailedPrecondition,
			expectedDetails: []proto.Message{
				&gitalypb.Repository{},
			},
		},
		{
			desc: "status with multiple details",
			err:  status.Error(codes.FailedPrecondition, "message"),
			details: []proto.Message{
				&gitalypb.Repository{RelativePath: "a"},
				&gitalypb.Repository{RelativePath: "b"},
			},
			expectedMessage: "rpc error: code = FailedPrecondition desc = message",
			expectedCode:    codes.FailedPrecondition,
			expectedDetails: []proto.Message{
				&gitalypb.Repository{RelativePath: "a"},
				&gitalypb.Repository{RelativePath: "b"},
			},
		},
		{
			desc: "status with mixed type details",
			err:  status.Error(codes.FailedPrecondition, "message"),
			details: []proto.Message{
				&gitalypb.Repository{},
				&gitalypb.GitCommit{},
			},
			expectedMessage: "rpc error: code = FailedPrecondition desc = message",
			expectedCode:    codes.FailedPrecondition,
			expectedDetails: []proto.Message{
				&gitalypb.Repository{},
				&gitalypb.GitCommit{},
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			detailedErr, err := ErrWithDetails(tc.err, tc.details...)
			require.Equal(t, tc.expectedErr, err)
			if err != nil {
				return
			}

			require.Equal(t, tc.expectedMessage, detailedErr.Error())

			st, ok := status.FromError(detailedErr)
			require.True(t, ok, "error should be a status")
			require.Equal(t, tc.expectedCode, st.Code())

			statusProto := st.Proto()
			require.NotNil(t, statusProto)

			var details []proto.Message
			for _, detail := range statusProto.GetDetails() {
				detailProto, err := detail.UnmarshalNew()
				require.NoError(t, err)
				details = append(details, detailProto)
			}
			testassert.ProtoEqual(t, tc.expectedDetails, details)
		})
	}
}
