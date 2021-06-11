package testassert

import (
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/testing/protocmp"
)

// ProtoEqual asserts that expected and actual protobuf messages are equal.
// It can accept not only proto.Message, but slices, maps, and structs too.
// This is required as comparing messages directly with `require.Equal` doesn't
// work.
func ProtoEqual(t testing.TB, expected, actual interface{}) {
	t.Helper()
	require.Empty(t, cmp.Diff(expected, actual, protocmp.Transform()))
}

// GrpcEqualErr asserts that expected and actual gRPC errors are equal.
// This is required as comparing messages directly with `require.Equal` doesn't
// work.
func GrpcEqualErr(t testing.TB, expected, actual error) {
	t.Helper()
	// .Proto() handles nil receiver
	ProtoEqual(t, status.Convert(expected).Proto(), status.Convert(actual).Proto())
}

// ContainsGrpcError checks that an equal gRPC error is present in the slice of errors.
func ContainsGrpcError(t testing.TB, errs []error, err error) {
	t.Helper()
	errStatus := status.Convert(err).Proto()
	for _, e := range errs {
		if cmp.Equal(status.Convert(e).Proto(), errStatus, protocmp.Transform()) {
			return
		}
	}
	require.FailNow(t, fmt.Sprintf("%#v does not contain %#v", errs, err))
}
