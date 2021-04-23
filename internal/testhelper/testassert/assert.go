package testassert

import (
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"
)

// ProtoEqual asserts that expected and actual protobuf messages are equal.
// This is required as comparing messages directly with `require.Equal` doesn't
// work.
func ProtoEqual(t testing.TB, expected proto.Message, actual proto.Message) {
	require.True(t, proto.Equal(expected, actual), "proto messages not equal\nexpected: %v\ngot:      %v", expected, actual)
}
