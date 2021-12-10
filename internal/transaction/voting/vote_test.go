package voting

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestVoteFromHash(t *testing.T) {
	_, err := VoteFromHash([]byte{})
	require.Error(t, err)

	_, err = VoteFromHash(bytes.Repeat([]byte{1}, voteSize-1))
	require.Equal(t, fmt.Errorf("invalid vote length %d", 19), err)

	_, err = VoteFromHash(bytes.Repeat([]byte{1}, voteSize+1))
	require.Equal(t, fmt.Errorf("invalid vote length %d", 21), err)

	vote, err := VoteFromHash(bytes.Repeat([]byte{1}, voteSize))
	require.NoError(t, err)
	require.Equal(t, bytes.Repeat([]byte{1}, voteSize), vote.Bytes())
}

func TestVoteFromString(t *testing.T) {
	_, err := VoteFromString("")
	require.Equal(t, fmt.Errorf("invalid vote length 0"), err)

	_, err = VoteFromString("x")
	require.Error(t, err)
	var invalidByteError hex.InvalidByteError
	require.True(t, errors.As(err, &invalidByteError))
	require.Equal(t, hex.InvalidByteError('x'), invalidByteError)

	_, err = VoteFromString("1234")
	require.Equal(t, fmt.Errorf("invalid vote length 2"), err)

	_, err = VoteFromString(strings.Repeat("1", (voteSize+1)*2))
	require.Equal(t, fmt.Errorf("invalid vote length 21"), err)

	vote, err := VoteFromString(strings.Repeat("1", voteSize*2))
	require.Equal(t, Vote{
		0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11,
		0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11,
	}, vote)
	require.NoError(t, err)
}

func TestVoteFromData(t *testing.T) {
	require.Equal(t, Vote{
		0xda, 0x39, 0xa3, 0xee, 0x5e, 0x6b, 0x4b, 0x0d, 0x32, 0x55,
		0xbf, 0xef, 0x95, 0x60, 0x18, 0x90, 0xaf, 0xd8, 0x07, 0x09,
	}, VoteFromData([]byte{}))

	require.Equal(t, Vote{
		0x88, 0x43, 0xd7, 0xf9, 0x24, 0x16, 0x21, 0x1d, 0xe9, 0xeb,
		0xb9, 0x63, 0xff, 0x4c, 0xe2, 0x81, 0x25, 0x93, 0x28, 0x78,
	}, VoteFromData([]byte("foobar")))
}

func TestVoteHash(t *testing.T) {
	hash := NewVoteHash()

	vote, err := hash.Vote()
	require.NoError(t, err)
	require.Equal(t, VoteFromData([]byte{}), vote)

	_, err = hash.Write([]byte("foo"))
	require.NoError(t, err)
	vote, err = hash.Vote()
	require.NoError(t, err)
	require.Equal(t, VoteFromData([]byte("foo")), vote)

	_, err = hash.Write([]byte("bar"))
	require.NoError(t, err)

	vote, err = hash.Vote()
	require.NoError(t, err)
	require.Equal(t, VoteFromData([]byte("foobar")), vote)
}
