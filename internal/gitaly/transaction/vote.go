package transaction

import (
	"crypto/sha1"
	"fmt"
)

const (
	// voteSize is the number of bytes of a vote.
	voteSize = sha1.Size
)

// Vote is a vote cast by a node.
type Vote [voteSize]byte

// Bytes returns a byte slice containing the hash.
func (v Vote) Bytes() []byte {
	return v[:]
}

// VoteFromHash converts the given byte slice containing a hash into a vote.
func VoteFromHash(bytes []byte) (Vote, error) {
	if len(bytes) != voteSize {
		return Vote{}, fmt.Errorf("invalid vote length %d", len(bytes))
	}

	var vote Vote
	copy(vote[:], bytes)

	return vote, nil
}

// VoteFromData hashes the given data and converts it to a vote.
func VoteFromData(data []byte) Vote {
	return sha1.Sum(data)
}
