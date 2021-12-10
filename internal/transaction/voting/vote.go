package voting

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"hash"
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

// String returns the hex representation of the vote hash.
func (v Vote) String() string {
	return hex.EncodeToString(v[:])
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

// VoteFromString converts the given string representation of the hash into a vote.
func VoteFromString(s string) (Vote, error) {
	bytes, err := hex.DecodeString(s)
	if err != nil {
		return Vote{}, fmt.Errorf("invalid vote string: %w", err)
	}

	return VoteFromHash(bytes)
}

// VoteFromData hashes the given data and converts it to a vote.
func VoteFromData(data []byte) Vote {
	return sha1.Sum(data)
}

// VoteHash is the hash structure used to compute a Vote from arbitrary data.
type VoteHash struct {
	hash.Hash
}

// NewVoteHash returns a new VoteHash.
func NewVoteHash() VoteHash {
	return VoteHash{sha1.New()}
}

// Vote hashes all data written into VoteHash and returns the resulting Vote.
func (v VoteHash) Vote() (Vote, error) {
	return VoteFromHash(v.Sum(nil))
}
