package git

import (
	"crypto/sha1"
	"fmt"
	"math/big"
	"regexp"
)

var refsAllowed = regexp.MustCompile(`HEAD|(refs/(heads|tags|keep-around|merge-requests|environments|notes)/)`)

// Checksum is a hash representation of all references in a repository.
// Checksum must not be copied after first use.
type Checksum struct {
	sum big.Int
}

// AddBytes adds a reference to the checksum that is a line in the output format of `git-show-ref`.
func (c *Checksum) AddBytes(line []byte) {
	if !refsAllowed.Match(line) {
		return
	}

	h := sha1.New()
	// hash.Hash will never return an error.
	_, _ = h.Write(line)

	c.update(h.Sum(nil))
}

// Add adds a reference to the checksum.
func (c *Checksum) Add(ref Reference) {
	if !refsAllowed.MatchString(ref.Name.String()) {
		return
	}

	h := sha1.New()
	// hash.Hash will never return an error.
	_, _ = fmt.Fprintf(h, "%s %s", ref.Target, ref.Name)

	c.update(h.Sum(nil))
}

func (c *Checksum) update(refSum []byte) {
	if c.sum.BitLen() == 0 {
		c.sum.SetBytes(refSum)
	} else {
		var hash big.Int
		hash.SetBytes(refSum)
		c.sum.Xor(&c.sum, &hash)
	}
}

// IsZero returns true when no references have been added to the checksum.
func (c *Checksum) IsZero() bool {
	return c.sum.BitLen() == 0
}

// Bytes returns the checksum as a slice of bytes.
func (c *Checksum) Bytes() []byte {
	return c.sum.Bytes()
}
