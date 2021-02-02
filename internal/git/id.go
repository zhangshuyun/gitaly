package git

import (
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
)

const (
	// EmptyTreeOID is the Git tree object hash that corresponds to an empty tree (directory)
	EmptyTreeOID = ObjectID("4b825dc642cb6eb9a060e54bf8d69288fbee4904")

	// ZeroOID is the special value that Git uses to signal a ref or object does not exist
	ZeroOID = ObjectID("0000000000000000000000000000000000000000")
)

var (
	// ErrInvalidObjectID is returned in case an object ID's string
	// representation is not a valid one.
	ErrInvalidObjectID = errors.New("invalid object ID")

	objectIDRegex = regexp.MustCompile(`\A[0-9a-f]{40}\z`)
)

// ObjectID represents an object ID.
type ObjectID string

// NewObjectIDFromHex constructs a new ObjectID from the given hex
// representation of the object ID. Returns ErrInvalidObjectID if the given
// OID is not valid.
func NewObjectIDFromHex(hex string) (ObjectID, error) {
	if err := ValidateObjectID(hex); err != nil {
		return "", err
	}
	return ObjectID(hex), nil
}

// String returns the hex representation of the ObjectID.
func (oid ObjectID) String() string {
	return string(oid)
}

// Bytes returns the byte representation of the ObjectID.
func (oid ObjectID) Bytes() ([]byte, error) {
	decoded, err := hex.DecodeString(string(oid))
	if err != nil {
		return nil, err
	}
	return decoded, nil
}

// Revision returns a revision of the ObjectID. This directly returns the hex
// representation as every object ID is a valid revision.
func (oid ObjectID) Revision() Revision {
	return Revision(oid.String())
}

// ValidateObjectID checks if id is a syntactically correct object ID. Abbreviated
// object IDs are not deemed to be valid. Returns an ErrInvalidObjectID if the
// id is not valid.
func ValidateObjectID(id string) error {
	if objectIDRegex.MatchString(id) {
		return nil
	}

	return fmt.Errorf("%w: %q", ErrInvalidObjectID, id)
}

// IsZeroOID is a shortcut for `something == git.ZeroOID.String()`
func (oid ObjectID) IsZeroOID() bool {
	return string(oid) == string(ZeroOID)
}
