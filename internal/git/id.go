package git

import (
	"errors"
	"fmt"
	"regexp"
)

var (
	// ErrInvalidObjectID is returned in case an object ID's string
	// representation is not a valid one.
	ErrInvalidObjectID = errors.New("invalid object ID")

	objectIDRegex = regexp.MustCompile(`\A[0-9a-f]{40}\z`)
)

// ValidateObjectID checks if id is a syntactically correct object ID. Abbreviated
// object IDs are not deemed to be valid. Returns an ErrInvalidObjectID if the
// id is not valid.
func ValidateObjectID(id string) error {
	if objectIDRegex.MatchString(id) {
		return nil
	}

	return fmt.Errorf("%w: %q", ErrInvalidObjectID, id)
}
