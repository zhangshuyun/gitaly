package praefectutil

import (
	"crypto/sha256"
	"fmt"
	"strconv"
)

// DeriveReplicaPath derives a repository's disk storage path from its repository ID. The repository ID
// is hashed with SHA256 and the first four hex digits of the hash are used as the two subdirectories to
// ensure even distribution into subdirectories. The format is @praefect/v<version number>/ab/cd/<repository-id>.
// The prefix '@praefect' marks the directory clearly as Praefect owned. The <version> component leaves room for
// evolving our disk storage schema in future. If we make changes that may conflict with existing storage paths,
// we can simply increment the version.
func DeriveReplicaPath(repositoryID int64) string {
	hasher := sha256.New()
	// String representation of the ID is used to make it easier to derive the replica paths with
	// external tools. The error is ignored as the hash.Hash interface is documented to never return
	// an error.
	hasher.Write([]byte(strconv.FormatInt(repositoryID, 10)))
	hash := hasher.Sum(nil)
	return fmt.Sprintf("@praefect/v1/%x/%x/%d", hash[0:1], hash[1:2], repositoryID)
}
