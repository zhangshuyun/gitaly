package praefectutil

import (
	"crypto/sha256"
	"fmt"
	"strconv"
	"strings"
)

// poolPathPrefix is the prefix directory where Praefect places object pools.
const poolPathPrefix = "@cluster/pools/"

// IsPoolPath returns whether the relative path indicates this is a Praefect generated object pool path.
func IsPoolPath(relativePath string) bool {
	return strings.HasPrefix(relativePath, poolPathPrefix)
}

// DeriveReplicaPath derives a repository's disk storage path from its repository ID. The repository ID
// is hashed with SHA256 and the first four hex digits of the hash are used as the two subdirectories to
// ensure even distribution into subdirectories. The format is @cluster/repositories/ab/cd/<repository-id>.
func DeriveReplicaPath(repositoryID int64) string {
	return deriveDiskPath("@cluster/repositories/", repositoryID)
}

// DerivePoolPath derives an object pools's disk storage path from its repository ID. The repository ID
// is hashed with SHA256 and the first four hex digits of the hash are used as the two subdirectories to
// ensure even distribution into subdirectories. The format is @cluster/pools/ab/cd/<repository-id>. The pools
// have a different directory prefix from other repositories so Gitaly can identify them in OptimizeRepository
// and avoid pruning them.
func DerivePoolPath(repositoryID int64) string {
	return deriveDiskPath(poolPathPrefix, repositoryID)
}

func deriveDiskPath(prefixDir string, repositoryID int64) string {
	hasher := sha256.New()
	// String representation of the ID is used to make it easier to derive the replica paths with
	// external tools. The error is ignored as the hash.Hash interface is documented to never return
	// an error.
	hasher.Write([]byte(strconv.FormatInt(repositoryID, 10)))
	hash := hasher.Sum(nil)
	return fmt.Sprintf("%s%x/%x/%d", prefixDir, hash[0:1], hash[1:2], repositoryID)
}
