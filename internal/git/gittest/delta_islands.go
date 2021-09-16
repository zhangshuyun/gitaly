package gittest

import (
	"crypto/rand"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
)

// TestDeltaIslands is based on the tests in
// https://github.com/git/git/blob/master/t/t5320-delta-islands.sh .
func TestDeltaIslands(t *testing.T, cfg config.Cfg, repoPath string, repack func() error) {
	// Create blobs that we expect Git to use delta compression on.
	blob1, err := io.ReadAll(io.LimitReader(rand.Reader, 100000))
	require.NoError(t, err)

	blob2 := append(blob1, "\nblob 2"...)

	// Assume Git prefers the largest blob as the delta base.
	badBlob := append(blob2, "\nbad blob"...)

	blob1ID := commitBlob(t, cfg, repoPath, "refs/heads/branch1", blob1)
	blob2ID := commitBlob(t, cfg, repoPath, "refs/tags/tag2", blob2)

	// The bad blob will only be reachable via a non-standard ref. Because of
	// that it should be excluded from delta chains in the main island.
	badBlobID := commitBlob(t, cfg, repoPath, "refs/bad/ref3", badBlob)

	// So far we have create blobs and commits but they will be in loose
	// object files; we want them to be delta compressed. Run repack to make
	// that happen.
	Exec(t, cfg, "-C", repoPath, "repack", "-ad")

	assert.Equal(t, badBlobID, deltaBase(t, cfg, repoPath, blob1ID), "expect blob 1 delta base to be bad blob after test setup")
	assert.Equal(t, badBlobID, deltaBase(t, cfg, repoPath, blob2ID), "expect blob 2 delta base to be bad blob after test setup")

	require.NoError(t, repack(), "repack after delta island setup")

	assert.Equal(t, blob2ID, deltaBase(t, cfg, repoPath, blob1ID), "blob 1 delta base should be blob 2 after repack")

	// blob2 is the bigger of the two so it should be the delta base
	assert.Equal(t, git.ZeroOID.String(), deltaBase(t, cfg, repoPath, blob2ID), "blob 2 should not be delta compressed after repack")
}

func commitBlob(t *testing.T, cfg config.Cfg, repoPath, ref string, content []byte) string {
	blobID := WriteBlob(t, cfg, repoPath, content)

	// No parent, that means this will be an initial commit. Not very
	// realistic but it doesn't matter for delta compression.
	commitID := WriteCommit(t, cfg, repoPath,
		WithTreeEntries(TreeEntry{
			Mode: "100644", OID: blobID, Path: "file",
		}),
		WithParents(),
	)

	Exec(t, cfg, "-C", repoPath, "update-ref", ref, commitID.String())

	return blobID.String()
}

func deltaBase(t *testing.T, cfg config.Cfg, repoPath string, blobID string) string {
	catfileOut := ExecStream(t, cfg, strings.NewReader(blobID), "-C", repoPath, "cat-file", "--batch-check=%(deltabase)")

	return chompToString(catfileOut)
}

func chompToString(s []byte) string { return strings.TrimSuffix(string(s), "\n") }
