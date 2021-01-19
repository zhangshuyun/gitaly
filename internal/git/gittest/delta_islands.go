package gittest

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
)

// TestDeltaIslands is based on the tests in
// https://github.com/git/git/blob/master/t/t5320-delta-islands.sh .
func TestDeltaIslands(t *testing.T, repoPath string, repack func() error) {
	// Create blobs that we expect Git to use delta compression on.
	blob1 := make([]byte, 100000)
	_, err := io.ReadFull(rand.Reader, blob1)
	require.NoError(t, err)

	blob2 := append(blob1, "\nblob 2"...)

	// Assume Git prefers the largest blob as the delta base.
	badBlob := append(blob2, "\nbad blob"...)

	blob1ID := commitBlob(t, repoPath, "refs/heads/branch1", blob1)
	blob2ID := commitBlob(t, repoPath, "refs/tags/tag2", blob2)

	// The bad blob will only be reachable via a non-standard ref. Because of
	// that it should be excluded from delta chains in the main island.
	badBlobID := commitBlob(t, repoPath, "refs/bad/ref3", badBlob)

	// So far we have create blobs and commits but they will be in loose
	// object files; we want them to be delta compressed. Run repack to make
	// that happen.
	testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "repack", "-ad")

	assert.Equal(t, badBlobID, deltaBase(t, repoPath, blob1ID), "expect blob 1 delta base to be bad blob after test setup")
	assert.Equal(t, badBlobID, deltaBase(t, repoPath, blob2ID), "expect blob 2 delta base to be bad blob after test setup")

	require.NoError(t, repack(), "repack after delta island setup")

	assert.Equal(t, blob2ID, deltaBase(t, repoPath, blob1ID), "blob 1 delta base should be blob 2 after repack")

	// blob2 is the bigger of the two so it should be the delta base
	assert.Equal(t, git.ZeroOID.String(), deltaBase(t, repoPath, blob2ID), "blob 2 should not be delta compressed after repack")
}

func commitBlob(t *testing.T, repoPath, ref string, content []byte) string {
	hashObjectOut := testhelper.MustRunCommand(t, bytes.NewReader(content), "git", "-C", repoPath, "hash-object", "-w", "--stdin")
	blobID := chompToString(hashObjectOut)

	treeSpec := fmt.Sprintf("100644 blob %s\tfile\n", blobID)
	mktreeOut := testhelper.MustRunCommand(t, strings.NewReader(treeSpec), "git", "-C", repoPath, "mktree")
	treeID := chompToString(mktreeOut)

	// No parent, that means this will be an initial commit. Not very
	// realistic but it doesn't matter for delta compression.
	commitTreeOut := testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "commit-tree", "-m", "msg", treeID)
	commitID := chompToString(commitTreeOut)

	testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "update-ref", ref, commitID)

	return blobID
}

func deltaBase(t *testing.T, repoPath string, blobID string) string {
	catfileOut := testhelper.MustRunCommand(t, strings.NewReader(blobID), "git", "-C", repoPath, "cat-file", "--batch-check=%(deltabase)")

	return chompToString(catfileOut)
}

func chompToString(s []byte) string { return strings.TrimSuffix(string(s), "\n") }
