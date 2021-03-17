// +build static,system_libgit2

package testhelper

import (
	"testing"
	"time"

	git "github.com/libgit2/git2go/v31"
	"github.com/stretchr/testify/require"
)

// DefaultAuthor is the author used by BuildCommit
var DefaultAuthor = git.Signature{
	Name:  "Foo",
	Email: "foo@example.com",
	When:  time.Date(2020, 1, 1, 1, 1, 1, 1, time.FixedZone("UTC+2", 2*60*60)),
}

func BuildCommit(t testing.TB, repoPath string, parents []*git.Oid, fileContents map[string]string) *git.Oid {
	repo, err := git.OpenRepository(repoPath)
	require.NoError(t, err)
	defer repo.Free()

	odb, err := repo.Odb()
	require.NoError(t, err)

	treeBuilder, err := repo.TreeBuilder()
	require.NoError(t, err)

	for file, contents := range fileContents {
		oid, err := odb.Write([]byte(contents), git.ObjectBlob)
		require.NoError(t, err)
		treeBuilder.Insert(file, oid, git.FilemodeBlob)
	}

	tree, err := treeBuilder.Write()
	require.NoError(t, err)

	var commit *git.Oid
	commit, err = repo.CreateCommitFromIds("", &DefaultAuthor, &DefaultAuthor, "Message", tree, parents...)
	require.NoError(t, err)

	return commit
}

// SignatureEqual checks if the name, email, and timestamp on the given commit
// signatures are equal
func SignatureEqual(t testing.TB, expected, actual *git.Signature) {
	require.Equal(t, expected.Name, actual.Name, "Expected Signature Name to be %s, but was %s", expected.Name, actual.Name)
	require.Equal(t, expected.Email, actual.Email, "Expected Signature Email to be %s, but was %s", expected.Email, actual.Email)

	// Git stores timestamps as Unix times with offset
	require.Equal(t, expected.When.Unix(), actual.When.Unix(), "Expected Signature Unix timestamp to be %v, but was %v", expected.When.Unix(), actual.When.Unix())
	require.Equal(t, expected.Offset(), actual.Offset(), "Expected Signature offset to be %v, but was %v", expected.Offset(), actual.Offset())
}
