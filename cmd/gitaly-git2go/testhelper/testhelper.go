// +build static,system_libgit2

package testhelper

import (
	"testing"
	"time"

	git "github.com/libgit2/git2go/v31"
	"github.com/stretchr/testify/require"
)

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

	committer := git.Signature{
		Name:  "Foo",
		Email: "foo@example.com",
		When:  time.Date(2020, 1, 1, 1, 1, 1, 1, time.FixedZone("UTC+2", 2*60*60)),
	}

	var commit *git.Oid
	commit, err = repo.CreateCommitFromIds("", &committer, &committer, "Message", tree, parents...)
	require.NoError(t, err)

	return commit
}
