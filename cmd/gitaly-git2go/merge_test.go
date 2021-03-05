// +build static,system_libgit2

package main

import (
	"fmt"
	"testing"
	"time"

	git "github.com/libgit2/git2go/v31"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	cmdtesthelper "gitlab.com/gitlab-org/gitaly/cmd/gitaly-git2go/testhelper"
	"gitlab.com/gitlab-org/gitaly/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/internal/git2go"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
)

func TestMergeFailsWithMissingArguments(t *testing.T) {
	_, repoPath, cleanup := gittest.CloneRepo(t)
	defer cleanup()

	testcases := []struct {
		desc        string
		request     git2go.MergeCommand
		expectedErr string
	}{
		{
			desc:        "no arguments",
			expectedErr: "merge: invalid parameters: missing repository",
		},
		{
			desc:        "missing repository",
			request:     git2go.MergeCommand{AuthorName: "Foo", AuthorMail: "foo@example.com", Message: "Foo", Ours: "HEAD", Theirs: "HEAD"},
			expectedErr: "merge: invalid parameters: missing repository",
		},
		{
			desc:        "missing author name",
			request:     git2go.MergeCommand{Repository: repoPath, AuthorMail: "foo@example.com", Message: "Foo", Ours: "HEAD", Theirs: "HEAD"},
			expectedErr: "merge: invalid parameters: missing author name",
		},
		{
			desc:        "missing author mail",
			request:     git2go.MergeCommand{Repository: repoPath, AuthorName: "Foo", Message: "Foo", Ours: "HEAD", Theirs: "HEAD"},
			expectedErr: "merge: invalid parameters: missing author mail",
		},
		{
			desc:        "missing message",
			request:     git2go.MergeCommand{Repository: repoPath, AuthorName: "Foo", AuthorMail: "foo@example.com", Ours: "HEAD", Theirs: "HEAD"},
			expectedErr: "merge: invalid parameters: missing message",
		},
		{
			desc:        "missing ours",
			request:     git2go.MergeCommand{Repository: repoPath, AuthorName: "Foo", AuthorMail: "foo@example.com", Message: "Foo", Theirs: "HEAD"},
			expectedErr: "merge: invalid parameters: missing ours",
		},
		{
			desc:        "missing theirs",
			request:     git2go.MergeCommand{Repository: repoPath, AuthorName: "Foo", AuthorMail: "foo@example.com", Message: "Foo", Ours: "HEAD"},
			expectedErr: "merge: invalid parameters: missing theirs",
		},
	}

	for _, tc := range testcases {
		t.Run(tc.desc, func(t *testing.T) {
			ctx, cancel := testhelper.Context()
			defer cancel()

			_, err := tc.request.Run(ctx, config.Config)
			require.Error(t, err)
			require.Equal(t, tc.expectedErr, err.Error())
		})
	}
}

func TestMergeFailsWithInvalidRepositoryPath(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	_, err := git2go.MergeCommand{
		Repository: "/does/not/exist", AuthorName: "Foo", AuthorMail: "foo@example.com", Message: "Foo", Ours: "HEAD", Theirs: "HEAD",
	}.Run(ctx, config.Config)
	require.Error(t, err)
	require.Contains(t, err.Error(), "merge: could not open repository")
}

func TestMergeTrees(t *testing.T) {
	testcases := []struct {
		desc             string
		base             map[string]string
		ours             map[string]string
		theirs           map[string]string
		expected         map[string]string
		expectedResponse git2go.MergeResult
		expectedStderr   string
	}{
		{
			desc: "trivial merge succeeds",
			base: map[string]string{
				"file": "a",
			},
			ours: map[string]string{
				"file": "a",
			},
			theirs: map[string]string{
				"file": "a",
			},
			expected: map[string]string{
				"file": "a",
			},
			expectedResponse: git2go.MergeResult{
				CommitID: "7d5ae8fb6d2b301c53560bd728004d77778998df",
			},
		},
		{
			desc: "non-trivial merge succeeds",
			base: map[string]string{
				"file": "a\nb\nc\nd\ne\nf\n",
			},
			ours: map[string]string{
				"file": "0\na\nb\nc\nd\ne\nf\n",
			},
			theirs: map[string]string{
				"file": "a\nb\nc\nd\ne\nf\n0\n",
			},
			expected: map[string]string{
				"file": "0\na\nb\nc\nd\ne\nf\n0\n",
			},
			expectedResponse: git2go.MergeResult{
				CommitID: "348b9b489c3ca128a4555c7a51b20335262519c7",
			},
		},
		{
			desc: "multiple files succeed",
			base: map[string]string{
				"1": "foo",
				"2": "bar",
				"3": "qux",
			},
			ours: map[string]string{
				"1": "foo",
				"2": "modified",
				"3": "qux",
			},
			theirs: map[string]string{
				"1": "modified",
				"2": "bar",
				"3": "qux",
			},
			expected: map[string]string{
				"1": "modified",
				"2": "modified",
				"3": "qux",
			},
			expectedResponse: git2go.MergeResult{
				CommitID: "e9be4578f89ea52d44936fb36517e837d698b34b",
			},
		},
		{
			desc: "conflicting merge fails",
			base: map[string]string{
				"1": "foo",
			},
			ours: map[string]string{
				"1": "bar",
			},
			theirs: map[string]string{
				"1": "qux",
			},
			expectedStderr: "merge: could not auto-merge due to conflicts\n",
		},
	}

	for _, tc := range testcases {
		_, repoPath, cleanup := gittest.CloneRepo(t)
		defer cleanup()

		base := cmdtesthelper.BuildCommit(t, repoPath, []*git.Oid{nil}, tc.base)
		ours := cmdtesthelper.BuildCommit(t, repoPath, []*git.Oid{base}, tc.ours)
		theirs := cmdtesthelper.BuildCommit(t, repoPath, []*git.Oid{base}, tc.theirs)

		authorDate := time.Date(2020, 7, 30, 7, 45, 50, 0, time.FixedZone("UTC+2", +2*60*60))

		t.Run(tc.desc, func(t *testing.T) {
			ctx, cancel := testhelper.Context()
			defer cancel()

			response, err := git2go.MergeCommand{
				Repository: repoPath,
				AuthorName: "John Doe",
				AuthorMail: "john.doe@example.com",
				AuthorDate: authorDate,
				Message:    "Merge message",
				Ours:       ours.String(),
				Theirs:     theirs.String(),
			}.Run(ctx, config.Config)

			if tc.expectedStderr != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.expectedStderr)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tc.expectedResponse, response)

			repo, err := git.OpenRepository(repoPath)
			require.NoError(t, err)
			defer repo.Free()

			commitOid, err := git.NewOid(response.CommitID)
			require.NoError(t, err)

			commit, err := repo.LookupCommit(commitOid)
			require.NoError(t, err)

			tree, err := commit.Tree()
			require.NoError(t, err)
			require.EqualValues(t, len(tc.expected), tree.EntryCount())

			for name, contents := range tc.expected {
				entry := tree.EntryByName(name)
				require.NotNil(t, entry)

				blob, err := repo.LookupBlob(entry.Id)
				require.NoError(t, err)
				require.Equal(t, []byte(contents), blob.Contents())
			}
		})
	}
}

func TestMerge_recursive(t *testing.T) {
	_, repoPath, cleanup := gittest.InitBareRepo(t)
	defer cleanup()

	base := cmdtesthelper.BuildCommit(t, repoPath, nil, map[string]string{"base": "base\n"})

	oursContents := map[string]string{"base": "base\n", "ours": "ours-0\n"}
	ours := make([]*git.Oid, git2go.MergeRecursionLimit)
	ours[0] = cmdtesthelper.BuildCommit(t, repoPath, []*git.Oid{base}, oursContents)

	theirsContents := map[string]string{"base": "base\n", "theirs": "theirs-0\n"}
	theirs := make([]*git.Oid, git2go.MergeRecursionLimit)
	theirs[0] = cmdtesthelper.BuildCommit(t, repoPath, []*git.Oid{base}, theirsContents)

	// We're now creating a set of criss-cross merges which look like the following graph:
	//
	//        o---o---o---o---o-   -o---o ours
	//       / \ / \ / \ / \ / \ . / \ /
	// base o   X   X   X   X    .    X
	//       \ / \ / \ / \ / \ / . \ / \
	//        o---o---o---o---o-   -o---o theirs
	//
	// We then merge ours with theirs. The peculiarity about this merge is that the merge base
	// is not unique, and as a result the merge will generate virtual merge bases for each of
	// the criss-cross merges. This operation may thus be heavily expensive to perform.
	for i := 1; i < git2go.MergeRecursionLimit; i++ {
		oursContents["ours"] = fmt.Sprintf("ours-%d\n", i)
		oursContents["theirs"] = fmt.Sprintf("theirs-%d\n", i-1)
		theirsContents["ours"] = fmt.Sprintf("ours-%d\n", i-1)
		theirsContents["theirs"] = fmt.Sprintf("theirs-%d\n", i)

		ours[i] = cmdtesthelper.BuildCommit(t, repoPath, []*git.Oid{ours[i-1], theirs[i-1]}, oursContents)
		theirs[i] = cmdtesthelper.BuildCommit(t, repoPath, []*git.Oid{theirs[i-1], ours[i-1]}, theirsContents)
	}

	authorDate := time.Date(2020, 7, 30, 7, 45, 50, 0, time.FixedZone("UTC+2", +2*60*60))

	ctx, cancel := testhelper.Context()
	defer cancel()

	// When creating the criss-cross merges, we have been doing evil merges
	// as each merge has applied changes from the other side while at the
	// same time incrementing the own file contents. As we exceed the merge
	// limit, git will just pick one of both possible merge bases when
	// hitting that limit instead of computing another virtual merge base.
	// The result is thus a merge of the following three commits:
	//
	// merge base           ours                theirs
	// ----------           ----                ------
	//
	// base:   "base"       base:   "base"      base:   "base"
	// theirs: "theirs-1"   theirs: "theirs-1   theirs: "theirs-2"
	// ours:   "ours-0"     ours:   "ours-2"    ours:   "ours-1"
	//
	// This is a classical merge commit as "ours" differs in all three
	// cases. We thus expect a merge conflict, which unfortunately
	// demonstrates that restricting the recursion limit may cause us to
	// fail resolution.
	_, err := git2go.MergeCommand{
		Repository: repoPath,
		AuthorName: "John Doe",
		AuthorMail: "john.doe@example.com",
		AuthorDate: authorDate,
		Message:    "Merge message",
		Ours:       ours[len(ours)-1].String(),
		Theirs:     theirs[len(theirs)-1].String(),
	}.Run(ctx, config.Config)
	require.Error(t, err)
	require.Equal(t, err.Error(), "merge: could not auto-merge due to conflicts\n")

	// Otherwise, if we're merging an earlier criss-cross merge which has
	// half of the limit many criss-cross patterns, we exactly hit the
	// recursion limit and thus succeed.
	response, err := git2go.MergeCommand{
		Repository: repoPath,
		AuthorName: "John Doe",
		AuthorMail: "john.doe@example.com",
		AuthorDate: authorDate,
		Message:    "Merge message",
		Ours:       ours[git2go.MergeRecursionLimit/2].String(),
		Theirs:     theirs[git2go.MergeRecursionLimit/2].String(),
	}.Run(ctx, config.Config)
	require.NoError(t, err)

	repo, err := git.OpenRepository(repoPath)
	require.NoError(t, err)

	commitOid, err := git.NewOid(response.CommitID)
	require.NoError(t, err)

	commit, err := repo.LookupCommit(commitOid)
	require.NoError(t, err)

	tree, err := commit.Tree()
	require.NoError(t, err)

	require.EqualValues(t, 3, tree.EntryCount())
	for name, contents := range map[string]string{
		"base":   "base\n",
		"ours":   "ours-10\n",
		"theirs": "theirs-10\n",
	} {
		entry := tree.EntryByName(name)
		require.NotNil(t, entry)

		blob, err := repo.LookupBlob(entry.Id)
		require.NoError(t, err)
		require.Equal(t, []byte(contents), blob.Contents())
	}
}
