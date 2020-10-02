// +build static,system_libgit2

package main

import (
	"testing"
	"time"

	git "github.com/libgit2/git2go/v30"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v13/internal/git2go"
	"gitlab.com/gitlab-org/gitaly/v13/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v13/internal/testhelper"
)

func TestMergeFailsWithMissingArguments(t *testing.T) {
	_, repoPath, cleanup := testhelper.NewTestRepo(t)
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

func buildCommit(t *testing.T, repoPath string, parent *git.Oid, fileContents map[string]string) *git.Oid {
	repo, err := git.OpenRepository(repoPath)
	require.NoError(t, err)

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
	if parent != nil {
		commit, err = repo.CreateCommitFromIds("", &committer, &committer, "Message", tree, parent)
	} else {
		commit, err = repo.CreateCommitFromIds("", &committer, &committer, "Message", tree)
	}
	require.NoError(t, err)

	return commit
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
		_, repoPath, cleanup := testhelper.NewTestRepo(t)
		defer cleanup()

		base := buildCommit(t, repoPath, nil, tc.base)
		ours := buildCommit(t, repoPath, base, tc.ours)
		theirs := buildCommit(t, repoPath, base, tc.theirs)

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
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tc.expectedStderr)
				return
			}

			assert.NoError(t, err)
			assert.Equal(t, tc.expectedResponse, response)

			repo, err := git.OpenRepository(repoPath)
			require.NoError(t, err)

			commitOid, err := git.NewOid(response.CommitID)
			require.NoError(t, err)

			commit, err := repo.LookupCommit(commitOid)
			require.NoError(t, err)

			tree, err := commit.Tree()
			require.NoError(t, err)
			require.Equal(t, uint64(len(tc.expected)), tree.EntryCount())

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
