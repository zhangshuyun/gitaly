// +build static,system_libgit2

package command

import (
	"testing"
	"time"

	git "github.com/libgit2/git2go/v30"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/git2go"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
)

func TestRevert_validation(t *testing.T) {
	_, repoPath, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()

	testcases := []struct {
		desc        string
		request     git2go.RevertCommand
		expectedErr string
	}{
		{
			desc:        "no arguments",
			expectedErr: "revert: invalid parameters: missing repository",
		},
		{
			desc:        "missing repository",
			request:     git2go.RevertCommand{AuthorName: "Foo", AuthorMail: "foo@example.com", Message: "Foo", Ours: "HEAD", Revert: "HEAD"},
			expectedErr: "revert: invalid parameters: missing repository",
		},
		{
			desc:        "missing author name",
			request:     git2go.RevertCommand{Repository: repoPath, AuthorMail: "foo@example.com", Message: "Foo", Ours: "HEAD", Revert: "HEAD"},
			expectedErr: "revert: invalid parameters: missing author name",
		},
		{
			desc:        "missing author mail",
			request:     git2go.RevertCommand{Repository: repoPath, AuthorName: "Foo", Message: "Foo", Ours: "HEAD", Revert: "HEAD"},
			expectedErr: "revert: invalid parameters: missing author mail",
		},
		{
			desc:        "missing message",
			request:     git2go.RevertCommand{Repository: repoPath, AuthorName: "Foo", AuthorMail: "foo@example.com", Ours: "HEAD", Revert: "HEAD"},
			expectedErr: "revert: invalid parameters: missing message",
		},
		{
			desc:        "missing ours",
			request:     git2go.RevertCommand{Repository: repoPath, AuthorName: "Foo", AuthorMail: "foo@example.com", Message: "Foo", Revert: "HEAD"},
			expectedErr: "revert: invalid parameters: missing ours",
		},
		{
			desc:        "missing revert",
			request:     git2go.RevertCommand{Repository: repoPath, AuthorName: "Foo", AuthorMail: "foo@example.com", Message: "Foo", Ours: "HEAD"},
			expectedErr: "revert: invalid parameters: missing revert",
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

func TestRevert_trees(t *testing.T) {
	testcases := []struct {
		desc             string
		setupRepo        func(t testing.TB, repoPath string) (ours, revert string)
		expected         map[string]string
		expectedResponse git2go.RevertResult
		expectedStderr   string
	}{
		{
			desc: "trivial revert succeeds",
			setupRepo: func(t testing.TB, repoPath string) (ours, revert string) {
				baseOid := buildCommit(t, repoPath, nil, map[string]string{
					"a": "apple",
					"b": "banana",
				})
				revertOid := buildCommit(t, repoPath, baseOid, map[string]string{
					"a": "apple",
					"b": "pineapple",
				})
				oursOid := buildCommit(t, repoPath, revertOid, map[string]string{
					"a": "apple",
					"b": "pineapple",
					"c": "carrot",
				})

				return oursOid.String(), revertOid.String()
			},
			expected: map[string]string{
				"a": "apple",
				"b": "banana",
				"c": "carrot",
			},
			expectedResponse: git2go.RevertResult{
				CommitID: "0be709b57f18ad3f592a8cb7c57f920861392573",
			},
		},
		{
			desc: "conflicting revert fails",
			setupRepo: func(t testing.TB, repoPath string) (ours, revert string) {
				baseOid := buildCommit(t, repoPath, nil, map[string]string{
					"a": "apple",
				})
				revertOid := buildCommit(t, repoPath, baseOid, map[string]string{
					"a": "pineapple",
				})
				oursOid := buildCommit(t, repoPath, revertOid, map[string]string{
					"a": "carrot",
				})

				return oursOid.String(), revertOid.String()
			},
			expectedStderr: "revert: could not revert due to conflicts\n",
		},
		{
			desc: "nonexistent ours fails",
			setupRepo: func(t testing.TB, repoPath string) (ours, revert string) {
				revertOid := buildCommit(t, repoPath, nil, map[string]string{
					"a": "apple",
				})

				return "nonexistent", revertOid.String()
			},
			expectedStderr: "revert: ours commit lookup: could not lookup reference \"nonexistent\": revspec 'nonexistent' not found\n",
		},
		{
			desc: "nonexistent revert fails",
			setupRepo: func(t testing.TB, repoPath string) (ours, revert string) {
				oursOid := buildCommit(t, repoPath, nil, map[string]string{
					"a": "apple",
				})

				return oursOid.String(), "nonexistent"
			},
			expectedStderr: "revert: revert commit lookup: could not lookup reference \"nonexistent\": revspec 'nonexistent' not found\n",
		},
	}
	for _, tc := range testcases {
		t.Run(tc.desc, func(t *testing.T) {
			_, repoPath, cleanup := testhelper.NewTestRepo(t)
			defer cleanup()

			ours, revert := tc.setupRepo(t, repoPath)

			ctx, cancel := testhelper.Context()
			defer cancel()

			authorDate := time.Date(2020, 7, 30, 7, 45, 50, 0, time.FixedZone("UTC+2", +2*60*60))

			request := git2go.RevertCommand{
				Repository: repoPath,
				AuthorName: "Foo",
				AuthorMail: "foo@example.com",
				AuthorDate: authorDate,
				Message:    "Foo",
				Ours:       ours,
				Revert:     revert,
			}

			response, err := request.Run(ctx, config.Config)

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
