// +build static,system_libgit2

package main

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/lstree"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git2go"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testcfg"
)

func TestSubmodule(t *testing.T) {
	commitMessage := []byte("Update Submodule message")

	testCases := []struct {
		desc           string
		command        git2go.SubmoduleCommand
		expectedStderr string
	}{
		{
			desc: "Update submodule",
			command: git2go.SubmoduleCommand{
				AuthorName: string(gittest.TestUser.Name),
				AuthorMail: string(gittest.TestUser.Email),
				Message:    string(commitMessage),
				CommitSHA:  "41fa1bc9e0f0630ced6a8a211d60c2af425ecc2d",
				Submodule:  "gitlab-grack",
				Branch:     "master",
			},
		},
		{
			desc: "Update submodule inside folder",
			command: git2go.SubmoduleCommand{
				AuthorName: string(gittest.TestUser.Name),
				AuthorMail: string(gittest.TestUser.Email),
				Message:    string(commitMessage),
				CommitSHA:  "e25eda1fece24ac7a03624ed1320f82396f35bd8",
				Submodule:  "test_inside_folder/another_folder/six",
				Branch:     "submodule_inside_folder",
			},
		},
		{
			desc: "Invalid branch",
			command: git2go.SubmoduleCommand{
				AuthorName: string(gittest.TestUser.Name),
				AuthorMail: string(gittest.TestUser.Email),
				Message:    string(commitMessage),
				CommitSHA:  "e25eda1fece24ac7a03624ed1320f82396f35bd8",
				Submodule:  "test_inside_folder/another_folder/six",
				Branch:     "non/existent",
			},
			expectedStderr: "Invalid branch",
		},
		{
			desc: "Invalid submodule",
			command: git2go.SubmoduleCommand{
				AuthorName: string(gittest.TestUser.Name),
				AuthorMail: string(gittest.TestUser.Email),
				Message:    string(commitMessage),
				CommitSHA:  "e25eda1fece24ac7a03624ed1320f82396f35bd8",
				Submodule:  "non-existent-submodule",
				Branch:     "master",
			},
			expectedStderr: "Invalid submodule path",
		},
		{
			desc: "Duplicate reference",
			command: git2go.SubmoduleCommand{
				AuthorName: string(gittest.TestUser.Name),
				AuthorMail: string(gittest.TestUser.Email),
				Message:    string(commitMessage),
				CommitSHA:  "409f37c4f05865e4fb208c771485f211a22c4c2d",
				Submodule:  "six",
				Branch:     "master",
			},
			expectedStderr: "The submodule six is already at 409f37c4f",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			cfg, repoProto, repoPath := testcfg.BuildWithRepo(t)
			testhelper.ConfigureGitalyGit2GoBin(t, cfg)
			repo := localrepo.NewTestRepo(t, cfg, repoProto)

			tc.command.Repository = repoPath

			ctx, cancel := testhelper.Context()
			defer cancel()

			response, err := tc.command.Run(ctx, cfg)
			if tc.expectedStderr != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.expectedStderr)
				return
			}
			require.NoError(t, err)

			commit, err := repo.ReadCommit(ctx, git.Revision(response.CommitID))
			require.NoError(t, err)
			require.Equal(t, commit.Author.Email, gittest.TestUser.Email)
			require.Equal(t, commit.Committer.Email, gittest.TestUser.Email)
			require.Equal(t, commit.Subject, commitMessage)

			entry := gittest.Exec(
				t,
				cfg,
				"-C",
				repoPath,
				"ls-tree",
				"-z",
				fmt.Sprintf("%s^{tree}:", response.CommitID),
				tc.command.Submodule,
			)
			parser := lstree.NewParser(bytes.NewReader(entry))
			parsedEntry, err := parser.NextEntry()
			require.NoError(t, err)
			require.Equal(t, tc.command.Submodule, parsedEntry.Path)
			require.Equal(t, tc.command.CommitSHA, parsedEntry.Oid)
		})
	}
}
