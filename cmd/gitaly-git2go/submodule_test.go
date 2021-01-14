// +build static,system_libgit2

package main

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/log"
	"gitlab.com/gitlab-org/gitaly/internal/git/lstree"
	"gitlab.com/gitlab-org/gitaly/internal/git2go"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
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
				AuthorName: string(testhelper.TestUser.Name),
				AuthorMail: string(testhelper.TestUser.Email),
				Message:    string(commitMessage),
				CommitSHA:  "41fa1bc9e0f0630ced6a8a211d60c2af425ecc2d",
				Submodule:  "gitlab-grack",
				Branch:     "master",
			},
		},
		{
			desc: "Update submodule inside folder",
			command: git2go.SubmoduleCommand{
				AuthorName: string(testhelper.TestUser.Name),
				AuthorMail: string(testhelper.TestUser.Email),
				Message:    string(commitMessage),
				CommitSHA:  "e25eda1fece24ac7a03624ed1320f82396f35bd8",
				Submodule:  "test_inside_folder/another_folder/six",
				Branch:     "submodule_inside_folder",
			},
		},
		{
			desc: "Invalid branch",
			command: git2go.SubmoduleCommand{
				AuthorName: string(testhelper.TestUser.Name),
				AuthorMail: string(testhelper.TestUser.Email),
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
				AuthorName: string(testhelper.TestUser.Name),
				AuthorMail: string(testhelper.TestUser.Email),
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
				AuthorName: string(testhelper.TestUser.Name),
				AuthorMail: string(testhelper.TestUser.Email),
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
			testRepo, testRepoPath, cleanup := testhelper.NewTestRepo(t)
			defer cleanup()

			tc.command.Repository = testRepoPath

			ctx, cancel := testhelper.Context()
			defer cancel()

			response, err := tc.command.Run(ctx, config.Config)
			if tc.expectedStderr != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.expectedStderr)
				return
			}
			require.NoError(t, err)

			commit, err := log.GetCommit(ctx, config.NewLocator(config.Config), testRepo, git.Revision(response.CommitID))
			require.NoError(t, err)
			require.Equal(t, commit.Author.Email, testhelper.TestUser.Email)
			require.Equal(t, commit.Committer.Email, testhelper.TestUser.Email)
			require.Equal(t, commit.Subject, commitMessage)

			entry := testhelper.MustRunCommand(
				t,
				nil,
				"git",
				"-C",
				testRepoPath,
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
