package operations

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/git/log"
	"gitlab.com/gitlab-org/gitaly/internal/git/lstree"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
)

func TestSuccessfulUserUpdateSubmoduleRequest(t *testing.T) {
	stop, serverSocketPath := runOperationServiceServer(t)
	defer stop()

	client, conn := NewOperationClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, testRepoPath, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()

	cleanupSrv := SetupAndStartGitlabServer(t, user.GlId, testRepo.GlRepository)
	defer cleanupSrv()

	commitMessage := []byte("Update Submodule message")

	testCases := []struct {
		desc      string
		submodule string
		commitSha string
		branch    string
	}{
		{
			desc:      "Update submodule",
			submodule: "gitlab-grack",
			commitSha: "41fa1bc9e0f0630ced6a8a211d60c2af425ecc2d",
			branch:    "master",
		},
		{
			desc:      "Update submodule inside folder",
			submodule: "test_inside_folder/another_folder/six",
			commitSha: "e25eda1fece24ac7a03624ed1320f82396f35bd8",
			branch:    "submodule_inside_folder",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			ctx, cancel := testhelper.Context()
			defer cancel()

			request := &gitalypb.UserUpdateSubmoduleRequest{
				Repository:    testRepo,
				User:          user,
				Submodule:     []byte(testCase.submodule),
				CommitSha:     testCase.commitSha,
				Branch:        []byte(testCase.branch),
				CommitMessage: commitMessage,
			}

			response, err := client.UserUpdateSubmodule(ctx, request)
			require.NoError(t, err)
			require.Empty(t, response.GetCommitError())
			require.Empty(t, response.GetPreReceiveError())

			commit, err := log.GetCommit(ctx, testRepo, response.BranchUpdate.CommitId)
			require.NoError(t, err)
			require.Equal(t, commit.Author.Email, user.Email)
			require.Equal(t, commit.Committer.Email, user.Email)
			require.Equal(t, commit.Subject, commitMessage)

			entry := testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "ls-tree", "-z", fmt.Sprintf("%s^{tree}:", response.BranchUpdate.CommitId), testCase.submodule)
			parser := lstree.NewParser(bytes.NewReader(entry))
			parsedEntry, err := parser.NextEntry()
			require.NoError(t, err)
			require.Equal(t, testCase.submodule, string(parsedEntry.Path))
			require.Equal(t, testCase.commitSha, parsedEntry.Oid)
		})
	}
}

func TestFailedUserUpdateSubmoduleRequestDueToValidations(t *testing.T) {
	stop, serverSocketPath := runOperationServiceServer(t)
	defer stop()

	client, conn := NewOperationClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()

	testCases := []struct {
		desc    string
		request *gitalypb.UserUpdateSubmoduleRequest
		code    codes.Code
	}{
		{
			desc: "empty Repository",
			request: &gitalypb.UserUpdateSubmoduleRequest{
				Repository:    nil,
				User:          user,
				Submodule:     []byte("six"),
				CommitSha:     "db54006ff1c999fd485af44581dabe9b6c85a701",
				Branch:        []byte("some-branch"),
				CommitMessage: []byte("Update Submodule message"),
			},
			code: codes.InvalidArgument,
		},
		{
			desc: "empty User",
			request: &gitalypb.UserUpdateSubmoduleRequest{
				Repository:    testRepo,
				User:          nil,
				Submodule:     []byte("six"),
				CommitSha:     "db54006ff1c999fd485af44581dabe9b6c85a701",
				Branch:        []byte("some-branch"),
				CommitMessage: []byte("Update Submodule message"),
			},
			code: codes.InvalidArgument,
		},
		{
			desc: "empty Submodule",
			request: &gitalypb.UserUpdateSubmoduleRequest{
				Repository:    testRepo,
				User:          user,
				Submodule:     nil,
				CommitSha:     "db54006ff1c999fd485af44581dabe9b6c85a701",
				Branch:        []byte("some-branch"),
				CommitMessage: []byte("Update Submodule message"),
			},
			code: codes.InvalidArgument,
		},
		{
			desc: "empty CommitSha",
			request: &gitalypb.UserUpdateSubmoduleRequest{
				Repository:    testRepo,
				User:          user,
				Submodule:     []byte("six"),
				CommitSha:     "",
				Branch:        []byte("some-branch"),
				CommitMessage: []byte("Update Submodule message"),
			},
			code: codes.InvalidArgument,
		},
		{
			desc: "invalid CommitSha",
			request: &gitalypb.UserUpdateSubmoduleRequest{
				Repository:    testRepo,
				User:          user,
				Submodule:     []byte("six"),
				CommitSha:     "foobar",
				Branch:        []byte("some-branch"),
				CommitMessage: []byte("Update Submodule message"),
			},
			code: codes.InvalidArgument,
		},
		{
			desc: "invalid CommitSha",
			request: &gitalypb.UserUpdateSubmoduleRequest{
				Repository:    testRepo,
				User:          user,
				Submodule:     []byte("six"),
				CommitSha:     "db54006ff1c999fd485a",
				Branch:        []byte("some-branch"),
				CommitMessage: []byte("Update Submodule message"),
			},
			code: codes.InvalidArgument,
		},
		{
			desc: "empty Branch",
			request: &gitalypb.UserUpdateSubmoduleRequest{
				Repository:    testRepo,
				User:          user,
				Submodule:     []byte("six"),
				CommitSha:     "db54006ff1c999fd485af44581dabe9b6c85a701",
				Branch:        nil,
				CommitMessage: []byte("Update Submodule message"),
			},
			code: codes.InvalidArgument,
		},
		{
			desc: "empty CommitMessage",
			request: &gitalypb.UserUpdateSubmoduleRequest{
				Repository:    testRepo,
				User:          user,
				Submodule:     []byte("six"),
				CommitSha:     "db54006ff1c999fd485af44581dabe9b6c85a701",
				Branch:        []byte("some-branch"),
				CommitMessage: nil,
			},
			code: codes.InvalidArgument,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			ctx, cancel := testhelper.Context()
			defer cancel()

			_, err := client.UserUpdateSubmodule(ctx, testCase.request)
			testhelper.RequireGrpcError(t, err, testCase.code)
			require.Contains(t, err.Error(), testCase.desc)
		})
	}
}

func TestFailedUserUpdateSubmoduleRequestDueToInvalidBranch(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	stop, serverSocketPath := runOperationServiceServer(t)
	defer stop()

	client, conn := NewOperationClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()

	request := &gitalypb.UserUpdateSubmoduleRequest{
		Repository:    testRepo,
		User:          user,
		Submodule:     []byte("six"),
		CommitSha:     "db54006ff1c999fd485af44581dabe9b6c85a701",
		Branch:        []byte("non/existent"),
		CommitMessage: []byte("Update Submodule message"),
	}

	_, err := client.UserUpdateSubmodule(ctx, request)
	testhelper.RequireGrpcError(t, err, codes.InvalidArgument)
	require.Contains(t, err.Error(), "Cannot find branch")
}

func TestFailedUserUpdateSubmoduleRequestDueToInvalidSubmodule(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	stop, serverSocketPath := runOperationServiceServer(t)
	defer stop()

	client, conn := NewOperationClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()

	request := &gitalypb.UserUpdateSubmoduleRequest{
		Repository:    testRepo,
		User:          user,
		Submodule:     []byte("non-existent-submodule"),
		CommitSha:     "db54006ff1c999fd485af44581dabe9b6c85a701",
		Branch:        []byte("master"),
		CommitMessage: []byte("Update Submodule message"),
	}

	response, err := client.UserUpdateSubmodule(ctx, request)
	require.NoError(t, err)
	require.Equal(t, response.CommitError, "Invalid submodule path")
}

func TestFailedUserUpdateSubmoduleRequestDueToSameReference(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	stop, serverSocketPath := runOperationServiceServer(t)
	defer stop()

	client, conn := NewOperationClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()

	cleanupSrv := SetupAndStartGitlabServer(t, user.GlId, testRepo.GlRepository)
	defer cleanupSrv()

	request := &gitalypb.UserUpdateSubmoduleRequest{
		Repository:    testRepo,
		User:          user,
		Submodule:     []byte("six"),
		CommitSha:     "41fa1bc9e0f0630ced6a8a211d60c2af425ecc2d",
		Branch:        []byte("master"),
		CommitMessage: []byte("Update Submodule message"),
	}

	_, err := client.UserUpdateSubmodule(ctx, request)
	require.NoError(t, err)

	response, err := client.UserUpdateSubmodule(ctx, request)
	require.NoError(t, err)
	require.Contains(t, response.CommitError, "is already at")
}

func TestFailedUserUpdateSubmoduleRequestDueToRepositoryEmpty(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	stop, serverSocketPath := runOperationServiceServer(t)
	defer stop()

	client, conn := NewOperationClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanup := testhelper.InitRepoWithWorktree(t)
	defer cleanup()

	request := &gitalypb.UserUpdateSubmoduleRequest{
		Repository:    testRepo,
		User:          user,
		Submodule:     []byte("six"),
		CommitSha:     "41fa1bc9e0f0630ced6a8a211d60c2af425ecc2d",
		Branch:        []byte("master"),
		CommitMessage: []byte("Update Submodule message"),
	}

	response, err := client.UserUpdateSubmodule(ctx, request)
	require.NoError(t, err)
	require.Equal(t, response.CommitError, "Repository is empty")
}
