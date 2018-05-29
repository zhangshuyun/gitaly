package operations

import (
	"testing"

	"github.com/stretchr/testify/require"
	pb "gitlab.com/gitlab-org/gitaly-proto/go"
	"gitlab.com/gitlab-org/gitaly/internal/git/log"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"google.golang.org/grpc/codes"
)

var (
	user = &pb.User{
		Name:  []byte("Jane Doe"),
		Email: []byte("janedoe@gitlab.com"),
		GlId:  "user-123",
	}
	author = &pb.User{
		Name:  []byte("John Doe"),
		Email: []byte("johndoe@gitlab.com"),
	}
	branchName    = "not-merged-branch"
	startSha      = "b83d6e391c22777fca1ed3012fce84f633d7fed0"
	endSha        = "54cec5282aa9f21856362fe321c800c236a61615"
	commitMessage = []byte("Squash message")
)

func TestSuccessfulUserSquashRequest(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	server, serverSocketPath := runOperationServiceServer(t)
	defer server.Stop()

	client, conn := NewOperationClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()

	request := &pb.UserSquashRequest{
		Repository:    testRepo,
		User:          user,
		SquashId:      "1",
		Branch:        []byte(branchName),
		Author:        author,
		CommitMessage: commitMessage,
		StartSha:      startSha,
		EndSha:        endSha,
	}

	response, err := client.UserSquash(ctx, request)
	require.NoError(t, err)
	require.Empty(t, response.GetGitError())

	commit, err := log.GetCommit(ctx, testRepo, response.SquashSha, "")
	require.NoError(t, err)
	require.Equal(t, commit.ParentIds, []string{startSha})
	require.Equal(t, string(commit.Author.Email), "johndoe@gitlab.com")
	require.Equal(t, string(commit.Committer.Email), "janedoe@gitlab.com")
	require.Equal(t, commit.Subject, commitMessage)
}

func TestFailedUserSquashRequestDueToGitError(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	server, serverSocketPath := runOperationServiceServer(t)
	defer server.Stop()

	client, conn := NewOperationClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()

	conflictingStartSha := "bbd36ad238d14e1c03ece0f3358f545092dc9ca3"
	branchName := "gitaly-stuff"

	request := &pb.UserSquashRequest{
		Repository:    testRepo,
		User:          user,
		SquashId:      "1",
		Branch:        []byte(branchName),
		Author:        author,
		CommitMessage: commitMessage,
		StartSha:      conflictingStartSha,
		EndSha:        endSha,
	}

	response, err := client.UserSquash(ctx, request)
	require.NoError(t, err)
	require.Contains(t, response.GitError, "error: large_diff_old_name.md: does not exist in index")
}

func TestFailedUserSquashRequestDueToValidations(t *testing.T) {
	server, serverSocketPath := runOperationServiceServer(t)
	defer server.Stop()

	client, conn := NewOperationClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()

	testCases := []struct {
		desc    string
		request *pb.UserSquashRequest
		code    codes.Code
	}{
		{
			desc: "empty Repository",
			request: &pb.UserSquashRequest{
				Repository:    nil,
				User:          user,
				SquashId:      "1",
				Branch:        []byte("some-branch"),
				Author:        user,
				CommitMessage: commitMessage,
				StartSha:      startSha,
				EndSha:        endSha,
			},
			code: codes.InvalidArgument,
		},
		{
			desc: "empty User",
			request: &pb.UserSquashRequest{
				Repository:    testRepo,
				User:          nil,
				SquashId:      "1",
				Branch:        []byte("some-branch"),
				Author:        user,
				CommitMessage: commitMessage,
				StartSha:      startSha,
				EndSha:        endSha,
			},
			code: codes.InvalidArgument,
		},
		{
			desc: "empty SquashId",
			request: &pb.UserSquashRequest{
				Repository:    testRepo,
				User:          user,
				SquashId:      "",
				Branch:        []byte("some-branch"),
				Author:        user,
				CommitMessage: commitMessage,
				StartSha:      startSha,
				EndSha:        endSha,
			},
			code: codes.InvalidArgument,
		},
		{
			desc: "empty Branch",
			request: &pb.UserSquashRequest{
				Repository:    testRepo,
				User:          user,
				SquashId:      "1",
				Branch:        nil,
				Author:        user,
				CommitMessage: commitMessage,
				StartSha:      startSha,
				EndSha:        endSha,
			},
			code: codes.InvalidArgument,
		},
		{
			desc: "empty StartSha",
			request: &pb.UserSquashRequest{
				Repository:    testRepo,
				User:          user,
				SquashId:      "1",
				Branch:        []byte("some-branch"),
				Author:        user,
				CommitMessage: commitMessage,
				StartSha:      "",
				EndSha:        endSha,
			},
			code: codes.InvalidArgument,
		},
		{
			desc: "empty EndSha",
			request: &pb.UserSquashRequest{
				Repository:    testRepo,
				User:          user,
				SquashId:      "1",
				Branch:        []byte("some-branch"),
				Author:        user,
				CommitMessage: commitMessage,
				StartSha:      startSha,
				EndSha:        "",
			},
			code: codes.InvalidArgument,
		},
		{
			desc: "empty Author",
			request: &pb.UserSquashRequest{
				Repository:    testRepo,
				User:          user,
				SquashId:      "1",
				Branch:        []byte("some-branch"),
				Author:        nil,
				CommitMessage: commitMessage,
				StartSha:      startSha,
				EndSha:        endSha,
			},
			code: codes.InvalidArgument,
		},
		{
			desc: "empty CommitMessage",
			request: &pb.UserSquashRequest{
				Repository:    testRepo,
				User:          user,
				SquashId:      "1",
				Branch:        []byte("some-branch"),
				Author:        user,
				CommitMessage: nil,
				StartSha:      startSha,
				EndSha:        endSha,
			},
			code: codes.InvalidArgument,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			ctx, cancel := testhelper.Context()
			defer cancel()

			_, err := client.UserSquash(ctx, testCase.request)
			testhelper.RequireGrpcError(t, err, testCase.code)
		})
	}
}
