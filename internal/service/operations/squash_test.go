package operations

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly-proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/internal/git/log"
	"gitlab.com/gitlab-org/gitaly/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"google.golang.org/grpc/codes"
)

var (
	author = &gitalypb.User{
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

	request := &gitalypb.UserSquashRequest{
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

	commit, err := log.GetCommit(ctx, testRepo, response.SquashSha)
	require.NoError(t, err)
	require.Equal(t, []string{startSha}, commit.ParentIds)
	require.Equal(t, author.Name, commit.Author.Name)
	require.Equal(t, author.Email, commit.Author.Email)
	require.Equal(t, user.Name, commit.Committer.Name)
	require.Equal(t, user.Email, commit.Committer.Email)
	require.Equal(t, commitMessage, commit.Subject)
}

func ensureSplitIndexExists(t *testing.T, repoDir string) bool {
	testhelper.MustRunCommand(t, nil, "git", "-C", repoDir, "update-index", "--add")

	fis, err := ioutil.ReadDir(filepath.Join(repoDir, ".git"))
	require.NoError(t, err)
	for _, fi := range fis {
		if strings.HasPrefix(fi.Name(), "sharedindex") {
			return true
		}
	}
	return false
}

func TestSuccessfulUserSquashRequestWith3wayMerge(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	server, serverSocketPath := runOperationServiceServer(t)
	defer server.Stop()

	client, conn := NewOperationClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepoWithWorktree(t)
	defer cleanupFn()

	// The following patch inserts a single line so that we create a
	// file that is slightly different from a branched version of this
	// file.  A 3-way merge must be used in order to merge a branch that
	// alters lines involved in this diff.
	patch := `
diff --git a/files/ruby/popen.rb b/files/ruby/popen.rb
index 5b812ff..ff85394 100644
--- a/files/ruby/popen.rb
+++ b/files/ruby/popen.rb
@@ -19,6 +19,7 @@ module Popen

     @cmd_output = ""
     @cmd_status = 0
+
     Open3.popen3(vars, *cmd, options) do |stdin, stdout, stderr, wait_thr|
       @cmd_output << stdout.read
       @cmd_output << stderr.read
`
	tmpFile, err := ioutil.TempFile(os.TempDir(), "squash-test")
	require.NoError(t, err)

	tmpFile.Write([]byte(patch))
	tmpFile.Close()

	defer os.Remove(tmpFile.Name())

	testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "checkout", "-b", "3way-test", "v1.0.0")
	testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "apply", tmpFile.Name())
	testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "commit", "-m", "test", "-a")
	baseSha := text.ChompBytes(testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "rev-parse", "HEAD"))

	request := &gitalypb.UserSquashRequest{
		Repository:    testRepo,
		User:          user,
		SquashId:      "1",
		Branch:        []byte("3way-test"),
		Author:        author,
		CommitMessage: commitMessage,
		// The diff between two of these commits results in some changes to files/ruby/popen.rb
		StartSha: "6f6d7e7ed97bb5f0054f2b1df789b39ca89b6ff9",
		EndSha:   "570e7b2abdd848b95f2f578043fc23bd6f6fd24d",
	}

	response, err := client.UserSquash(ctx, request)
	require.NoError(t, err)
	require.Empty(t, response.GetGitError())

	commit, err := log.GetCommit(ctx, testRepo, response.SquashSha)
	require.NoError(t, err)
	require.Equal(t, []string{baseSha}, commit.ParentIds)
	require.Equal(t, author.Name, commit.Author.Name)
	require.Equal(t, author.Email, commit.Author.Email)
	require.Equal(t, user.Name, commit.Committer.Name)
	require.Equal(t, user.Email, commit.Committer.Email)
	require.Equal(t, commitMessage, commit.Subject)

	// Ensure Git metadata is cleaned up
	worktreeList := text.ChompBytes(testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "worktree", "list", "--porcelain"))
	expectedOut := fmt.Sprintf("worktree %s\nHEAD %s\nbranch refs/heads/3way-test\n", testRepoPath, baseSha)
	require.Equal(t, expectedOut, worktreeList)

	// Ensure actual worktree is removed
	files, err := ioutil.ReadDir(filepath.Join(testRepoPath, ".git", "gitlab-worktree"))
	require.NoError(t, err)
	require.Equal(t, 0, len(files))
}

func TestSplitIndex(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	server, serverSocketPath := runOperationServiceServer(t)
	defer server.Stop()

	client, conn := NewOperationClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, testRepoPath, cleanup := testhelper.NewTestRepoWithWorktree(t)
	defer cleanup()

	require.False(t, ensureSplitIndexExists(t, testRepoPath))

	request := &gitalypb.UserSquashRequest{
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
	require.True(t, ensureSplitIndexExists(t, testRepoPath))
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

	request := &gitalypb.UserSquashRequest{
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
		request *gitalypb.UserSquashRequest
		code    codes.Code
	}{
		{
			desc: "empty Repository",
			request: &gitalypb.UserSquashRequest{
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
			request: &gitalypb.UserSquashRequest{
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
			request: &gitalypb.UserSquashRequest{
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
			request: &gitalypb.UserSquashRequest{
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
			request: &gitalypb.UserSquashRequest{
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
			request: &gitalypb.UserSquashRequest{
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
			request: &gitalypb.UserSquashRequest{
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
			request: &gitalypb.UserSquashRequest{
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
			require.Contains(t, err.Error(), testCase.desc)
		})
	}
}
