package operations

import (
	"context"
	"fmt"
	"io/ioutil"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/git/log"
	"gitlab.com/gitlab-org/gitaly/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
)

var (
	author = &gitalypb.User{
		Name:  []byte("John Doe"),
		Email: []byte("johndoe@gitlab.com"),
		GlId:  testhelper.GlID,
	}
	branchName    = "not-merged-branch"
	startSha      = "b83d6e391c22777fca1ed3012fce84f633d7fed0"
	endSha        = "54cec5282aa9f21856362fe321c800c236a61615"
	commitMessage = []byte("Squash message")
)

func TestSuccessfulUserSquashRequest(t *testing.T) {
	featureSet, err := testhelper.NewFeatureSets(nil, featureflag.GitalyRubyCallHookRPC, featureflag.GoUpdateHook)
	require.NoError(t, err)
	ctx, cancel := testhelper.Context()
	defer cancel()

	for _, features := range featureSet {
		t.Run(features.String(), func(t *testing.T) {
			ctx = features.WithParent(ctx)
			testSuccessfulUserSquashRequest(t, ctx)
		})
	}
}

func testSuccessfulUserSquashRequest(t *testing.T, ctx context.Context) {
	serverSocketPath, stop := runOperationServiceServer(t)
	defer stop()

	client, conn := newOperationClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()

	request := &gitalypb.UserSquashRequest{
		Repository:    testRepo,
		User:          testhelper.TestUser,
		SquashId:      "1",
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
	require.Equal(t, testhelper.TestUser.Name, commit.Committer.Name)
	require.Equal(t, testhelper.TestUser.Email, commit.Committer.Email)
	require.Equal(t, commitMessage, commit.Subject)
}

func ensureSplitIndexExists(t *testing.T, repoDir string) bool {
	testhelper.MustRunCommand(t, nil, "git", "-C", repoDir, "update-index", "--add")

	fis, err := ioutil.ReadDir(repoDir)
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

	serverSocketPath, stop := runOperationServiceServer(t)
	defer stop()

	client, conn := newOperationClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	request := &gitalypb.UserSquashRequest{
		Repository:    testRepo,
		User:          testhelper.TestUser,
		SquashId:      "1",
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
	require.Equal(t, []string{"6f6d7e7ed97bb5f0054f2b1df789b39ca89b6ff9"}, commit.ParentIds)
	require.Equal(t, author.Name, commit.Author.Name)
	require.Equal(t, author.Email, commit.Author.Email)
	require.Equal(t, testhelper.TestUser.Name, commit.Committer.Name)
	require.Equal(t, testhelper.TestUser.Email, commit.Committer.Email)
	require.Equal(t, commitMessage, commit.Subject)

	// Ensure Git metadata is cleaned up
	worktreeList := text.ChompBytes(testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "worktree", "list", "--porcelain"))
	expectedOut := fmt.Sprintf("worktree %s\nbare\n", testRepoPath)
	require.Equal(t, expectedOut, worktreeList)

	// Ensure actual worktree is removed
	files, err := ioutil.ReadDir(filepath.Join(testRepoPath, "gitlab-worktree"))
	require.NoError(t, err)
	require.Equal(t, 0, len(files))
}

func TestSplitIndex(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	serverSocketPath, stop := runOperationServiceServer(t)
	defer stop()

	client, conn := newOperationClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, testRepoPath, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()

	require.False(t, ensureSplitIndexExists(t, testRepoPath))

	request := &gitalypb.UserSquashRequest{
		Repository:    testRepo,
		User:          testhelper.TestUser,
		SquashId:      "1",
		Author:        author,
		CommitMessage: commitMessage,
		StartSha:      startSha,
		EndSha:        endSha,
	}

	response, err := client.UserSquash(ctx, request)
	require.NoError(t, err)
	require.Empty(t, response.GetGitError())
	require.False(t, ensureSplitIndexExists(t, testRepoPath))
}

func TestSquashRequestWithRenamedFiles(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	serverSocketPath, stop := runOperationServiceServer(t)
	defer stop()

	client, conn := newOperationClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepoWithWorktree(t)
	defer cleanupFn()

	originalFilename := "original-file.txt"
	renamedFilename := "renamed-file.txt"

	testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "config", "testhelper.TestUser.name", string(author.Name))
	testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "config", "testhelper.TestUser.email", string(author.Email))
	testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "checkout", "-b", "squash-rename-test", "master")
	require.NoError(t, ioutil.WriteFile(filepath.Join(testRepoPath, originalFilename), []byte("This is a test"), 0644))
	testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "add", ".")
	testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "commit", "-m", "test file")

	startCommitID := text.ChompBytes(testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "rev-parse", "HEAD"))

	testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "mv", originalFilename, renamedFilename)
	testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "commit", "-a", "-m", "renamed test file")

	// Modify the original file in another branch
	testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "checkout", "-b", "squash-rename-branch", startCommitID)
	require.NoError(t, ioutil.WriteFile(filepath.Join(testRepoPath, originalFilename), []byte("This is a change"), 0644))
	testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "commit", "-a", "-m", "test")

	require.NoError(t, ioutil.WriteFile(filepath.Join(testRepoPath, originalFilename), []byte("This is another change"), 0644))
	testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "commit", "-a", "-m", "test")

	endCommitID := text.ChompBytes(testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "rev-parse", "HEAD"))

	request := &gitalypb.UserSquashRequest{
		Repository:    testRepo,
		User:          testhelper.TestUser,
		SquashId:      "1",
		Author:        author,
		CommitMessage: commitMessage,
		StartSha:      startCommitID,
		EndSha:        endCommitID,
	}

	response, err := client.UserSquash(ctx, request)
	require.NoError(t, err)
	require.Empty(t, response.GetGitError())

	commit, err := log.GetCommit(ctx, testRepo, response.SquashSha)
	require.NoError(t, err)
	require.Equal(t, []string{startCommitID}, commit.ParentIds)
	require.Equal(t, author.Name, commit.Author.Name)
	require.Equal(t, author.Email, commit.Author.Email)
	require.Equal(t, testhelper.TestUser.Name, commit.Committer.Name)
	require.Equal(t, testhelper.TestUser.Email, commit.Committer.Email)
	require.Equal(t, commitMessage, commit.Subject)
}

func TestSuccessfulUserSquashRequestWithMissingFileOnTargetBranch(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	serverSocketPath, stop := runOperationServiceServer(t)
	defer stop()

	client, conn := newOperationClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()

	conflictingStartSha := "bbd36ad238d14e1c03ece0f3358f545092dc9ca3"

	request := &gitalypb.UserSquashRequest{
		Repository:    testRepo,
		User:          testhelper.TestUser,
		SquashId:      "1",
		Author:        author,
		CommitMessage: commitMessage,
		StartSha:      conflictingStartSha,
		EndSha:        endSha,
	}

	response, err := client.UserSquash(ctx, request)
	require.NoError(t, err)
	require.Empty(t, response.GetGitError())
}

func TestFailedUserSquashRequestDueToValidations(t *testing.T) {
	serverSocketPath, stop := runOperationServiceServer(t)
	defer stop()

	client, conn := newOperationClient(t, serverSocketPath)
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
				User:          testhelper.TestUser,
				SquashId:      "1",
				Author:        author,
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
				Author:        author,
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
				User:          testhelper.TestUser,
				SquashId:      "",
				Author:        author,
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
				User:          testhelper.TestUser,
				SquashId:      "1",
				Author:        author,
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
				User:          testhelper.TestUser,
				SquashId:      "1",
				Author:        author,
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
				User:          testhelper.TestUser,
				SquashId:      "1",
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
				User:          testhelper.TestUser,
				SquashId:      "1",
				Author:        author,
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
