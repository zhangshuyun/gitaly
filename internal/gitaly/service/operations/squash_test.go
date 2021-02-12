package operations

import (
	"context"
	"fmt"
	"io/ioutil"
	"path/filepath"
	"strings"
	"testing"

	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

	t.Run("with sparse checkout", func(t *testing.T) {
		testSuccessfulUserSquashRequest(t, ctx, startSha, endSha)
	})

	t.Run("without sparse checkout", func(t *testing.T) {
		// there are no files that could be used for sparse checkout for those two commits
		testSuccessfulUserSquashRequest(t, ctx, "60ecb67744cb56576c30214ff52294f8ce2def98", "c84ff944ff4529a70788a5e9003c2b7feae29047")
	})
}

func testSuccessfulUserSquashRequest(t *testing.T, ctx context.Context, start, end string) {
	serverSocketPath, stop := runOperationServiceServer(t)
	defer stop()

	client, conn := newOperationClient(t, serverSocketPath)
	defer conn.Close()

	repoProto, repoPath, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()
	repo := localrepo.New(git.NewExecCommandFactory(config.Config), repoProto, config.Config)

	request := &gitalypb.UserSquashRequest{
		Repository:    repoProto,
		User:          testhelper.TestUser,
		SquashId:      "1",
		Author:        author,
		CommitMessage: commitMessage,
		StartSha:      start,
		EndSha:        end,
	}

	response, err := client.UserSquash(ctx, request)
	require.NoError(t, err)
	require.Empty(t, response.GetGitError())

	commit, err := repo.ReadCommit(ctx, git.Revision(response.SquashSha))
	require.NoError(t, err)
	require.Equal(t, []string{start}, commit.ParentIds)
	require.Equal(t, author.Name, commit.Author.Name)
	require.Equal(t, author.Email, commit.Author.Email)
	require.Equal(t, testhelper.TestUser.Name, commit.Committer.Name)
	require.Equal(t, testhelper.TestUser.Email, commit.Committer.Email)
	require.Equal(t, commitMessage, commit.Subject)

	treeData := testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "ls-tree", "--name-only", response.SquashSha)
	files := strings.Fields(text.ChompBytes(treeData))
	require.Subset(t, files, []string{"VERSION", "README", "files", ".gitattributes"}, "ensure the files remain on their places")
}

func TestUserSquash_stableID(t *testing.T) {
	serverSocketPath, stop := runOperationServiceServer(t)
	defer stop()

	client, conn := newOperationClient(t, serverSocketPath)
	defer conn.Close()

	repoProto, _, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()
	repo := localrepo.New(git.NewExecCommandFactory(config.Config), repoProto, config.Config)

	ctx, cancel := testhelper.Context()
	defer cancel()

	response, err := client.UserSquash(ctx, &gitalypb.UserSquashRequest{
		Repository:    repoProto,
		User:          testhelper.TestUser,
		SquashId:      "1",
		Author:        author,
		CommitMessage: []byte("Squashed commit"),
		StartSha:      startSha,
		EndSha:        endSha,
		Timestamp:     &timestamp.Timestamp{Seconds: 1234512345},
	})
	require.NoError(t, err)
	require.Empty(t, response.GetGitError())

	commit, err := repo.ReadCommit(ctx, git.Revision(response.SquashSha))
	require.NoError(t, err)
	require.Equal(t, &gitalypb.GitCommit{
		Id:     "2773b7aee7d81ea96d2f48aa080cae08eaae26d5",
		TreeId: "324242f415a3cdbfc088103b496379fd91965854",
		ParentIds: []string{
			"b83d6e391c22777fca1ed3012fce84f633d7fed0",
		},
		Subject:  []byte("Squashed commit"),
		Body:     []byte("Squashed commit\n"),
		BodySize: 16,
		Author: &gitalypb.CommitAuthor{
			Name:     author.Name,
			Email:    author.Email,
			Date:     &timestamp.Timestamp{Seconds: 1234512345},
			Timezone: []byte("+0000"),
		},
		Committer: &gitalypb.CommitAuthor{
			Name:     testhelper.TestUser.Name,
			Email:    testhelper.TestUser.Email,
			Date:     &timestamp.Timestamp{Seconds: 1234512345},
			Timezone: []byte("+0000"),
		},
	}, commit)
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

	repoProto, repoPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()
	repo := localrepo.New(git.NewExecCommandFactory(config.Config), repoProto, config.Config)

	request := &gitalypb.UserSquashRequest{
		Repository:    repoProto,
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

	commit, err := repo.ReadCommit(ctx, git.Revision(response.SquashSha))
	require.NoError(t, err)
	require.Equal(t, []string{"6f6d7e7ed97bb5f0054f2b1df789b39ca89b6ff9"}, commit.ParentIds)
	require.Equal(t, author.Name, commit.Author.Name)
	require.Equal(t, author.Email, commit.Author.Email)
	require.Equal(t, testhelper.TestUser.Name, commit.Committer.Name)
	require.Equal(t, testhelper.TestUser.Email, commit.Committer.Email)
	require.Equal(t, commitMessage, commit.Subject)

	// Handle symlinks in macOS from /tmp -> /private/tmp
	repoPath, err = filepath.EvalSymlinks(repoPath)
	require.NoError(t, err)

	// Ensure Git metadata is cleaned up
	worktreeList := text.ChompBytes(testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "worktree", "list", "--porcelain"))
	expectedOut := fmt.Sprintf("worktree %s\nbare\n", repoPath)
	require.Equal(t, expectedOut, worktreeList)

	// Ensure actual worktree is removed
	files, err := ioutil.ReadDir(filepath.Join(repoPath, "gitlab-worktree"))
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

	repoProto, repoPath, cleanupFn := testhelper.NewTestRepoWithWorktree(t)
	defer cleanupFn()
	repo := localrepo.New(git.NewExecCommandFactory(config.Config), repoProto, config.Config)

	originalFilename := "original-file.txt"
	renamedFilename := "renamed-file.txt"

	testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "config", "testhelper.TestUser.name", string(author.Name))
	testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "config", "testhelper.TestUser.email", string(author.Email))
	testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "checkout", "-b", "squash-rename-test", "master")
	require.NoError(t, ioutil.WriteFile(filepath.Join(repoPath, originalFilename), []byte("This is a test"), 0644))
	testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "add", ".")
	testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "commit", "-m", "test file")

	startCommitID := text.ChompBytes(testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "rev-parse", "HEAD"))

	testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "mv", originalFilename, renamedFilename)
	testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "commit", "-a", "-m", "renamed test file")

	// Modify the original file in another branch
	testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "checkout", "-b", "squash-rename-branch", startCommitID)
	require.NoError(t, ioutil.WriteFile(filepath.Join(repoPath, originalFilename), []byte("This is a change"), 0644))
	testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "commit", "-a", "-m", "test")

	require.NoError(t, ioutil.WriteFile(filepath.Join(repoPath, originalFilename), []byte("This is another change"), 0644))
	testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "commit", "-a", "-m", "test")

	endCommitID := text.ChompBytes(testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "rev-parse", "HEAD"))

	request := &gitalypb.UserSquashRequest{
		Repository:    repoProto,
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

	commit, err := repo.ReadCommit(ctx, git.Revision(response.SquashSha))
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
				Author:        testhelper.TestUser,
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
				Author:        testhelper.TestUser,
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
				Author:        testhelper.TestUser,
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
				Author:        testhelper.TestUser,
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
				Author:        testhelper.TestUser,
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
				Author:        testhelper.TestUser,
				CommitMessage: nil,
				StartSha:      startSha,
				EndSha:        endSha,
			},
			code: codes.InvalidArgument,
		},
		{
			desc: "worktree id can't contain slashes",
			request: &gitalypb.UserSquashRequest{
				Repository:    testRepo,
				User:          testhelper.TestUser,
				SquashId:      "1/2",
				Author:        testhelper.TestUser,
				CommitMessage: commitMessage,
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

func TestUserSquashWithGitError(t *testing.T) {
	serverSocketPath, stop := runOperationServiceServer(t)
	defer stop()

	client, conn := newOperationClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()

	testCases := []struct {
		desc     string
		request  *gitalypb.UserSquashRequest
		gitError string
	}{
		{
			desc: "not existing start SHA",
			request: &gitalypb.UserSquashRequest{
				Repository:    testRepo,
				SquashId:      "1",
				User:          testhelper.TestUser,
				Author:        testhelper.TestUser,
				CommitMessage: commitMessage,
				StartSha:      "doesntexisting",
				EndSha:        endSha,
			},
			gitError: "fatal: ambiguous argument 'doesntexisting...54cec5282aa9f21856362fe321c800c236a61615'",
		},
		{
			desc: "not existing end SHA",
			request: &gitalypb.UserSquashRequest{
				Repository:    testRepo,
				SquashId:      "1",
				User:          testhelper.TestUser,
				Author:        testhelper.TestUser,
				CommitMessage: commitMessage,
				StartSha:      startSha,
				EndSha:        "doesntexisting",
			},
			gitError: "fatal: ambiguous argument 'b83d6e391c22777fca1ed3012fce84f633d7fed0...doesntexisting'",
		},
		{
			desc: "user has no name set",
			request: &gitalypb.UserSquashRequest{
				Repository:    testRepo,
				SquashId:      "1",
				User:          &gitalypb.User{Email: testhelper.TestUser.Email},
				Author:        testhelper.TestUser,
				CommitMessage: commitMessage,
				StartSha:      startSha,
				EndSha:        endSha,
			},
			gitError: "fatal: empty ident name (for <janedoe@gitlab.com>) not allowed",
		},
		{
			desc: "author has no name set",
			request: &gitalypb.UserSquashRequest{
				Repository:    testRepo,
				SquashId:      "1",
				User:          testhelper.TestUser,
				Author:        &gitalypb.User{Email: testhelper.TestUser.Email},
				CommitMessage: commitMessage,
				StartSha:      startSha,
				EndSha:        endSha,
			},
			gitError: "fatal: empty ident name (for <janedoe@gitlab.com>) not allowed",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			ctx, cancel := testhelper.Context()
			defer cancel()

			resp, err := client.UserSquash(ctx, testCase.request)
			s, ok := status.FromError(err)
			require.True(t, ok)
			assert.Equal(t, codes.OK, s.Code())
			assert.Empty(t, resp.SquashSha)
			assert.Contains(t, resp.GitError, testCase.gitError)
		})
	}
}
