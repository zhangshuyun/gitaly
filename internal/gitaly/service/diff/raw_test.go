package diff

import (
	"fmt"
	"io/ioutil"
	"regexp"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/streamio"
	"google.golang.org/grpc/codes"
)

func TestSuccessfulRawDiffRequest(t *testing.T) {
	cfg, repo, repoPath, client := setupDiffService(t)

	ctx, cancel := testhelper.Context()
	defer cancel()

	rightCommit := "e395f646b1499e8e0279445fc99a0596a65fab7e"
	leftCommit := "8a0f2ee90d940bfb0ba1e14e8214b0649056e4ab"
	rpcRequest := &gitalypb.RawDiffRequest{Repository: repo, RightCommitId: rightCommit, LeftCommitId: leftCommit}

	c, err := client.RawDiff(ctx, rpcRequest)
	require.NoError(t, err)

	_, sandboxRepoPath, cleanupFn := gittest.CloneRepoWithWorktree(t)
	defer cleanupFn()

	reader := streamio.NewReader(func() ([]byte, error) {
		response, err := c.Recv()
		return response.GetData(), err
	})

	committerName := "Scrooge McDuck"
	committerEmail := "scrooge@mcduck.com"
	gittest.Exec(t, cfg, "-C", sandboxRepoPath, "reset", "--hard", leftCommit)

	gittest.ExecStream(t, cfg, reader, "-C", sandboxRepoPath, "apply")
	gittest.ExecStream(t, cfg, reader, "-C", sandboxRepoPath, "add", ".")
	gittest.Exec(t, cfg, "-C", sandboxRepoPath,
		"-c", fmt.Sprintf("user.name=%s", committerName),
		"-c", fmt.Sprintf("user.email=%s", committerEmail),
		"commit", "-m", "Applying received raw diff")

	expectedTreeStructure := gittest.Exec(t, cfg, "-C", repoPath, "ls-tree", "-r", rightCommit)
	actualTreeStructure := gittest.Exec(t, cfg, "-C", sandboxRepoPath, "ls-tree", "-r", "HEAD")
	require.Equal(t, expectedTreeStructure, actualTreeStructure)
}

func TestFailedRawDiffRequestDueToValidations(t *testing.T) {
	_, repo, _, client := setupDiffService(t)

	testCases := []struct {
		desc    string
		request *gitalypb.RawDiffRequest
		code    codes.Code
	}{
		{
			desc: "empty left commit",
			request: &gitalypb.RawDiffRequest{
				Repository:    repo,
				LeftCommitId:  "",
				RightCommitId: "e395f646b1499e8e0279445fc99a0596a65fab7e",
			},
			code: codes.InvalidArgument,
		},
		{
			desc: "empty right commit",
			request: &gitalypb.RawDiffRequest{
				Repository:    repo,
				RightCommitId: "",
				LeftCommitId:  "e395f646b1499e8e0279445fc99a0596a65fab7e",
			},
			code: codes.InvalidArgument,
		},
		{
			desc: "empty repo",
			request: &gitalypb.RawDiffRequest{
				Repository:    nil,
				RightCommitId: "8a0f2ee90d940bfb0ba1e14e8214b0649056e4ab",
				LeftCommitId:  "e395f646b1499e8e0279445fc99a0596a65fab7e",
			},
			code: codes.InvalidArgument,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			ctx, cancel := testhelper.Context()
			defer cancel()

			c, _ := client.RawDiff(ctx, testCase.request)
			testhelper.RequireGrpcError(t, drainRawDiffResponse(c), testCase.code)
		})
	}
}

func TestSuccessfulRawPatchRequest(t *testing.T) {
	cfg, repo, repoPath, client := setupDiffService(t)

	ctx, cancel := testhelper.Context()
	defer cancel()

	rightCommit := "e395f646b1499e8e0279445fc99a0596a65fab7e"
	leftCommit := "8a0f2ee90d940bfb0ba1e14e8214b0649056e4ab"
	rpcRequest := &gitalypb.RawPatchRequest{Repository: repo, RightCommitId: rightCommit, LeftCommitId: leftCommit}

	c, err := client.RawPatch(ctx, rpcRequest)
	require.NoError(t, err)

	reader := streamio.NewReader(func() ([]byte, error) {
		response, err := c.Recv()
		return response.GetData(), err
	})

	_, sandboxRepoPath, cleanupFn := gittest.CloneRepoWithWorktree(t)
	defer cleanupFn()

	gittest.Exec(t, cfg, "-C", sandboxRepoPath, "reset", "--hard", leftCommit)

	gittest.ExecStream(t, cfg, reader, "-C", sandboxRepoPath, "am")

	expectedTreeStructure := gittest.Exec(t, cfg, "-C", repoPath, "ls-tree", "-r", rightCommit)
	actualTreeStructure := gittest.Exec(t, cfg, "-C", sandboxRepoPath, "ls-tree", "-r", "HEAD")
	require.Equal(t, expectedTreeStructure, actualTreeStructure)
}

func TestFailedRawPatchRequestDueToValidations(t *testing.T) {
	_, repo, _, client := setupDiffService(t)

	testCases := []struct {
		desc    string
		request *gitalypb.RawPatchRequest
		code    codes.Code
	}{
		{
			desc: "empty left commit",
			request: &gitalypb.RawPatchRequest{
				Repository:    repo,
				LeftCommitId:  "",
				RightCommitId: "e395f646b1499e8e0279445fc99a0596a65fab7e",
			},
			code: codes.InvalidArgument,
		},
		{
			desc: "empty right commit",
			request: &gitalypb.RawPatchRequest{
				Repository:    repo,
				RightCommitId: "",
				LeftCommitId:  "e395f646b1499e8e0279445fc99a0596a65fab7e",
			},
			code: codes.InvalidArgument,
		},
		{
			desc: "empty repo",
			request: &gitalypb.RawPatchRequest{
				Repository:    nil,
				RightCommitId: "8a0f2ee90d940bfb0ba1e14e8214b0649056e4ab",
				LeftCommitId:  "e395f646b1499e8e0279445fc99a0596a65fab7e",
			},
			code: codes.InvalidArgument,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			ctx, cancel := testhelper.Context()
			defer cancel()

			c, _ := client.RawPatch(ctx, testCase.request)
			testhelper.RequireGrpcError(t, drainRawPatchResponse(c), testCase.code)
		})
	}
}

func TestRawPatchContainsGitLabSignature(t *testing.T) {
	_, repo, _, client := setupDiffService(t)

	ctx, cancel := testhelper.Context()
	defer cancel()

	rightCommit := "e395f646b1499e8e0279445fc99a0596a65fab7e"
	leftCommit := "8a0f2ee90d940bfb0ba1e14e8214b0649056e4ab"
	rpcRequest := &gitalypb.RawPatchRequest{Repository: repo, RightCommitId: rightCommit, LeftCommitId: leftCommit}

	c, err := client.RawPatch(ctx, rpcRequest)
	require.NoError(t, err)

	reader := streamio.NewReader(func() ([]byte, error) {
		response, err := c.Recv()
		return response.GetData(), err
	})

	patch, err := ioutil.ReadAll(reader)
	require.NoError(t, err)

	require.Regexp(t, regexp.MustCompile(`\n-- \nGitLab\s+$`), string(patch))
}

func drainRawDiffResponse(c gitalypb.DiffService_RawDiffClient) error {
	var err error
	for err == nil {
		_, err = c.Recv()
	}
	return err
}

func drainRawPatchResponse(c gitalypb.DiffService_RawPatchClient) error {
	var err error
	for err == nil {
		_, err = c.Recv()
	}
	return err
}
