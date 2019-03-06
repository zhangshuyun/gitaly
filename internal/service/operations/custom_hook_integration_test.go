package operations

import (
	"context"
	"os/exec"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly-proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
)

var testCases = []struct {
	scriptContent       string
	responseContains    []string // strings we expect to be contained
	responseNotContains []string // strings we should not see in PreReceiveError
}{
	{
		"echo \"msg to STDOUT\";",
		[]string{},
		[]string{"msg to STDOUT"},
	},
	{
		"echo \"GitLab: msg to STDOUT\";",
		[]string{"msg to STDOUT"},
		[]string{},
	},
	{
		"echo \"gitlab: msg to STDOUT\";", // case-insensitive match on prefix
		[]string{"msg to STDOUT"},
		[]string{},
	},
	{
		"echo \"GitLab: msg to STDOUT\na second line\";",
		[]string{"msg to STDOUT"},
		[]string{"a second line"},
	},
	{
		"echo \"GitLab:msg to STDOUT\";", // no whitespace separation from prefix
		[]string{"msg to STDOUT"},
		[]string{},
	},
	{
		"echo \"msg to STDOUT, not prefixed by GitLab:, but containing it\";",
		[]string{},
		[]string{"msg to STDOUT, not prefixed by GitLab:, but containing it"},
	},
	{
		"1>&2 echo \"GitLab: msg to STDERR\";",
		[]string{"msg to STDERR"},
		[]string{},
	},
	{
		"1>&2 echo \"msg to STDERR, not prefixed by GitLab, but containing it\";",
		[]string{},
		[]string{"msg to STDERR, not prefixed by GitLab:, but containing it"},
	},
}

func TestCustomHookResponseWhenHooksFail(t *testing.T) {
	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	server, serverSocketPath := runOperationServiceServer(t)
	defer server.Stop()

	client, conn := newOperationClient(t, serverSocketPath)
	defer conn.Close()

	request := &gitalypb.UserCreateBranchRequest{
		Repository: testRepo,
		BranchName: []byte("new-branch"),
		StartPoint: []byte("c7fbe50c7c7419d9701eebe64b1fdacc3df5b9dd"),
		User:       user,
	}

	for _, testCase := range testCases {
		script := testhelper.FailingHookScript(testCase.scriptContent)

		for _, hookName := range gitlabPreHooks {
			remove, err := OverrideHooks(hookName, []byte(script))

			require.NoError(t, err)
			defer remove()

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			response, err := client.UserCreateBranch(ctx, request)
			require.NoError(t, err)

			for _, msg := range testCase.responseContains {
				require.Contains(t, response.PreReceiveError, msg)
			}

			for _, msg := range testCase.responseNotContains {
				require.NotContains(t, response.PreReceiveError, msg)
			}
		}
	}
}

func TestCustomHookResponseWhenHooksSucceed(t *testing.T) {
	testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	server, serverSocketPath := runOperationServiceServer(t)
	defer server.Stop()

	client, conn := newOperationClient(t, serverSocketPath)
	defer conn.Close()

	branchName := "new-branch"

	request := &gitalypb.UserCreateBranchRequest{
		Repository: testRepo,
		BranchName: []byte(branchName),
		StartPoint: []byte("c7fbe50c7c7419d9701eebe64b1fdacc3df5b9dd"),
		User:       user,
	}

	for _, testCase := range testCases {
		script := testhelper.SuccessfulHookScript(testCase.scriptContent)

		for _, hookName := range gitlabPreHooks {
			t.Run(hookName, func(t *testing.T) {
				defer exec.Command("git", "-C", testRepoPath, "branch", "-D", branchName).Run()

				remove, err := OverrideHooks(hookName, []byte(script))
				require.NoError(t, err)
				defer remove()

				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()

				response, err := client.UserCreateBranch(ctx, request)
				require.NoError(t, err)

				// response.PreReceiveError is always empty when script exits with success
				require.Empty(t, response.PreReceiveError)
			})
		}
	}
}
