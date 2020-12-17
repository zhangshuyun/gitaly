package operations

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strings"
	"testing"
	"testing/iotest"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/git/log"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/streamio"
	"google.golang.org/grpc/codes"
)

func TestSuccessfulUserApplyPatch(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	testSuccessfulUserApplyPatch(t, ctx)
}

func testSuccessfulUserApplyPatch(t *testing.T, ctx context.Context) {
	serverSocketPath, stop := runOperationServiceServer(t)
	defer stop()

	client, conn := newOperationClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	testPatchReadme := "testdata/0001-A-commit-from-a-patch.patch"
	testPatchFeature := "testdata/0001-This-does-not-apply-to-the-feature-branch.patch"

	testCases := []struct {
		desc           string
		branchName     string
		branchCreated  bool
		patches        []string
		commitMessages []string
	}{
		{
			desc:           "a new branch",
			branchName:     "patched-branch",
			branchCreated:  true,
			patches:        []string{testPatchReadme},
			commitMessages: []string{"A commit from a patch"},
		},
		{
			desc:           "an existing branch",
			branchName:     "feature",
			branchCreated:  false,
			patches:        []string{testPatchReadme},
			commitMessages: []string{"A commit from a patch"},
		},
		{
			desc:           "multiple patches",
			branchName:     "branch-with-multiple-patches",
			branchCreated:  true,
			patches:        []string{testPatchReadme, testPatchFeature},
			commitMessages: []string{"A commit from a patch", "This does not apply to the `feature` branch"},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			stream, err := client.UserApplyPatch(ctx)
			require.NoError(t, err)

			headerRequest := applyPatchHeaderRequest(testRepo, testhelper.TestUser, testCase.branchName)
			require.NoError(t, stream.Send(headerRequest))

			writer := streamio.NewWriter(func(p []byte) error {
				patchRequest := applyPatchPatchesRequest(p)

				return stream.Send(patchRequest)
			})

			for _, patchFileName := range testCase.patches {
				func() {
					file, err := os.Open(patchFileName)
					require.NoError(t, err)
					defer file.Close()

					byteReader := iotest.OneByteReader(file)
					_, err = io.Copy(writer, byteReader)
					require.NoError(t, err)
				}()
			}

			response, err := stream.CloseAndRecv()
			require.NoError(t, err)

			response.GetBranchUpdate()
			require.Equal(t, testCase.branchCreated, response.GetBranchUpdate().GetBranchCreated())

			branches := testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "branch")
			require.Contains(t, string(branches), testCase.branchName)

			maxCount := fmt.Sprintf("--max-count=%d", len(testCase.commitMessages))

			gitArgs := []string{
				"-C",
				testRepoPath,
				"log",
				testCase.branchName,
				"--format=%H",
				maxCount,
				"--reverse",
			}

			output := testhelper.MustRunCommand(t, nil, "git", gitArgs...)
			shas := strings.Split(string(output), "\n")
			// Throw away the last element, as that's going to be
			// an empty string.
			if len(shas) > 0 {
				shas = shas[:len(shas)-1]
			}

			for index, sha := range shas {
				commit, err := log.GetCommit(ctx, testRepo, sha)
				require.NoError(t, err)

				require.NotNil(t, commit)
				require.Equal(t, string(commit.Subject), testCase.commitMessages[index])
				require.Equal(t, string(commit.Author.Email), "patchuser@gitlab.org")
				require.Equal(t, string(commit.Committer.Email), string(testhelper.TestUser.Email))
			}
		})
	}
}

func TestFailedPatchApplyPatch(t *testing.T) {
	serverSocketPath, stop := runOperationServiceServer(t)
	defer stop()

	client, conn := newOperationClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	ctx, cancel := testhelper.Context()
	defer cancel()

	testPatch, err := ioutil.ReadFile("testdata/0001-This-does-not-apply-to-the-feature-branch.patch")
	require.NoError(t, err)

	stream, err := client.UserApplyPatch(ctx)
	require.NoError(t, err)

	headerRequest := applyPatchHeaderRequest(testRepo, testhelper.TestUser, "feature")
	require.NoError(t, stream.Send(headerRequest))

	patchRequest := applyPatchPatchesRequest(testPatch)
	require.NoError(t, stream.Send(patchRequest))

	_, err = stream.CloseAndRecv()
	testhelper.RequireGrpcError(t, err, codes.FailedPrecondition)
}

func TestFailedValidationUserApplyPatch(t *testing.T) {
	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	testCases := []struct {
		desc         string
		errorMessage string
		repo         *gitalypb.Repository
		user         *gitalypb.User
		branchName   string
	}{
		{
			desc:         "missing Repository",
			errorMessage: "missing Repository",
			branchName:   "new-branch",
			user:         testhelper.TestUser,
		},

		{
			desc:         "missing Branch",
			errorMessage: "missing Branch",
			repo:         testRepo,
			user:         testhelper.TestUser,
		},
		{
			desc:         "empty BranchName",
			errorMessage: "missing Branch",
			repo:         testRepo,
			user:         testhelper.TestUser,
			branchName:   "",
		},
		{
			desc:         "missing User",
			errorMessage: "missing User",
			branchName:   "new-branch",
			repo:         testRepo,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			request := applyPatchHeaderRequest(testCase.repo, testCase.user, testCase.branchName)
			err := validateUserApplyPatchHeader(request.GetHeader())

			require.Contains(t, err.Error(), testCase.errorMessage)
		})
	}
}

func applyPatchHeaderRequest(repo *gitalypb.Repository, user *gitalypb.User, branch string) *gitalypb.UserApplyPatchRequest {
	header := &gitalypb.UserApplyPatchRequest_Header_{
		Header: &gitalypb.UserApplyPatchRequest_Header{
			Repository:   repo,
			User:         user,
			TargetBranch: []byte(branch),
		},
	}
	return &gitalypb.UserApplyPatchRequest{
		UserApplyPatchRequestPayload: header,
	}
}

func applyPatchPatchesRequest(patches []byte) *gitalypb.UserApplyPatchRequest {
	requestPatches := &gitalypb.UserApplyPatchRequest_Patches{
		Patches: patches,
	}

	return &gitalypb.UserApplyPatchRequest{
		UserApplyPatchRequestPayload: requestPatches,
	}
}
