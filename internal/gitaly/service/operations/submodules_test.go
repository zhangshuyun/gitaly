package operations

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/internal/git/lstree"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
)

func TestSuccessfulUserUpdateSubmoduleRequest(t *testing.T) {
	testhelper.NewFeatureSets(
		[]featureflag.FeatureFlag{featureflag.GoUserUpdateSubmodule},
	).Run(t, testSuccessfulUserUpdateSubmoduleRequest)
}

func testSuccessfulUserUpdateSubmoduleRequest(t *testing.T, ctx context.Context) {
	serverSocketPath, stop := runOperationServiceServer(t)
	defer stop()

	client, conn := newOperationClient(t, serverSocketPath)
	defer conn.Close()

	testRepoProto, testRepoPath, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()

	testRepo := localrepo.New(git.NewExecCommandFactory(config.Config), testRepoProto, config.Config)

	// This reference is created to check that we can correctly commit onto
	// a branch which has a name starting with "refs/heads/".
	currentOID, err := testRepo.ResolveRevision(ctx, "refs/heads/master")
	require.NoError(t, err)
	require.NoError(t, testRepo.UpdateRef(ctx, "refs/heads/refs/heads/master", currentOID, git.ZeroOID))

	// If something uses the branch name as an unqualified reference, then
	// git would return the tag instead of the branch. We thus create a tag
	// with a different OID than the current master branch.
	prevOID, err := testRepo.ResolveRevision(ctx, "refs/heads/master~")
	require.NoError(t, err)
	require.NoError(t, testRepo.UpdateRef(ctx, "refs/tags/master", prevOID, git.ZeroOID))

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
			desc:      "Update submodule on weird branch",
			submodule: "gitlab-grack",
			commitSha: "41fa1bc9e0f0630ced6a8a211d60c2af425ecc2d",
			branch:    "refs/heads/master",
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
			request := &gitalypb.UserUpdateSubmoduleRequest{
				Repository:    testRepoProto,
				User:          testhelper.TestUser,
				Submodule:     []byte(testCase.submodule),
				CommitSha:     testCase.commitSha,
				Branch:        []byte(testCase.branch),
				CommitMessage: commitMessage,
			}

			response, err := client.UserUpdateSubmodule(ctx, request)
			require.NoError(t, err)
			require.Empty(t, response.GetCommitError())
			require.Empty(t, response.GetPreReceiveError())

			commit, err := testRepo.ReadCommit(ctx, git.Revision(response.BranchUpdate.CommitId))
			require.NoError(t, err)
			require.Equal(t, commit.Author.Email, testhelper.TestUser.Email)
			require.Equal(t, commit.Committer.Email, testhelper.TestUser.Email)
			require.Equal(t, commit.Subject, commitMessage)

			entry := testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "ls-tree", "-z", fmt.Sprintf("%s^{tree}:", response.BranchUpdate.CommitId), testCase.submodule)
			parser := lstree.NewParser(bytes.NewReader(entry))
			parsedEntry, err := parser.NextEntry()
			require.NoError(t, err)
			require.Equal(t, testCase.submodule, parsedEntry.Path)
			require.Equal(t, testCase.commitSha, parsedEntry.Oid)
		})
	}
}

func TestUserUpdateSubmodule_stableID(t *testing.T) {
	testhelper.NewFeatureSets(
		[]featureflag.FeatureFlag{featureflag.GoUserUpdateSubmodule},
	).Run(t, testUserUpdateSubmoduleStableID)
}

func testUserUpdateSubmoduleStableID(t *testing.T, ctx context.Context) {
	serverSocketPath, stop := runOperationServiceServer(t)
	defer stop()

	client, conn := newOperationClient(t, serverSocketPath)
	defer conn.Close()

	repoProto, _, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()
	repo := localrepo.New(git.NewExecCommandFactory(config.Config), repoProto, config.Config)

	response, err := client.UserUpdateSubmodule(ctx, &gitalypb.UserUpdateSubmoduleRequest{
		Repository:    repoProto,
		User:          testhelper.TestUser,
		Submodule:     []byte("gitlab-grack"),
		CommitSha:     "41fa1bc9e0f0630ced6a8a211d60c2af425ecc2d",
		Branch:        []byte("master"),
		CommitMessage: []byte("Update Submodule message"),
		Timestamp:     &timestamp.Timestamp{Seconds: 12345},
	})
	require.NoError(t, err)
	require.Empty(t, response.GetCommitError())
	require.Empty(t, response.GetPreReceiveError())

	commit, err := repo.ReadCommit(ctx, git.Revision(response.BranchUpdate.CommitId))
	require.NoError(t, err)
	require.Equal(t, &gitalypb.GitCommit{
		Id: "e7752dfc2105bc830f8fa59b19dd4f3e49c8c44e",
		ParentIds: []string{
			"1e292f8fedd741b75372e19097c76d327140c312",
		},
		TreeId:   "569d23230fd644aaeb2fcb239c52ef1fcaa171c3",
		Subject:  []byte("Update Submodule message"),
		Body:     []byte("Update Submodule message"),
		BodySize: 24,
		Author: &gitalypb.CommitAuthor{
			Name:     testhelper.TestUser.Name,
			Email:    testhelper.TestUser.Email,
			Date:     &timestamp.Timestamp{Seconds: 12345},
			Timezone: []byte("+0000"),
		},
		Committer: &gitalypb.CommitAuthor{
			Name:     testhelper.TestUser.Name,
			Email:    testhelper.TestUser.Email,
			Date:     &timestamp.Timestamp{Seconds: 12345},
			Timezone: []byte("+0000"),
		},
	}, commit)
}

func TestFailedUserUpdateSubmoduleRequestDueToValidations(t *testing.T) {
	testhelper.NewFeatureSets(
		[]featureflag.FeatureFlag{featureflag.GoUserUpdateSubmodule},
	).Run(t, testFailedUserUpdateSubmoduleRequestDueToValidations)
}

func testFailedUserUpdateSubmoduleRequestDueToValidations(t *testing.T, ctx context.Context) {
	serverSocketPath, stop := runOperationServiceServer(t)
	defer stop()

	client, conn := newOperationClient(t, serverSocketPath)
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
				User:          testhelper.TestUser,
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
				User:          testhelper.TestUser,
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
				User:          testhelper.TestUser,
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
				User:          testhelper.TestUser,
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
				User:          testhelper.TestUser,
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
				User:          testhelper.TestUser,
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
				User:          testhelper.TestUser,
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
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()

			_, err := client.UserUpdateSubmodule(ctx, testCase.request)
			testhelper.RequireGrpcError(t, err, testCase.code)
			require.Contains(t, err.Error(), testCase.desc)
		})
	}
}

func TestFailedUserUpdateSubmoduleRequestDueToInvalidBranch(t *testing.T) {
	testhelper.NewFeatureSets(
		[]featureflag.FeatureFlag{featureflag.GoUserUpdateSubmodule},
	).Run(t, testFailedUserUpdateSubmoduleRequestDueToInvalidBranch)
}

func testFailedUserUpdateSubmoduleRequestDueToInvalidBranch(t *testing.T, ctx context.Context) {
	serverSocketPath, stop := runOperationServiceServer(t)
	defer stop()

	client, conn := newOperationClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()

	request := &gitalypb.UserUpdateSubmoduleRequest{
		Repository:    testRepo,
		User:          testhelper.TestUser,
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
	testhelper.NewFeatureSets(
		[]featureflag.FeatureFlag{featureflag.GoUserUpdateSubmodule},
	).Run(t, testFailedUserUpdateSubmoduleRequestDueToInvalidSubmodule)
}

func testFailedUserUpdateSubmoduleRequestDueToInvalidSubmodule(t *testing.T, ctx context.Context) {
	serverSocketPath, stop := runOperationServiceServer(t)
	defer stop()

	client, conn := newOperationClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()

	request := &gitalypb.UserUpdateSubmoduleRequest{
		Repository:    testRepo,
		User:          testhelper.TestUser,
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
	testhelper.NewFeatureSets(
		[]featureflag.FeatureFlag{featureflag.GoUserUpdateSubmodule},
	).Run(t, testFailedUserUpdateSubmoduleRequestDueToSameReference)
}

func testFailedUserUpdateSubmoduleRequestDueToSameReference(t *testing.T, ctx context.Context) {
	serverSocketPath, stop := runOperationServiceServer(t)
	defer stop()

	client, conn := newOperationClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()

	request := &gitalypb.UserUpdateSubmoduleRequest{
		Repository:    testRepo,
		User:          testhelper.TestUser,
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
	testhelper.NewFeatureSets(
		[]featureflag.FeatureFlag{featureflag.GoUserUpdateSubmodule},
	).Run(t, testFailedUserUpdateSubmoduleRequestDueToRepositoryEmpty)
}

func testFailedUserUpdateSubmoduleRequestDueToRepositoryEmpty(t *testing.T, ctx context.Context) {
	serverSocketPath, stop := runOperationServiceServer(t)
	defer stop()

	client, conn := newOperationClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanup := testhelper.InitRepoWithWorktree(t)
	defer cleanup()

	request := &gitalypb.UserUpdateSubmoduleRequest{
		Repository:    testRepo,
		User:          testhelper.TestUser,
		Submodule:     []byte("six"),
		CommitSha:     "41fa1bc9e0f0630ced6a8a211d60c2af425ecc2d",
		Branch:        []byte("master"),
		CommitMessage: []byte("Update Submodule message"),
	}

	response, err := client.UserUpdateSubmodule(ctx, request)
	require.NoError(t, err)
	require.Equal(t, response.CommitError, "Repository is empty")
}
