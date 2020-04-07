package operations

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/git/log"
	"gitlab.com/gitlab-org/gitaly/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
)

func TestSuccessfulUserDeleteTagRequest(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	serverSocketPath, stop := runOperationServiceServer(t)
	defer stop()

	client, conn := newOperationClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	tagNameInput := "to-be-deleted-soon-tag"

	defer exec.Command("git", "-C", testRepoPath, "tag", "-d", tagNameInput).Run()

	user := &gitalypb.User{
		Name:  []byte("Ahmad Sherif"),
		Email: []byte("ahmad@gitlab.com"),
		GlId:  "user-123",
	}

	testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "tag", tagNameInput)

	cleanupSrv := SetupAndStartGitlabServer(t, user.GlId, testRepo.GlRepository)
	defer cleanupSrv()

	request := &gitalypb.UserDeleteTagRequest{
		Repository: testRepo,
		TagName:    []byte(tagNameInput),
		User:       user,
	}

	_, err := client.UserDeleteTag(ctx, request)
	require.NoError(t, err)

	tags := testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "tag")
	require.NotContains(t, string(tags), tagNameInput, "tag name still exists in tags list")
}

func TestSuccessfulGitHooksForUserDeleteTagRequest(t *testing.T) {
	serverSocketPath, stop := runOperationServiceServer(t)
	defer stop()

	client, conn := newOperationClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	cleanupSrv := SetupAndStartGitlabServer(t, user.GlId, testRepo.GlRepository)
	defer cleanupSrv()

	tagNameInput := "to-be-déleted-soon-tag"
	defer exec.Command("git", "-C", testRepoPath, "tag", "-d", tagNameInput).Run()

	user := &gitalypb.User{
		Name:       []byte("Ahmad Sherif"),
		Email:      []byte("ahmad@gitlab.com"),
		GlId:       "user-123",
		GlUsername: "johndoe",
	}

	request := &gitalypb.UserDeleteTagRequest{
		Repository: testRepo,
		TagName:    []byte(tagNameInput),
		User:       user,
	}

	for _, hookName := range GitlabHooks {
		t.Run(hookName, func(t *testing.T) {
			testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "tag", tagNameInput)

			hookOutputTempPath, cleanup := testhelper.WriteEnvToCustomHook(t, testRepoPath, hookName)
			defer cleanup()

			ctx, cancel := testhelper.Context()
			defer cancel()

			_, err := client.UserDeleteTag(ctx, request)
			require.NoError(t, err)

			output := testhelper.MustReadFile(t, hookOutputTempPath)
			require.Contains(t, string(output), "GL_USERNAME=johndoe")
		})
	}
}

func TestSuccessfulUserCreateTagRequest(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	serverSocketPath, stop := runOperationServiceServer(t)
	defer stop()

	client, conn := newOperationClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	targetRevision := "c7fbe50c7c7419d9701eebe64b1fdacc3df5b9dd"
	targetRevisionCommit, err := log.GetCommit(ctx, testRepo, targetRevision)
	require.NoError(t, err)

	user := &gitalypb.User{
		Name:  []byte("Ahmad Sherif"),
		Email: []byte("ahmad@gitlab.com"),
		GlId:  "user-123",
	}
	inputTagName := "to-be-créated-soon"

	cwd, err := os.Getwd()
	require.NoError(t, err)
	preReceiveHook := filepath.Join(cwd, "testdata/pre-receive-expect-object-type")
	updateHook := filepath.Join(cwd, "testdata/update-expect-object-type")

	testCases := []struct {
		desc               string
		tagName            string
		message            string
		targetRevision     string
		expectedTag        *gitalypb.Tag
		expectedObjectType string
	}{
		{
			desc:           "lightweight tag",
			tagName:        inputTagName,
			targetRevision: targetRevision,
			expectedTag: &gitalypb.Tag{
				Name:         []byte(inputTagName),
				TargetCommit: targetRevisionCommit,
			},
			expectedObjectType: "commit",
		},
		{
			desc:           "annotated tag",
			tagName:        inputTagName,
			targetRevision: targetRevision,
			message:        "This is an annotated tag",
			expectedTag: &gitalypb.Tag{
				Name:         []byte(inputTagName),
				TargetCommit: targetRevisionCommit,
				Message:      []byte("This is an annotated tag"),
				MessageSize:  24,
			},
			expectedObjectType: "tag",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			for hook, content := range map[string]string{
				"pre-receive": fmt.Sprintf("#!/bin/sh\n%s %s \"$@\"", preReceiveHook, testCase.expectedObjectType),
				"update":      fmt.Sprintf("#!/bin/sh\n%s %s \"$@\"", updateHook, testCase.expectedObjectType),
			} {
				hookCleanup, err := testhelper.WriteCustomHook(testRepoPath, hook, []byte(content))
				require.NoError(t, err)
				defer hookCleanup()
			}

			request := &gitalypb.UserCreateTagRequest{
				Repository:     testRepo,
				TagName:        []byte(testCase.tagName),
				TargetRevision: []byte(testCase.targetRevision),
				User:           user,
				Message:        []byte(testCase.message),
			}

			cleanupSrv := SetupAndStartGitlabServer(t, user.GlId, testRepo.GlRepository)
			defer cleanupSrv()

			ctx, cancel := testhelper.Context()
			defer cancel()

			response, err := client.UserCreateTag(ctx, request)
			require.NoError(t, err, "error from calling RPC")
			require.Empty(t, response.PreReceiveError, "PreReceiveError must be empty, signalling the push was accepted")

			defer exec.Command("git", "-C", testRepoPath, "tag", "-d", inputTagName).Run()

			id := testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "rev-parse", inputTagName)
			testCase.expectedTag.Id = text.ChompBytes(id)

			require.Equal(t, testCase.expectedTag, response.Tag)

			tag := testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "tag")
			require.Contains(t, string(tag), inputTagName)
		})
	}
}

func TestSuccessfulGitHooksForUserCreateTagRequest(t *testing.T) {
	testSuccessfulGitHooksForUserCreateTagRequest(t, false)
}

func TestSuccessfulGitHooksForUserCreateTagRequestWithHookRPCs(t *testing.T) {
	testSuccessfulGitHooksForUserCreateTagRequest(t, true)
}

func testSuccessfulGitHooksForUserCreateTagRequest(t *testing.T, callHookRPC bool) {
	serverSocketPath, stop := runOperationServiceServer(t)
	defer stop()

	client, conn := newOperationClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	tagName := "new-tag"
	user := &gitalypb.User{
		Name:       []byte("Ahmad Sherif"),
		Email:      []byte("ahmad@gitlab.com"),
		GlId:       "user-123",
		GlUsername: "johndoe",
	}

	cleanupSrv := SetupAndStartGitlabServer(t, user.GlId, testRepo.GlRepository)
	defer cleanupSrv()

	request := &gitalypb.UserCreateTagRequest{
		Repository:     testRepo,
		TagName:        []byte(tagName),
		TargetRevision: []byte("c7fbe50c7c7419d9701eebe64b1fdacc3df5b9dd"),
		User:           user,
	}

	ctx, cancel := testhelper.Context()
	defer cancel()

	if callHookRPC {
		ctx = outgoingCtxWithRubyFeatureFlag(ctx, "call-hook-rpc")
	}

	for _, hookName := range GitlabHooks {
		t.Run(hookName, func(t *testing.T) {
			defer exec.Command("git", "-C", testRepoPath, "tag", "-d", tagName).Run()

			hookOutputTempPath, cleanup := testhelper.WriteEnvToCustomHook(t, testRepoPath, hookName)
			defer cleanup()

			response, err := client.UserCreateTag(ctx, request)
			require.NoError(t, err)
			require.Empty(t, response.PreReceiveError)

			output := string(testhelper.MustReadFile(t, hookOutputTempPath))
			require.Contains(t, output, "GL_USERNAME="+user.GlUsername)
		})
	}
}

func TestFailedUserDeleteTagRequestDueToValidation(t *testing.T) {
	serverSocketPath, stop := runOperationServiceServer(t)
	defer stop()

	client, conn := newOperationClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	user := &gitalypb.User{
		Name:  []byte("Ahmad Sherif"),
		Email: []byte("ahmad@gitlab.com"),
		GlId:  "user-123",
	}

	testCases := []struct {
		desc    string
		request *gitalypb.UserDeleteTagRequest
		code    codes.Code
	}{
		{
			desc: "empty user",
			request: &gitalypb.UserDeleteTagRequest{
				Repository: testRepo,
				TagName:    []byte("does-matter-the-name-if-user-is-empty"),
			},
			code: codes.InvalidArgument,
		},
		{
			desc: "empty tag name",
			request: &gitalypb.UserDeleteTagRequest{
				Repository: testRepo,
				User:       user,
			},
			code: codes.InvalidArgument,
		},
		{
			desc: "non-existent tag name",
			request: &gitalypb.UserDeleteTagRequest{
				Repository: testRepo,
				User:       user,
				TagName:    []byte("i-do-not-exist"),
			},
			code: codes.FailedPrecondition,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			ctx, cancel := testhelper.Context()
			defer cancel()

			_, err := client.UserDeleteTag(ctx, testCase.request)
			testhelper.RequireGrpcError(t, err, testCase.code)
		})
	}
}

func TestFailedUserDeleteTagDueToHooks(t *testing.T) {
	serverSocketPath, stop := runOperationServiceServer(t)
	defer stop()

	client, conn := newOperationClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	tagNameInput := "to-be-deleted-soon-tag"
	testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "tag", tagNameInput)
	defer exec.Command("git", "-C", testRepoPath, "tag", "-d", tagNameInput).Run()

	user := &gitalypb.User{
		Name:  []byte("Ahmad Sherif"),
		Email: []byte("ahmad@gitlab.com"),
		GlId:  "user-123",
	}

	cleanupSrv := SetupAndStartGitlabServer(t, user.GlId, testRepo.GlRepository)
	defer cleanupSrv()

	request := &gitalypb.UserDeleteTagRequest{
		Repository: testRepo,
		TagName:    []byte(tagNameInput),
		User:       user,
	}

	hookContent := []byte("#!/bin/sh\necho GL_ID=$GL_ID\nexit 1")

	for _, hookName := range gitlabPreHooks {
		t.Run(hookName, func(t *testing.T) {
			remove, err := testhelper.WriteCustomHook(testRepoPath, hookName, hookContent)
			require.NoError(t, err)
			defer remove()

			ctx, cancel := testhelper.Context()
			defer cancel()

			response, err := client.UserDeleteTag(ctx, request)
			require.Nil(t, err)
			require.Contains(t, response.PreReceiveError, "GL_ID="+user.GlId)

			tags := testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "tag")
			require.Contains(t, string(tags), tagNameInput, "tag name does not exist in tags list")
		})
	}
}

func TestFailedUserCreateTagDueToHooks(t *testing.T) {
	serverSocketPath, stop := runOperationServiceServer(t)
	defer stop()

	client, conn := newOperationClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	user := &gitalypb.User{
		Name:       []byte("Ahmad Sherif"),
		Email:      []byte("ahmad@gitlab.com"),
		GlId:       "user-123",
		GlUsername: "johndoe",
	}

	cleanupSrv := SetupAndStartGitlabServer(t, user.GlId, testRepo.GlRepository)
	defer cleanupSrv()

	request := &gitalypb.UserCreateTagRequest{
		Repository:     testRepo,
		TagName:        []byte("new-tag"),
		TargetRevision: []byte("c7fbe50c7c7419d9701eebe64b1fdacc3df5b9dd"),
		User:           user,
	}

	hookContent := []byte("#!/bin/sh\necho GL_ID=$GL_ID\nexit 1")

	for _, hookName := range gitlabPreHooks {
		remove, err := testhelper.WriteCustomHook(testRepoPath, hookName, hookContent)
		require.NoError(t, err)
		defer remove()

		ctx, cancel := testhelper.Context()
		defer cancel()

		response, err := client.UserCreateTag(ctx, request)
		require.Nil(t, err)
		require.Contains(t, response.PreReceiveError, "GL_ID="+user.GlId)
	}
}

func TestFailedUserCreateTagRequestDueToTagExistence(t *testing.T) {
	serverSocketPath, stop := runOperationServiceServer(t)
	defer stop()

	client, conn := newOperationClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	user := &gitalypb.User{
		Name:  []byte("Ahmad Sherif"),
		Email: []byte("ahmad@gitlab.com"),
		GlId:  "user-123",
	}

	cleanupSrv := SetupAndStartGitlabServer(t, user.GlId, testRepo.GlRepository)
	defer cleanupSrv()

	testCase := struct {
		tagName        string
		targetRevision string
		user           *gitalypb.User
	}{
		tagName:        "v1.1.0",
		targetRevision: "master",
		user:           user,
	}

	request := &gitalypb.UserCreateTagRequest{
		Repository:     testRepo,
		TagName:        []byte(testCase.tagName),
		TargetRevision: []byte(testCase.targetRevision),
		User:           testCase.user,
	}

	ctx, cancel := testhelper.Context()
	defer cancel()

	response, err := client.UserCreateTag(ctx, request)
	require.NoError(t, err)
	require.Equal(t, response.Exists, true)
}

func TestFailedUserCreateTagRequestDueToValidation(t *testing.T) {
	serverSocketPath, stop := runOperationServiceServer(t)
	defer stop()

	client, conn := newOperationClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	user := &gitalypb.User{
		Name:  []byte("Ahmad Sherif"),
		Email: []byte("ahmad@gitlab.com"),
		GlId:  "user-123",
	}

	testCases := []struct {
		desc           string
		tagName        string
		targetRevision string
		user           *gitalypb.User
		code           codes.Code
	}{
		{
			desc:           "empty target revision",
			tagName:        "shiny-new-tag",
			targetRevision: "",
			user:           user,
			code:           codes.InvalidArgument,
		},
		{
			desc:           "empty user",
			tagName:        "shiny-new-tag",
			targetRevision: "master",
			user:           nil,
			code:           codes.InvalidArgument,
		},
		{
			desc:           "non-existing starting point",
			tagName:        "new-tag",
			targetRevision: "i-dont-exist",
			user:           user,
			code:           codes.FailedPrecondition,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			request := &gitalypb.UserCreateTagRequest{
				Repository:     testRepo,
				TagName:        []byte(testCase.tagName),
				TargetRevision: []byte(testCase.targetRevision),
				User:           testCase.user,
			}

			ctx, cancel := testhelper.Context()
			defer cancel()

			_, err := client.UserCreateTag(ctx, request)
			testhelper.RequireGrpcError(t, err, testCase.code)
		})
	}
}
