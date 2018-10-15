package operations

import (
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly-proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/internal/git/log"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"google.golang.org/grpc/codes"
)

func TestSuccessfulUserDeleteTagRequest(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	server, serverSocketPath := runOperationServiceServer(t)
	defer server.Stop()

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
	server, serverSocketPath := runOperationServiceServer(t)
	defer server.Stop()

	client, conn := newOperationClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	tagNameInput := "to-be-deleted-soon-tag"
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

			hookPath, hookOutputTempPath := WriteEnvToHook(t, testRepoPath, hookName)
			defer os.Remove(hookPath)
			defer os.Remove(hookOutputTempPath)

			ctx, cancel := testhelper.Context()
			defer cancel()

			_, err := client.UserDeleteTag(ctx, request)
			require.NoError(t, err)

			output := testhelper.MustReadFile(t, hookOutputTempPath)
			require.Contains(t, string(output), "GL_ID=user-123")
			require.Contains(t, string(output), "GL_USERNAME=johndoe")
		})
	}
}

func TestSuccessfulUserCreateTagRequest(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	server, serverSocketPath := runOperationServiceServer(t)
	defer server.Stop()

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
	inputTagName := "to-be-created-soon"

	testCases := []struct {
		desc           string
		tagName        string
		message        string
		targetRevision string
		expectedTag    *gitalypb.Tag
	}{
		{
			desc:           "lightweight tag",
			tagName:        inputTagName,
			targetRevision: targetRevision,
			expectedTag: &gitalypb.Tag{
				Name:         []byte(inputTagName),
				TargetCommit: targetRevisionCommit,
			},
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
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			request := &gitalypb.UserCreateTagRequest{
				Repository:     testRepo,
				TagName:        []byte(inputTagName),
				TargetRevision: []byte(testCase.targetRevision),
				User:           user,
				Message:        []byte(testCase.message),
			}

			ctx, cancel := testhelper.Context()
			defer cancel()

			response, err := client.UserCreateTag(ctx, request)
			defer exec.Command("git", "-C", testRepoPath, "tag", "-d", inputTagName).Run()

			id := testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "rev-parse", inputTagName)
			testCase.expectedTag.Id = strings.TrimSpace(string(id))

			require.NoError(t, err)
			require.Equal(t, testCase.expectedTag, response.Tag)
			require.Empty(t, response.PreReceiveError)

			tag := testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "tag")
			require.Contains(t, string(tag), inputTagName)
		})
	}
}

func TestSuccessfulGitHooksForUserCreateTagRequest(t *testing.T) {
	server, serverSocketPath := runOperationServiceServer(t)
	defer server.Stop()

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
	request := &gitalypb.UserCreateTagRequest{
		Repository:     testRepo,
		TagName:        []byte(tagName),
		TargetRevision: []byte("c7fbe50c7c7419d9701eebe64b1fdacc3df5b9dd"),
		User:           user,
	}

	for _, hookName := range GitlabHooks {
		t.Run(hookName, func(t *testing.T) {
			defer exec.Command("git", "-C", testRepoPath, "tag", "-d", tagName).Run()

			hookPath, hookOutputTempPath := WriteEnvToHook(t, testRepoPath, hookName)
			defer os.Remove(hookPath)
			defer os.Remove(hookOutputTempPath)

			ctx, cancel := testhelper.Context()
			defer cancel()

			response, err := client.UserCreateTag(ctx, request)
			require.NoError(t, err)
			require.Empty(t, response.PreReceiveError)

			output := string(testhelper.MustReadFile(t, hookOutputTempPath))
			require.Contains(t, output, "GL_ID="+user.GlId)
			require.Contains(t, output, "GL_USERNAME="+user.GlUsername)
		})
	}
}

func TestFailedUserDeleteTagRequestDueToValidation(t *testing.T) {
	server, serverSocketPath := runOperationServiceServer(t)
	defer server.Stop()

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
	server, serverSocketPath := runOperationServiceServer(t)
	defer server.Stop()

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

	request := &gitalypb.UserDeleteTagRequest{
		Repository: testRepo,
		TagName:    []byte(tagNameInput),
		User:       user,
	}

	hookContent := []byte("#!/bin/sh\necho GL_ID=$GL_ID\nexit 1")

	for _, hookName := range gitlabPreHooks {
		t.Run(hookName, func(t *testing.T) {
			hookPath := path.Join(testRepoPath, "hooks", hookName)
			ioutil.WriteFile(hookPath, hookContent, 0755)
			defer os.Remove(hookPath)

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
	server, serverSocketPath := runOperationServiceServer(t)
	defer server.Stop()

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
	request := &gitalypb.UserCreateTagRequest{
		Repository:     testRepo,
		TagName:        []byte("new-tag"),
		TargetRevision: []byte("c7fbe50c7c7419d9701eebe64b1fdacc3df5b9dd"),
		User:           user,
	}

	hookContent := []byte("#!/bin/sh\necho GL_ID=$GL_ID\nexit 1")

	for _, hookName := range gitlabPreHooks {
		hookPath := path.Join(testRepoPath, "hooks", hookName)
		ioutil.WriteFile(hookPath, hookContent, 0755)
		defer os.Remove(hookPath)

		ctx, cancel := testhelper.Context()
		defer cancel()

		response, err := client.UserCreateTag(ctx, request)
		require.Nil(t, err)
		require.Contains(t, response.PreReceiveError, "GL_ID="+user.GlId)
	}
}

func TestFailedUserCreateTagRequestDueToTagExistence(t *testing.T) {
	server, serverSocketPath := runOperationServiceServer(t)
	defer server.Stop()

	client, conn := newOperationClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	user := &gitalypb.User{
		Name:  []byte("Ahmad Sherif"),
		Email: []byte("ahmad@gitlab.com"),
		GlId:  "user-123",
	}

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
	server, serverSocketPath := runOperationServiceServer(t)
	defer server.Stop()

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
