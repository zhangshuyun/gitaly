package operations

import (
	"context"
	"fmt"
	"io/ioutil"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/command"
	"gitlab.com/gitlab-org/gitaly/internal/git/log"
	"gitlab.com/gitlab-org/gitaly/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
)

func TestSuccessfulUserDeleteTagRequest(t *testing.T) {
	testWithFeature(t, featureflag.GoUserDeleteTag, testSuccessfulUserDeleteTagRequest)
}

func testSuccessfulUserDeleteTagRequest(t *testing.T, ctx context.Context) {
	serverSocketPath, stop := runOperationServiceServer(t)
	defer stop()

	client, conn := newOperationClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	tagNameInput := "to-be-deleted-soon-tag"

	defer exec.Command(command.GitPath(), "-C", testRepoPath, "tag", "-d", tagNameInput).Run()

	testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "tag", tagNameInput)

	request := &gitalypb.UserDeleteTagRequest{
		Repository: testRepo,
		TagName:    []byte(tagNameInput),
		User:       testhelper.TestUser,
	}

	_, err := client.UserDeleteTag(ctx, request)
	require.NoError(t, err)

	tags := testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "tag")
	require.NotContains(t, string(tags), tagNameInput, "tag name still exists in tags list")
}

func TestSuccessfulGitHooksForUserDeleteTagRequest(t *testing.T) {
	testWithFeature(t, featureflag.GoUserDeleteTag, testSuccessfulGitHooksForUserDeleteTagRequest)
}

func testSuccessfulGitHooksForUserDeleteTagRequest(t *testing.T, ctx context.Context) {
	serverSocketPath, stop := runOperationServiceServer(t)
	defer stop()

	client, conn := newOperationClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	tagNameInput := "to-be-déleted-soon-tag"
	defer exec.Command(command.GitPath(), "-C", testRepoPath, "tag", "-d", tagNameInput).Run()

	request := &gitalypb.UserDeleteTagRequest{
		Repository: testRepo,
		TagName:    []byte(tagNameInput),
		User:       testhelper.TestUser,
	}

	for _, hookName := range GitlabHooks {
		t.Run(hookName, func(t *testing.T) {
			testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "tag", tagNameInput)

			hookOutputTempPath, cleanup := testhelper.WriteEnvToCustomHook(t, testRepoPath, hookName)
			defer cleanup()

			_, err := client.UserDeleteTag(ctx, request)
			require.NoError(t, err)

			output := testhelper.MustReadFile(t, hookOutputTempPath)
			require.Contains(t, string(output), "GL_USERNAME="+testhelper.TestUser.GlUsername)
		})
	}
}

func writeAssertObjectTypePreReceiveHook(t *testing.T) (string, func()) {
	t.Helper()

	hook := fmt.Sprintf(`#!/usr/bin/env ruby

commands = STDIN.each_line.map(&:chomp)
unless commands.size == 1
  abort "expected 1 ref update command, got #{commands.size}"
end

new_value = commands[0].split(' ', 3)[1]
abort 'missing new_value' unless new_value

out = IO.popen(%%W[%s cat-file -t #{new_value}], &:read)
abort 'cat-file failed' unless $?.success?

unless out.chomp == ARGV[0]
  abort "error: expected #{ARGV[0]} object, got #{out}"
end`, command.GitPath())

	dir, cleanup := testhelper.TempDir(t)
	hookPath := filepath.Join(dir, "pre-receive")

	require.NoError(t, ioutil.WriteFile(hookPath, []byte(hook), 0755))

	return hookPath, cleanup
}

func writeAssertObjectTypeUpdateHook(t *testing.T) (string, func()) {
	t.Helper()

	hook := fmt.Sprintf(`#!/usr/bin/env ruby

expected_object_type = ARGV.shift
new_value = ARGV[2]

abort "missing new_value" unless new_value

out = IO.popen(%%W[%s cat-file -t #{new_value}], &:read)
abort 'cat-file failed' unless $?.success?

unless out.chomp == expected_object_type
  abort "error: expected #{expected_object_type} object, got #{out}"
end`, command.GitPath())

	dir, cleanup := testhelper.TempDir(t)
	hookPath := filepath.Join(dir, "pre-receive")

	require.NoError(t, ioutil.WriteFile(hookPath, []byte(hook), 0755))

	return hookPath, cleanup
}

func TestSuccessfulUserCreateTagRequest(t *testing.T) {
	testWithFeature(t, featureflag.GoUserCreateTag, testSuccessfulUserCreateTagRequest)
}

func testSuccessfulUserCreateTagRequest(t *testing.T, ctx context.Context) {
	serverSocketPath, stop := runOperationServiceServer(t)
	defer stop()

	client, conn := newOperationClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	targetRevision := "c7fbe50c7c7419d9701eebe64b1fdacc3df5b9dd"
	targetRevisionCommit, err := log.GetCommit(ctx, testRepo, targetRevision)
	require.NoError(t, err)

	inputTagName := "to-be-créated-soon"

	preReceiveHook, cleanup := writeAssertObjectTypePreReceiveHook(t)
	defer cleanup()

	updateHook, cleanup := writeAssertObjectTypeUpdateHook(t)
	defer cleanup()

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
				User:           testhelper.TestUser,
				Message:        []byte(testCase.message),
			}

			response, err := client.UserCreateTag(ctx, request)
			require.NoError(t, err, "error from calling RPC")
			require.Empty(t, response.PreReceiveError, "PreReceiveError must be empty, signalling the push was accepted")

			defer exec.Command(command.GitPath(), "-C", testRepoPath, "tag", "-d", inputTagName).Run()

			id := testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "rev-parse", inputTagName)
			testCase.expectedTag.Id = text.ChompBytes(id)

			require.Equal(t, testCase.expectedTag, response.Tag)

			tag := testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "tag")
			require.Contains(t, string(tag), inputTagName)
		})
	}
}

func TestSuccessfulGitHooksForUserCreateTagRequest(t *testing.T) {
	testWithFeature(t, featureflag.GoUserCreateTag, testSuccessfulGitHooksForUserCreateTagRequest)
}

func testSuccessfulGitHooksForUserCreateTagRequest(t *testing.T, ctx context.Context) {
	serverSocketPath, stop := runOperationServiceServer(t)
	defer stop()

	client, conn := newOperationClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	projectPath := "project/path"
	testRepo.GlProjectPath = projectPath

	tagName := "new-tag"

	request := &gitalypb.UserCreateTagRequest{
		Repository:     testRepo,
		TagName:        []byte(tagName),
		TargetRevision: []byte("c7fbe50c7c7419d9701eebe64b1fdacc3df5b9dd"),
		User:           testhelper.TestUser,
	}

	for _, hookName := range GitlabHooks {
		t.Run(hookName, func(t *testing.T) {
			defer exec.Command(command.GitPath(), "-C", testRepoPath, "tag", "-d", tagName).Run()

			hookOutputTempPath, cleanup := testhelper.WriteEnvToCustomHook(t, testRepoPath, hookName)
			defer cleanup()

			response, err := client.UserCreateTag(ctx, request)
			require.NoError(t, err)
			require.Empty(t, response.PreReceiveError)

			output := string(testhelper.MustReadFile(t, hookOutputTempPath))
			require.Contains(t, output, "GL_USERNAME="+testhelper.TestUser.GlUsername)
			require.Contains(t, output, "GL_PROJECT_PATH="+projectPath)
		})
	}
}

func TestFailedUserDeleteTagRequestDueToValidation(t *testing.T) {
	testWithFeature(t, featureflag.GoUserDeleteTag, testFailedUserDeleteTagRequestDueToValidation)
}

func testFailedUserDeleteTagRequestDueToValidation(t *testing.T, ctx context.Context) {
	serverSocketPath, stop := runOperationServiceServer(t)
	defer stop()

	client, conn := newOperationClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

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
				User:       testhelper.TestUser,
			},
			code: codes.InvalidArgument,
		},
		{
			desc: "non-existent tag name",
			request: &gitalypb.UserDeleteTagRequest{
				Repository: testRepo,
				User:       testhelper.TestUser,
				TagName:    []byte("i-do-not-exist"),
			},
			code: codes.FailedPrecondition,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			_, err := client.UserDeleteTag(ctx, testCase.request)
			testhelper.RequireGrpcError(t, err, testCase.code)
		})
	}
}

func TestFailedUserDeleteTagDueToHooks(t *testing.T) {
	testWithFeature(t, featureflag.GoUserDeleteTag, testFailedUserDeleteTagDueToHooks)
}

func testFailedUserDeleteTagDueToHooks(t *testing.T, ctx context.Context) {
	serverSocketPath, stop := runOperationServiceServer(t)
	defer stop()

	client, conn := newOperationClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	tagNameInput := "to-be-deleted-soon-tag"
	testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "tag", tagNameInput)
	defer exec.Command(command.GitPath(), "-C", testRepoPath, "tag", "-d", tagNameInput).Run()

	request := &gitalypb.UserDeleteTagRequest{
		Repository: testRepo,
		TagName:    []byte(tagNameInput),
		User:       testhelper.TestUser,
	}

	hookContent := []byte("#!/bin/sh\necho GL_ID=$GL_ID\nexit 1")

	for _, hookName := range gitlabPreHooks {
		t.Run(hookName, func(t *testing.T) {
			remove, err := testhelper.WriteCustomHook(testRepoPath, hookName, hookContent)
			require.NoError(t, err)
			defer remove()

			response, err := client.UserDeleteTag(ctx, request)
			require.Nil(t, err)
			require.Contains(t, response.PreReceiveError, "GL_ID="+testhelper.TestUser.GlId)

			tags := testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "tag")
			require.Contains(t, string(tags), tagNameInput, "tag name does not exist in tags list")
		})
	}
}

func TestFailedUserCreateTagDueToHooks(t *testing.T) {
	testWithFeature(t, featureflag.GoUserCreateTag, testFailedUserCreateTagDueToHooks)
}

func testFailedUserCreateTagDueToHooks(t *testing.T, ctx context.Context) {
	serverSocketPath, stop := runOperationServiceServer(t)
	defer stop()

	client, conn := newOperationClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	request := &gitalypb.UserCreateTagRequest{
		Repository:     testRepo,
		TagName:        []byte("new-tag"),
		TargetRevision: []byte("c7fbe50c7c7419d9701eebe64b1fdacc3df5b9dd"),
		User:           testhelper.TestUser,
	}

	hookContent := []byte("#!/bin/sh\necho GL_ID=$GL_ID\nexit 1")

	for _, hookName := range gitlabPreHooks {
		remove, err := testhelper.WriteCustomHook(testRepoPath, hookName, hookContent)
		require.NoError(t, err)
		defer remove()

		response, err := client.UserCreateTag(ctx, request)
		require.Nil(t, err)
		require.Contains(t, response.PreReceiveError, "GL_ID="+testhelper.TestUser.GlId)
	}
}

func TestFailedUserCreateTagRequestDueToTagExistence(t *testing.T) {
	testWithFeature(t, featureflag.GoUserCreateTag, testFailedUserCreateTagRequestDueToTagExistence)
}

func testFailedUserCreateTagRequestDueToTagExistence(t *testing.T, ctx context.Context) {
	serverSocketPath, stop := runOperationServiceServer(t)
	defer stop()

	client, conn := newOperationClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	testCase := struct {
		tagName        string
		targetRevision string
		user           *gitalypb.User
	}{
		tagName:        "v1.1.0",
		targetRevision: "master",
		user:           testhelper.TestUser,
	}

	request := &gitalypb.UserCreateTagRequest{
		Repository:     testRepo,
		TagName:        []byte(testCase.tagName),
		TargetRevision: []byte(testCase.targetRevision),
		User:           testCase.user,
	}

	response, err := client.UserCreateTag(ctx, request)
	require.NoError(t, err)
	require.Equal(t, response.Exists, true)
}

func TestFailedUserCreateTagRequestDueToValidation(t *testing.T) {
	testWithFeature(t, featureflag.GoUserCreateTag, testFailedUserCreateTagRequestDueToValidation)
}

func testFailedUserCreateTagRequestDueToValidation(t *testing.T, ctx context.Context) {
	serverSocketPath, stop := runOperationServiceServer(t)
	defer stop()

	client, conn := newOperationClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

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
			user:           testhelper.TestUser,
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
			user:           testhelper.TestUser,
			code:           codes.FailedPrecondition,
		},
		// TODO: Test for TagExistsError
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			request := &gitalypb.UserCreateTagRequest{
				Repository:     testRepo,
				TagName:        []byte(testCase.tagName),
				TargetRevision: []byte(testCase.targetRevision),
				User:           testCase.user,
			}

			_, err := client.UserCreateTag(ctx, request)
			testhelper.RequireGrpcError(t, err, testCase.code)
		})
	}
}

func TestTagHookOutput(t *testing.T) {
	testWithFeature(t, featureflag.GoUserDeleteTag, testTagHookOutput)
}

func testTagHookOutput(t *testing.T, ctx context.Context) {
	serverSocketPath, stop := runOperationServiceServer(t)
	defer stop()

	client, conn := newOperationClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	testCases := []struct {
		desc        string
		hookContent string
		output      string
	}{
		{
			desc:        "empty stdout and empty stderr",
			hookContent: "#!/bin/sh\nexit 1",
			output:      "",
		},
		{
			desc:        "empty stdout and some stderr",
			hookContent: "#!/bin/sh\necho stderr >&2\nexit 1",
			output:      "stderr\n",
		},
		{
			desc:        "some stdout and empty stderr",
			hookContent: "#!/bin/sh\necho stdout\nexit 1",
			output:      "stdout\n",
		},
		{
			desc:        "some stdout and some stderr",
			hookContent: "#!/bin/sh\necho stdout\necho stderr >&2\nexit 1",
			output:      "stderr\n",
		},
		{
			desc:        "whitespace stdout and some stderr",
			hookContent: "#!/bin/sh\necho '   '\necho stderr >&2\nexit 1",
			output:      "stderr\n",
		},
		{
			desc:        "some stdout and whitespace stderr",
			hookContent: "#!/bin/sh\necho stdout\necho '   ' >&2\nexit 1",
			output:      "stdout\n",
		},
	}

	for _, hookName := range gitlabPreHooks {
		for _, testCase := range testCases {
			t.Run(hookName+"/"+testCase.desc, func(t *testing.T) {
				tagNameInput := "some-tag"
				request := &gitalypb.UserDeleteTagRequest{
					Repository: testRepo,
					TagName:    []byte(tagNameInput),
					User:       testhelper.TestUser,
				}

				ctx, cancel := testhelper.Context()
				defer cancel()

				remove, err := testhelper.WriteCustomHook(testRepoPath, hookName, []byte(testCase.hookContent))
				require.NoError(t, err)
				defer remove()

				defer exec.Command(command.GitPath(), "-C", testRepoPath, "tag", "-d", tagNameInput).Run()
				testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "tag", tagNameInput)

				response, err := client.UserDeleteTag(ctx, request)
				require.NoError(t, err)
				require.Equal(t, testCase.output, response.PreReceiveError)
			})
		}
	}
}
