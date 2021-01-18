package operations

import (
	"context"
	"fmt"
	"io/ioutil"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/log"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestSuccessfulUserDeleteTagRequest(t *testing.T) {
	testhelper.NewFeatureSets([]featureflag.FeatureFlag{
		featureflag.ReferenceTransactions,
		featureflag.GoUserDeleteTag,
	}).Run(t, testSuccessfulUserDeleteTagRequest)
}

func testSuccessfulUserDeleteTagRequest(t *testing.T, ctx context.Context) {
	serverSocketPath, stop := runOperationServiceServer(t)
	defer stop()

	client, conn := newOperationClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	tagNameInput := "to-be-deleted-soon-tag"

	defer exec.Command(config.Config.Git.BinPath, "-C", testRepoPath, "tag", "-d", tagNameInput).Run()

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
	testhelper.NewFeatureSets([]featureflag.FeatureFlag{
		featureflag.ReferenceTransactions,
		featureflag.GoUserDeleteTag,
	}).Run(t, testSuccessfulGitHooksForUserDeleteTagRequest)
}

func testSuccessfulGitHooksForUserDeleteTagRequest(t *testing.T, ctx context.Context) {
	serverSocketPath, stop := runOperationServiceServer(t)
	defer stop()

	client, conn := newOperationClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	tagNameInput := "to-be-déleted-soon-tag"
	defer exec.Command(config.Config.Git.BinPath, "-C", testRepoPath, "tag", "-d", tagNameInput).Run()

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

# We match a non-ASCII ref_name below
Encoding.default_external = Encoding::UTF_8
Encoding.default_internal = Encoding::UTF_8

expected_object_type = ARGV.shift
commands = STDIN.each_line.map(&:chomp)
unless commands.size == 1
  abort "expected 1 ref update command, got #{commands.size}"
end

old_value, new_value, ref_name = commands[0].split(' ', 3)
abort 'missing new_value' unless new_value

out = IO.popen(%%W[%s cat-file -t #{new_value}], &:read)
abort 'cat-file failed' unless $?.success?

if ref_name =~ /^refs\/[^\/]+\/skip-type-check-/
  exit 0
end

unless out.chomp == expected_object_type
  abort "pre-receive hook error: expected '#{ref_name}' update of '#{old_value}' (a) -> '#{new_value}' (b) for 'b' to be a '#{expected_object_type}' object, got '#{out}'"
end`, config.Config.Git.BinPath)

	dir, cleanup := testhelper.TempDir(t)
	hookPath := filepath.Join(dir, "pre-receive")

	require.NoError(t, ioutil.WriteFile(hookPath, []byte(hook), 0755))

	return hookPath, cleanup
}

func writeAssertObjectTypeUpdateHook(t *testing.T) (string, func()) {
	t.Helper()

	hook := fmt.Sprintf(`#!/usr/bin/env ruby

# We match a non-ASCII ref_name below
Encoding.default_external = Encoding::UTF_8
Encoding.default_internal = Encoding::UTF_8

expected_object_type = ARGV.shift
ref_name, old_value, new_value = ARGV[0..2]

abort "missing new_value" unless new_value

out = IO.popen(%%W[%s cat-file -t #{new_value}], &:read)
abort 'cat-file failed' unless $?.success?

if ref_name =~ /^refs\/[^\/]+\/skip-type-check-/
  exit 0
end

unless out.chomp == expected_object_type
  abort "update hook error: expected '#{ref_name}' update of '#{old_value}' (a) -> '#{new_value}' (b) for 'b' to be a '#{expected_object_type}' object, got '#{out}'"
end`, config.Config.Git.BinPath)

	dir, cleanup := testhelper.TempDir(t)
	hookPath := filepath.Join(dir, "pre-receive")

	require.NoError(t, ioutil.WriteFile(hookPath, []byte(hook), 0755))

	return hookPath, cleanup
}

func TestSuccessfulUserCreateTagRequest(t *testing.T) {
	testhelper.NewFeatureSets([]featureflag.FeatureFlag{
		featureflag.ReferenceTransactions,
		featureflag.GoUserCreateTag,
	}).Run(t, testSuccessfulUserCreateTagRequest)
}

func testSuccessfulUserCreateTagRequest(t *testing.T, ctx context.Context) {
	locator := config.NewLocator(config.Config)

	serverSocketPath, stop := runOperationServiceServer(t)
	defer stop()

	client, conn := newOperationClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	targetRevision := "c7fbe50c7c7419d9701eebe64b1fdacc3df5b9dd"
	targetRevisionCommit, err := log.GetCommit(ctx, locator, testRepo, git.Revision(targetRevision))
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
			desc:           "lightweight tag to commit",
			tagName:        inputTagName,
			targetRevision: targetRevision,
			expectedTag: &gitalypb.Tag{
				Name:         []byte(inputTagName),
				Id:           targetRevision,
				TargetCommit: targetRevisionCommit,
			},
			expectedObjectType: "commit",
		},
		{
			desc:           "annotated tag to commit",
			tagName:        inputTagName,
			targetRevision: targetRevision,
			message:        "This is an annotated tag",
			expectedTag: &gitalypb.Tag{
				Name: []byte(inputTagName),
				//Id: is a new object, filled in below
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
				hookCleanup := testhelper.WriteCustomHook(t, testRepoPath, hook, []byte(content))
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

			defer testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "tag", "-d", inputTagName)

			responseOk := &gitalypb.UserCreateTagResponse{
				Tag: testCase.expectedTag,
			}
			// Fake up *.Id for annotated tags
			if len(testCase.expectedTag.Id) == 0 {
				id := testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "rev-parse", inputTagName)
				responseOk.Tag.Id = text.ChompBytes(id)
			}

			require.Equal(t, responseOk, response)

			tag := testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "tag")
			require.Contains(t, string(tag), inputTagName)
		})
	}
}

func TestSuccessfulUserCreateTagRequestAnnotatedLightweightDisambiguation(t *testing.T) {
	testhelper.NewFeatureSets([]featureflag.FeatureFlag{
		featureflag.ReferenceTransactions,
		featureflag.GoUserCreateTag,
	}).Run(t, testSuccessfulUserCreateTagRequestAnnotatedLightweightDisambiguation)
}

func testSuccessfulUserCreateTagRequestAnnotatedLightweightDisambiguation(t *testing.T, ctx context.Context) {
	serverSocketPath, stop := runOperationServiceServer(t)
	defer stop()

	client, conn := newOperationClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	preReceiveHook, cleanup := writeAssertObjectTypePreReceiveHook(t)
	defer cleanup()

	updateHook, cleanup := writeAssertObjectTypeUpdateHook(t)
	defer cleanup()

	testCases := []struct {
		desc    string
		message string
		objType string
		err     error
	}{
		{
			desc:    "error: contains null byte",
			message: "\000",
			err:     status.Error(codes.Unknown, "ArgumentError: string contains null byte"),
		},
		{
			desc:    "annotated: some control characters",
			message: "\u0001\u0002\u0003\u0004\u0005\u0006\u0007\u0008",
			objType: "tag",
		},
		{
			desc:    "lightweight: empty message",
			message: "",
			objType: "commit",
		},
		{
			desc:    "lightweight: simple whitespace",
			message: " \t\t",
			objType: "commit",
		},
		{
			desc:    "lightweight: whitespace with newlines",
			message: "\t\n\f\r ",
			objType: "commit",
		},
		{
			desc:    "lightweight: simple Unicode whitespace",
			message: "\u00a0",
			objType: "tag",
		},
		{
			desc:    "lightweight: lots of Unicode whitespace",
			message: "\u0020\u00a0\u1680\u180e\u2000\u2001\u2002\u2003\u2004\u2005\u2006\u2007\u2008\u2009\u200a\u200b\u202f\u205f\u3000\ufeff",
			objType: "tag",
		},
		{
			desc:    "annotated: dot",
			message: ".",
			objType: "tag",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			for hook, content := range map[string]string{
				"pre-receive": fmt.Sprintf("#!/bin/sh\n%s %s \"$@\"", preReceiveHook, testCase.objType),
				"update":      fmt.Sprintf("#!/bin/sh\n%s %s \"$@\"", updateHook, testCase.objType),
			} {
				hookCleanup := testhelper.WriteCustomHook(t, testRepoPath, hook, []byte(content))
				defer hookCleanup()
			}

			tagName := "what-will-it-be"
			request := &gitalypb.UserCreateTagRequest{
				Repository:     testRepo,
				TagName:        []byte(tagName),
				TargetRevision: []byte("c7fbe50c7c7419d9701eebe64b1fdacc3df5b9dd"),
				User:           testhelper.TestUser,
				Message:        []byte(testCase.message),
			}

			response, err := client.UserCreateTag(ctx, request)

			if testCase.err != nil {
				require.Equal(t, testCase.err, err)
			} else {
				defer testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "tag", "-d", tagName)
				require.NoError(t, err)
				require.Empty(t, response.PreReceiveError)
			}
		})
	}
}

func TestSuccessfulUserCreateTagRequestWithParsedTargetRevision(t *testing.T) {
	testhelper.NewFeatureSets([]featureflag.FeatureFlag{
		featureflag.ReferenceTransactions,
		featureflag.GoUserCreateTag,
	}).Run(t, testSuccessfulUserCreateTagRequestWithParsedTargetRevision)
}

func testSuccessfulUserCreateTagRequestWithParsedTargetRevision(t *testing.T, ctx context.Context) {
	serverSocketPath, stop := runOperationServiceServer(t)
	defer stop()

	client, conn := newOperationClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "branch", "heads/master", "master~1")
	defer testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "branch", "-d", "heads/master")
	testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "branch", "refs/heads/master", "master~2")
	defer testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "branch", "-d", "refs/heads/master")

	testCases := []struct {
		desc             string
		targetRevision   string
		expectedRevision string
	}{
		{
			desc:             "tag",
			targetRevision:   "v1.0.0",
			expectedRevision: "refs/tags/v1.0.0",
		},
		{
			desc:             "tag~",
			targetRevision:   "v1.0.0~",
			expectedRevision: "refs/tags/v1.0.0~",
		},
		{
			desc:             "tags/tag~",
			targetRevision:   "tags/v1.0.0~",
			expectedRevision: "refs/tags/v1.0.0~",
		},
		{
			desc:             "refs/tag~",
			targetRevision:   "refs/tags/v1.0.0~",
			expectedRevision: "refs/tags/v1.0.0~",
		},
		{
			desc:             "master",
			targetRevision:   "master",
			expectedRevision: "master",
		},
		{
			desc:             "heads/master",
			targetRevision:   "heads/master",
			expectedRevision: "refs/heads/heads/master",
		},
		{
			desc:             "refs/heads/master",
			targetRevision:   "refs/heads/master",
			expectedRevision: "refs/heads/master",
		},
		{
			desc:             "heads/refs/heads/master",
			targetRevision:   "heads/refs/heads/master",
			expectedRevision: "refs/heads/refs/heads/master",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			tagName := "what-will-it-be"
			request := &gitalypb.UserCreateTagRequest{
				Repository:     testRepo,
				TagName:        []byte(tagName),
				TargetRevision: []byte(testCase.targetRevision),
				User:           testhelper.TestUser,
			}

			response, err := client.UserCreateTag(ctx, request)
			defer testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "tag", "-d", tagName)
			require.NoError(t, err)
			require.Empty(t, response.PreReceiveError)

			parsedID := testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "rev-parse", tagName)
			require.Equal(t, text.ChompBytes(parsedID), response.Tag.TargetCommit.Id)
		})
	}
}

func TestSuccessfulUserCreateTagRequestToNonCommit(t *testing.T) {
	testWithFeature(t, featureflag.GoUserCreateTag, testSuccessfulUserCreateTagRequestToNonCommit)
}

func testSuccessfulUserCreateTagRequestToNonCommit(t *testing.T, ctx context.Context) {
	serverSocketPath, stop := runOperationServiceServer(t)
	defer stop()

	client, conn := newOperationClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

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
			desc:           "lightweight tag to tree",
			tagName:        inputTagName,
			targetRevision: "612036fac47c5d31c212b17268e2f3ba807bce1e",
			expectedTag: &gitalypb.Tag{
				Name: []byte(inputTagName),
				Id:   "612036fac47c5d31c212b17268e2f3ba807bce1e",
			},
			expectedObjectType: "tree",
		},
		{
			desc:           "lightweight tag to blob",
			tagName:        inputTagName,
			targetRevision: "dfaa3f97ca337e20154a98ac9d0be76ddd1fcc82",
			expectedTag: &gitalypb.Tag{
				Name: []byte(inputTagName),
				Id:   "dfaa3f97ca337e20154a98ac9d0be76ddd1fcc82",
			},
			expectedObjectType: "blob",
		},
		{
			desc:           "annotated tag to tree",
			tagName:        inputTagName,
			targetRevision: "612036fac47c5d31c212b17268e2f3ba807bce1e",
			message:        "This is an annotated tag",
			expectedTag: &gitalypb.Tag{
				Name: []byte(inputTagName),
				//Id: is a new object, filled in below
				TargetCommit: nil,
				Message:      []byte("This is an annotated tag"),
				MessageSize:  24,
			},
			expectedObjectType: "tag",
		},
		{
			desc:           "annotated tag to blob",
			tagName:        inputTagName,
			targetRevision: "dfaa3f97ca337e20154a98ac9d0be76ddd1fcc82",
			message:        "This is an annotated tag",
			expectedTag: &gitalypb.Tag{
				Name: []byte(inputTagName),
				//Id: is a new object, filled in below
				TargetCommit: nil,
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
				hookCleanup := testhelper.WriteCustomHook(t, testRepoPath, hook, []byte(content))
				defer hookCleanup()
			}

			request := &gitalypb.UserCreateTagRequest{
				Repository:     testRepo,
				TagName:        []byte(testCase.tagName),
				TargetRevision: []byte(testCase.targetRevision),
				User:           testhelper.TestUser,
				Message:        []byte(testCase.message),
			}

			responseOk := &gitalypb.UserCreateTagResponse{
				Tag: testCase.expectedTag,
			}
			response, err := client.UserCreateTag(ctx, request)
			require.NoError(t, err)
			require.Empty(t, response.PreReceiveError)
			defer testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "tag", "-d", inputTagName)

			// Fake up *.Id for annotated tags
			if len(testCase.expectedTag.Id) == 0 {
				tagID := testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "rev-parse", inputTagName)
				responseOk.Tag.Id = text.ChompBytes(tagID)
			}
			require.Equal(t, responseOk, response)

			peeledID := testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "rev-parse", inputTagName+"^{}")
			require.Equal(t, testCase.targetRevision, text.ChompBytes(peeledID))

			objectType := testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "cat-file", "-t", inputTagName)
			require.Equal(t, testCase.expectedObjectType, text.ChompBytes(objectType))
		})
	}
}

func TestSuccessfulUserCreateTagNestedTags(t *testing.T) {
	testWithFeature(t, featureflag.GoUserCreateTag, testSuccessfulUserCreateTagNestedTags)
}

func testSuccessfulUserCreateTagNestedTags(t *testing.T, ctx context.Context) {
	locator := config.NewLocator(config.Config)

	serverSocketPath, stop := runOperationServiceServer(t)
	defer stop()

	client, conn := newOperationClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	preReceiveHook, cleanup := writeAssertObjectTypePreReceiveHook(t)
	defer cleanup()

	updateHook, cleanup := writeAssertObjectTypeUpdateHook(t)
	defer cleanup()

	testCases := []struct {
		desc             string
		targetObject     string
		targetObjectType string
		expectedTag      *gitalypb.Tag
	}{
		{
			desc:             "nested tags to commit",
			targetObject:     "c7fbe50c7c7419d9701eebe64b1fdacc3df5b9dd",
			targetObjectType: "commit",
		},
		{
			desc:             "nested tags to tree",
			targetObjectType: "tree",
			targetObject:     "612036fac47c5d31c212b17268e2f3ba807bce1e",
		},
		{
			desc:             "nested tags to blob",
			targetObject:     "dfaa3f97ca337e20154a98ac9d0be76ddd1fcc82",
			targetObjectType: "blob",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			// We resolve down to commit/tree/blob, but
			// we'll only ever push a "tag" here.
			hookObjectType := "tag"
			for hook, content := range map[string]string{
				"pre-receive": fmt.Sprintf("#!/bin/sh\n%s %s \"$@\"", preReceiveHook, hookObjectType),
				"update":      fmt.Sprintf("#!/bin/sh\n%s %s \"$@\"", updateHook, hookObjectType),
			} {
				hookCleanup := testhelper.WriteCustomHook(t, testRepoPath, hook, []byte(content))
				defer hookCleanup()
			}

			targetObject := testCase.targetObject
			nestLevel := 2
			for i := 0; i <= nestLevel; i++ {
				tagName := fmt.Sprintf("nested-tag-%v", i)
				tagMessage := fmt.Sprintf("This is level %v of a nested annotated tag to %v", i, testCase.targetObject)
				request := &gitalypb.UserCreateTagRequest{
					Repository:     testRepo,
					TagName:        []byte(tagName),
					TargetRevision: []byte(targetObject),
					User:           testhelper.TestUser,
					Message:        []byte(tagMessage),
				}
				response, err := client.UserCreateTag(ctx, request)
				require.NoError(t, err)
				require.Empty(t, response.PreReceiveError)
				defer testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "tag", "-d", tagName)

				createdID := testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "rev-parse", tagName)
				createdIDStr := text.ChompBytes(createdID)
				responseOk := &gitalypb.UserCreateTagResponse{
					Tag: &gitalypb.Tag{
						Name: request.TagName,
						Id:   createdIDStr,
						//TargetCommit: is dymamically determined, filled in below
						Message:     request.Message,
						MessageSize: int64(len(request.Message)),
					},
				}
				// Fake it up for all levels, except for ^{} == "commit"
				responseOk.Tag.TargetCommit = response.Tag.TargetCommit
				if testCase.targetObjectType == "commit" {
					responseOk.Tag.TargetCommit, err = log.GetCommit(ctx, locator, testRepo, git.Revision(testCase.targetObject))
					require.NoError(t, err)
				}
				require.Equal(t, responseOk, response)

				peeledID := testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "rev-parse", tagName+"^{}")
				peeledIDStr := text.ChompBytes(peeledID)
				require.Equal(t, testCase.targetObject, peeledIDStr)

				// Set up the next level of nesting...
				targetObject = response.Tag.Id

				// Create a *lightweight* tag pointing
				// to our N-level
				// tag->[commit|tree|blob]. The "tag"
				// field name will not match the tag
				// name.
				tagNameLight := fmt.Sprintf("skip-type-check-light-%s", tagName)
				request = &gitalypb.UserCreateTagRequest{
					Repository:     testRepo,
					TagName:        []byte(tagNameLight),
					TargetRevision: []byte(createdIDStr),
					User:           testhelper.TestUser,
				}
				response, err = client.UserCreateTag(ctx, request)
				defer testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "tag", "-d", tagNameLight)
				require.NoError(t, err)
				require.Empty(t, response.PreReceiveError)

				responseOk = &gitalypb.UserCreateTagResponse{
					Tag: &gitalypb.Tag{
						Name:         request.TagName,
						Id:           testCase.targetObject,
						TargetCommit: responseOk.Tag.TargetCommit,
						Message:      nil,
						MessageSize:  0,
					},
				}
				require.Equal(t, responseOk, response)

				createdIDLight := testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "rev-parse", tagNameLight)
				createdIDLightStr := text.ChompBytes(createdIDLight)
				require.Equal(t, testCase.targetObject, createdIDLightStr)
			}
		})
	}
}

// TODO: Rename to TestUserDeleteTag_successfulDeletionOfPrefixedTag,
// see
// https://gitlab.com/gitlab-org/gitaly/-/merge_requests/2839#note_458751929
func TestUserDeleteTagsuccessfulDeletionOfPrefixedTag(t *testing.T) {
	testhelper.NewFeatureSets([]featureflag.FeatureFlag{
		featureflag.ReferenceTransactions,
		featureflag.GoUserDeleteTag,
	}).Run(t, testUserDeleteTagsuccessfulDeletionOfPrefixedTag)
}

func testUserDeleteTagsuccessfulDeletionOfPrefixedTag(t *testing.T, ctx context.Context) {
	testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	serverSocketPath, stop := runOperationServiceServer(t)
	defer stop()

	client, conn := newOperationClient(t, serverSocketPath)
	defer conn.Close()

	testCases := []struct {
		desc         string
		tagNameInput string
		tagCommit    string
		user         *gitalypb.User
		response     *gitalypb.UserDeleteTagResponse
		err          error
	}{
		{
			desc:         "possible to delete a tag called refs/tags/something",
			tagNameInput: "refs/tags/can-find-this",
			tagCommit:    "c642fe9b8b9f28f9225d7ea953fe14e74748d53b",
			user:         testhelper.TestUser,
			response:     &gitalypb.UserDeleteTagResponse{},
			err:          nil,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "tag", testCase.tagNameInput, testCase.tagCommit)
			defer exec.Command(config.Config.Git.BinPath, "-C", testRepoPath, "tag", "-d", testCase.tagNameInput).Run()

			request := &gitalypb.UserDeleteTagRequest{
				Repository: testRepo,
				TagName:    []byte(testCase.tagNameInput),
				User:       testCase.user,
			}

			response, err := client.UserDeleteTag(ctx, request)
			require.Equal(t, testCase.err, err)
			require.Equal(t, testCase.response, response)

			refs := testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "for-each-ref", "--", "refs/tags/"+testCase.tagNameInput)
			require.NotContains(t, string(refs), testCase.tagCommit, "tag kept because we stripped off refs/tags/*")
		})
	}
}

func TestUserCreateTagsuccessfulCreationOfPrefixedTag(t *testing.T) {
	testWithFeature(t, featureflag.GoUserCreateTag, testUserCreateTagsuccessfulCreationOfPrefixedTag)
}

func testUserCreateTagsuccessfulCreationOfPrefixedTag(t *testing.T, ctx context.Context) {
	testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	locator := config.NewLocator(config.Config)

	serverSocketPath, stop := runOperationServiceServer(t)
	defer stop()

	client, conn := newOperationClient(t, serverSocketPath)
	defer conn.Close()

	testCases := []struct {
		desc                   string
		tagNameInput           string
		tagTargetRevisionInput string
		user                   *gitalypb.User
		err                    error
	}{
		{
			desc:                   "possible to create a tag called refs/tags/something",
			tagNameInput:           "refs/tags/can-create-this",
			tagTargetRevisionInput: "1a0b36b3cdad1d2ee32457c102a8c0b7056fa863",
			user:                   testhelper.TestUser,
			err:                    nil,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			defer testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "tag", "-d", testCase.tagNameInput)

			request := &gitalypb.UserCreateTagRequest{
				Repository:     testRepo,
				TagName:        []byte(testCase.tagNameInput),
				TargetRevision: []byte(testCase.tagTargetRevisionInput),
				User:           testCase.user,
			}

			response, err := client.UserCreateTag(ctx, request)
			require.Equal(t, testCase.err, err)
			commitOk, err := log.GetCommit(ctx, locator, testRepo, git.Revision(testCase.tagTargetRevisionInput))
			require.NoError(t, err)

			responseOk := &gitalypb.UserCreateTagResponse{
				Tag: &gitalypb.Tag{
					Name:         []byte(testCase.tagNameInput),
					Id:           testCase.tagTargetRevisionInput,
					TargetCommit: commitOk,
				},
			}

			require.Equal(t, responseOk, response)

			refs := testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "for-each-ref", "--", "refs/tags/"+testCase.tagNameInput)
			require.Contains(t, string(refs), testCase.tagTargetRevisionInput, "tag created, we did not strip off refs/tags/*")
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
			defer testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "tag", "-d", tagName)

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
	testhelper.NewFeatureSets([]featureflag.FeatureFlag{
		featureflag.GoUserDeleteTag,
		featureflag.ReferenceTransactions,
	}).Run(t, testFailedUserDeleteTagRequestDueToValidation)
}

func testFailedUserDeleteTagRequestDueToValidation(t *testing.T, ctx context.Context) {
	serverSocketPath, stop := runOperationServiceServer(t)
	defer stop()

	client, conn := newOperationClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	testCases := []struct {
		desc     string
		request  *gitalypb.UserDeleteTagRequest
		response *gitalypb.UserDeleteTagResponse
		err      error
	}{
		{
			desc: "empty user",
			request: &gitalypb.UserDeleteTagRequest{
				Repository: testRepo,
				TagName:    []byte("does-matter-the-name-if-user-is-empty"),
			},
			response: nil,
			err:      status.Error(codes.InvalidArgument, "empty user"),
		},
		{
			desc: "empty tag name",
			request: &gitalypb.UserDeleteTagRequest{
				Repository: testRepo,
				User:       testhelper.TestUser,
			},
			response: nil,
			err:      status.Error(codes.InvalidArgument, "empty tag name"),
		},
		{
			desc: "non-existent tag name",
			request: &gitalypb.UserDeleteTagRequest{
				Repository: testRepo,
				User:       testhelper.TestUser,
				TagName:    []byte("i-do-not-exist"),
			},
			response: nil,
			err:      status.Errorf(codes.FailedPrecondition, "tag not found: %s", "i-do-not-exist"),
		},
		{
			desc: "space in tag name",
			request: &gitalypb.UserDeleteTagRequest{
				Repository: testRepo,
				User:       testhelper.TestUser,
				TagName:    []byte("a tag"),
			},
			response: nil,
			err:      status.Errorf(codes.FailedPrecondition, "tag not found: %s", "a tag"),
		},
		{
			desc: "newline in tag name",
			request: &gitalypb.UserDeleteTagRequest{
				Repository: testRepo,
				User:       testhelper.TestUser,
				TagName:    []byte("a\ntag"),
			},
			response: nil,
			err:      status.Errorf(codes.FailedPrecondition, "tag not found: %s", "a\ntag"),
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			response, err := client.UserDeleteTag(ctx, testCase.request)
			require.Equal(t, testCase.err, err)
			testhelper.ProtoEqual(t, testCase.response, response)
		})
	}
}

func TestFailedUserDeleteTagDueToHooks(t *testing.T) {
	testhelper.NewFeatureSets([]featureflag.FeatureFlag{
		featureflag.GoUserDeleteTag,
		featureflag.ReferenceTransactions,
	}).Run(t, testFailedUserDeleteTagDueToHooks)
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
	defer testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "tag", "-d", tagNameInput)

	request := &gitalypb.UserDeleteTagRequest{
		Repository: testRepo,
		TagName:    []byte(tagNameInput),
		User:       testhelper.TestUser,
	}

	hookContent := []byte("#!/bin/sh\necho GL_ID=$GL_ID\nexit 1")

	for _, hookName := range gitlabPreHooks {
		t.Run(hookName, func(t *testing.T) {
			remove := testhelper.WriteCustomHook(t, testRepoPath, hookName, hookContent)
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
		remove := testhelper.WriteCustomHook(t, testRepoPath, hookName, hookContent)
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

	testCases := []struct {
		desc           string
		tagName        string
		targetRevision string
		user           *gitalypb.User
		response       *gitalypb.UserCreateTagResponse
		err            error
	}{
		{
			desc:           "simple existing tag",
			tagName:        "v1.1.0",
			targetRevision: "master",
			user:           testhelper.TestUser,
			response: &gitalypb.UserCreateTagResponse{
				Tag:    nil,
				Exists: true,
			},
			err: nil,
		},
		{
			desc:           "existing tag nonexisting target revision",
			tagName:        "v1.1.0",
			targetRevision: "does-not-exist",
			user:           testhelper.TestUser,
			response:       nil,
			err:            status.Errorf(codes.FailedPrecondition, "revspec '%s' not found", "does-not-exist"),
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

			response, err := client.UserCreateTag(ctx, request)
			require.Equal(t, testCase.err, err)
			require.Equal(t, testCase.response, response)
		})
	}
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

	injectedTag := "inject-tag\ntagger . <> 0 +0000\n\nInjected subject\n\n"
	testCases := []struct {
		desc           string
		tagName        string
		targetRevision string
		message        string
		user           *gitalypb.User
		response       *gitalypb.UserCreateTagResponse
		err            error
	}{
		{
			desc:           "empty target revision",
			tagName:        "shiny-new-tag",
			targetRevision: "",
			user:           testhelper.TestUser,
			response:       nil,
			err:            status.Error(codes.InvalidArgument, "empty target revision"),
		},
		{
			desc:           "empty user",
			tagName:        "shiny-new-tag",
			targetRevision: "master",
			user:           nil,
			response:       nil,
			err:            status.Error(codes.InvalidArgument, "empty user"),
		},
		{
			desc:           "empty starting point",
			tagName:        "new-tag",
			targetRevision: "",
			user:           testhelper.TestUser,
			response:       nil,
			err:            status.Error(codes.InvalidArgument, "empty target revision"),
		},
		{
			desc:           "non-existing starting point",
			tagName:        "new-tag",
			targetRevision: "i-dont-exist",
			user:           testhelper.TestUser,
			response:       nil,
			err:            status.Errorf(codes.FailedPrecondition, "revspec '%s' not found", "i-dont-exist"),
		},
		{
			desc:           "space in lightweight tag name",
			tagName:        "a tag",
			targetRevision: "master",
			user:           testhelper.TestUser,
			response:       nil,
			err:            status.Errorf(codes.Unknown, "Gitlab::Git::CommitError: Could not update refs/tags/%s. Please refresh and try again.", "a tag"),
		},
		{
			desc:           "space in annotated tag name",
			tagName:        "a tag",
			targetRevision: "master",
			message:        "a message",
			user:           testhelper.TestUser,
			response:       nil,
			err:            status.Errorf(codes.Unknown, "Gitlab::Git::CommitError: Could not update refs/tags/%s. Please refresh and try again.", "a tag"),
		},
		{
			desc:           "newline in lightweight tag name",
			tagName:        "a\ntag",
			targetRevision: "master",
			user:           testhelper.TestUser,
			response:       nil,
			err:            status.Errorf(codes.Unknown, "Gitlab::Git::CommitError: Could not update refs/tags/%s. Please refresh and try again.", "a\ntag"),
		},
		{
			desc:           "newline in annotated tag name",
			tagName:        "a\ntag",
			targetRevision: "master",
			message:        "a message",
			user:           testhelper.TestUser,
			response:       nil,
			err:            status.Error(codes.Unknown, "Rugged::InvalidError: failed to parse signature - expected prefix doesn't match actual"),
		},
		{
			desc:           "injection in lightweight tag name",
			tagName:        injectedTag,
			targetRevision: "master",
			user:           testhelper.TestUser,
			response:       nil,
			err:            status.Errorf(codes.Unknown, "Gitlab::Git::CommitError: Could not update refs/tags/%s. Please refresh and try again.", injectedTag),
		},
		{
			desc:           "injection in annotated tag name",
			tagName:        injectedTag,
			targetRevision: "master",
			message:        "a message",
			user:           testhelper.TestUser,
			response:       nil,
			err:            status.Errorf(codes.Unknown, "Gitlab::Git::CommitError: Could not update refs/tags/%s. Please refresh and try again.", injectedTag),
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			request := &gitalypb.UserCreateTagRequest{
				Repository:     testRepo,
				TagName:        []byte(testCase.tagName),
				TargetRevision: []byte(testCase.targetRevision),
				User:           testCase.user,
				Message:        []byte(testCase.message),
			}

			response, err := client.UserCreateTag(ctx, request)
			require.Equal(t, testCase.err, err)
			require.Equal(t, testCase.response, response)
		})
	}
}

func TestTagHookOutput(t *testing.T) {
	testhelper.NewFeatureSets([]featureflag.FeatureFlag{
		featureflag.GoUserDeleteTag,
		featureflag.ReferenceTransactions,
		featureflag.GoUserCreateTag,
	}).Run(t, testTagHookOutput)
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
				createRequest := &gitalypb.UserCreateTagRequest{
					Repository:     testRepo,
					TagName:        []byte(tagNameInput),
					TargetRevision: []byte("master"),
					User:           testhelper.TestUser,
				}
				deleteRequest := &gitalypb.UserDeleteTagRequest{
					Repository: testRepo,
					TagName:    []byte(tagNameInput),
					User:       testhelper.TestUser,
				}

				remove := testhelper.WriteCustomHook(t, testRepoPath, hookName, []byte(testCase.hookContent))
				defer remove()

				createResponse, err := client.UserCreateTag(ctx, createRequest)
				require.NoError(t, err)

				createResponseOk := &gitalypb.UserCreateTagResponse{
					Tag:             createResponse.Tag,
					Exists:          false,
					PreReceiveError: testCase.output,
				}
				require.Equal(t, createResponseOk, createResponse)

				defer testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "tag", "-d", tagNameInput)
				testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "tag", tagNameInput)

				deleteResponse, err := client.UserDeleteTag(ctx, deleteRequest)
				require.NoError(t, err)
				deleteResponseOk := &gitalypb.UserDeleteTagResponse{
					PreReceiveError: testCase.output,
				}
				require.Equal(t, deleteResponseOk, deleteResponse)
			})
		}
	}
}
