package operations

import (
	"context"
	"fmt"
	"net"
	"os/exec"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/client"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/log"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	gitalyhook "gitlab.com/gitlab-org/gitaly/internal/gitaly/hook"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/service/hook"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/metadata"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type testTransactionServer struct {
	gitalypb.UnimplementedRefTransactionServer
	called int
}

func (s *testTransactionServer) VoteTransaction(ctx context.Context, in *gitalypb.VoteTransactionRequest) (*gitalypb.VoteTransactionResponse, error) {
	s.called++
	return &gitalypb.VoteTransactionResponse{
		State: gitalypb.VoteTransactionResponse_COMMIT,
	}, nil
}

func TestSuccessfulCreateBranchRequest(t *testing.T) {
	testWithFeature(t, featureflag.GoUserCreateBranch, testSuccessfulCreateBranchRequest)
}

func testSuccessfulCreateBranchRequest(t *testing.T, ctx context.Context) {
	testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	serverSocketPath, stop := runOperationServiceServer(t)
	defer stop()

	client, conn := newOperationClient(t, serverSocketPath)
	defer conn.Close()

	locator := config.NewLocator(config.Config)
	startPoint := "c7fbe50c7c7419d9701eebe64b1fdacc3df5b9dd"
	startPointCommit, err := log.GetCommit(ctx, locator, testRepo, git.Revision(startPoint))
	require.NoError(t, err)

	testCases := []struct {
		desc           string
		branchName     string
		startPoint     string
		expectedBranch *gitalypb.Branch
	}{
		{
			desc:       "valid branch",
			branchName: "new-branch",
			startPoint: startPoint,
			expectedBranch: &gitalypb.Branch{
				Name:         []byte("new-branch"),
				TargetCommit: startPointCommit,
			},
		},
		// On input like heads/foo and refs/heads/foo we don't
		// DWYM and map it to refs/heads/foo and
		// refs/heads/foo, respectively. Instead we always
		// prepend refs/heads/*, so you get
		// refs/heads/heads/foo and refs/heads/refs/heads/foo
		{
			desc:       "valid branch",
			branchName: "heads/new-branch",
			startPoint: startPoint,
			expectedBranch: &gitalypb.Branch{
				Name:         []byte("heads/new-branch"),
				TargetCommit: startPointCommit,
			},
		},
		{
			desc:       "valid branch",
			branchName: "refs/heads/new-branch",
			startPoint: startPoint,
			expectedBranch: &gitalypb.Branch{
				Name:         []byte("refs/heads/new-branch"),
				TargetCommit: startPointCommit,
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			branchName := testCase.branchName
			request := &gitalypb.UserCreateBranchRequest{
				Repository: testRepo,
				BranchName: []byte(branchName),
				StartPoint: []byte(testCase.startPoint),
				User:       testhelper.TestUser,
			}

			ctx, cancel := testhelper.Context()
			defer cancel()

			response, err := client.UserCreateBranch(ctx, request)
			if testCase.expectedBranch != nil {
				defer exec.Command(config.Config.Git.BinPath, "-C", testRepoPath, "branch", "-D", branchName).Run()
			}

			require.NoError(t, err)
			require.Equal(t, testCase.expectedBranch, response.Branch)
			require.Empty(t, response.PreReceiveError)

			branches := testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "for-each-ref", "--", "refs/heads/"+branchName)
			require.Contains(t, string(branches), "refs/heads/"+branchName)
		})
	}
}

func TestUserCreateBranchWithTransaction(t *testing.T) {
	testRepo, testRepoPath, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()

	internalSocket := config.Config.GitalyInternalSocketPath()
	internalListener, err := net.Listen("unix", internalSocket)
	require.NoError(t, err)

	tcpSocket, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	transactionServer := &testTransactionServer{}
	srv := testhelper.NewServerWithAuth(t, nil, nil, config.Config.Auth.Token)
	locator := config.NewLocator(config.Config)
	hookManager := gitalyhook.NewManager(locator, gitalyhook.GitlabAPIStub, config.Config)

	conns := client.NewPool()
	defer conns.Close()

	server := NewServer(config.Config, RubyServer, hookManager, locator, conns)

	gitalypb.RegisterOperationServiceServer(srv.GrpcServer(), server)
	gitalypb.RegisterHookServiceServer(srv.GrpcServer(), hook.NewServer(config.Config, hookManager))
	gitalypb.RegisterRefTransactionServer(srv.GrpcServer(), transactionServer)

	srv.Start(t)
	defer srv.Stop()

	go srv.GrpcServer().Serve(internalListener)
	go srv.GrpcServer().Serve(tcpSocket)

	testcases := []struct {
		desc    string
		address string
		server  metadata.PraefectServer
	}{
		{
			desc:    "explicit TCP address",
			address: tcpSocket.Addr().String(),
			server: metadata.PraefectServer{
				ListenAddr: fmt.Sprintf("tcp://" + tcpSocket.Addr().String()),
				Token:      config.Config.Auth.Token,
			},
		},
		{
			desc:    "catch-all TCP address",
			address: tcpSocket.Addr().String(),
			server: metadata.PraefectServer{
				ListenAddr: fmt.Sprintf("tcp://0.0.0.0:%d", tcpSocket.Addr().(*net.TCPAddr).Port),
				Token:      config.Config.Auth.Token,
			},
		},
		{
			desc:    "Unix socket",
			address: "unix://" + internalSocket,
			server: metadata.PraefectServer{
				SocketPath: "unix://" + internalSocket,
				Token:      config.Config.Auth.Token,
			},
		},
	}

	for _, tc := range testcases {
		t.Run(tc.desc, func(t *testing.T) {
			defer exec.Command(config.Config.Git.BinPath, "-C", testRepoPath, "branch", "-D", "new-branch").Run()

			client, conn := newOperationClient(t, tc.address)
			defer conn.Close()

			ctx, cancel := testhelper.Context()
			defer cancel()
			ctx, err = tc.server.Inject(ctx)
			require.NoError(t, err)
			ctx, err = metadata.InjectTransaction(ctx, 1, "node", true)
			require.NoError(t, err)
			ctx = helper.IncomingToOutgoing(ctx)

			request := &gitalypb.UserCreateBranchRequest{
				Repository: testRepo,
				BranchName: []byte("new-branch"),
				StartPoint: []byte("c7fbe50c7c7419d9701eebe64b1fdacc3df5b9dd"),
				User:       testhelper.TestUser,
			}

			transactionServer.called = 0
			response, err := client.UserCreateBranch(ctx, request)
			require.NoError(t, err)
			require.Empty(t, response.PreReceiveError)
			require.Equal(t, 2, transactionServer.called)
		})
	}
}

func TestSuccessfulGitHooksForUserCreateBranchRequest(t *testing.T) {
	testhelper.NewFeatureSets([]featureflag.FeatureFlag{
		featureflag.ReferenceTransactions,
		featureflag.GoUserCreateBranch,
	}).Run(t, testSuccessfulGitHooksForUserCreateBranchRequest)
}

func testSuccessfulGitHooksForUserCreateBranchRequest(t *testing.T, ctx context.Context) {
	testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	serverSocketPath, stop := runOperationServiceServer(t)
	defer stop()

	client, conn := newOperationClient(t, serverSocketPath)
	defer conn.Close()

	branchName := "new-branch"
	request := &gitalypb.UserCreateBranchRequest{
		Repository: testRepo,
		BranchName: []byte(branchName),
		StartPoint: []byte("c7fbe50c7c7419d9701eebe64b1fdacc3df5b9dd"),
		User:       testhelper.TestUser,
	}

	for _, hookName := range GitlabHooks {
		t.Run(hookName, func(t *testing.T) {
			defer exec.Command(config.Config.Git.BinPath, "-C", testRepoPath, "branch", "-D", branchName).Run()

			hookOutputTempPath, cleanup := testhelper.WriteEnvToCustomHook(t, testRepoPath, hookName)
			defer cleanup()

			response, err := client.UserCreateBranch(ctx, request)
			require.NoError(t, err)
			require.Empty(t, response.PreReceiveError)

			output := string(testhelper.MustReadFile(t, hookOutputTempPath))
			require.Contains(t, output, "GL_USERNAME="+testhelper.TestUser.GlUsername)
		})
	}
}

func TestFailedUserCreateBranchDueToHooks(t *testing.T) {
	testWithFeature(t, featureflag.GoUserCreateBranch, testFailedUserCreateBranchDueToHooks)
}

func TestSuccessfulCreateBranchRequestWithStartPointRefPrefix(t *testing.T) {
	testWithFeature(t, featureflag.GoUserCreateBranch, testSuccessfulCreateBranchRequestWithStartPointRefPrefix)
}

func testSuccessfulCreateBranchRequestWithStartPointRefPrefix(t *testing.T, ctx context.Context) {
	serverSocketPath, stop := runOperationServiceServer(t)
	defer stop()

	client, conn := newOperationClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	testCases := []struct {
		desc             string
		branchName       string
		startPoint       string
		startPointCommit string
		user             *gitalypb.User
	}{
		// Similar to prefixing branchName in
		// TestSuccessfulCreateBranchRequest() above:
		// Unfortunately (and inconsistently), the StartPoint
		// reference does have DWYM semantics. See
		// https://gitlab.com/gitlab-org/gitaly/-/issues/3331
		{
			desc:             "the StartPoint parameter does DWYM references (boo!)",
			branchName:       "topic",
			startPoint:       "heads/master",
			startPointCommit: "9a944d90955aaf45f6d0c88f30e27f8d2c41cec0", // TODO: see below
			user:             testhelper.TestUser,
		},
		{
			desc:             "the StartPoint parameter does DWYM references (boo!) 2",
			branchName:       "topic2",
			startPoint:       "refs/heads/master",
			startPointCommit: "c642fe9b8b9f28f9225d7ea953fe14e74748d53b", // TODO: see below
			user:             testhelper.TestUser,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "update-ref", "refs/heads/"+testCase.startPoint,
				testCase.startPointCommit,
				"0000000000000000000000000000000000000000",
			)
			request := &gitalypb.UserCreateBranchRequest{
				Repository: testRepo,
				BranchName: []byte(testCase.branchName),
				StartPoint: []byte(testCase.startPoint),
				User:       testCase.user,
			}

			// BEGIN TODO: Uncomment if StartPoint started behaving sensibly
			// like BranchName. See
			// https://gitlab.com/gitlab-org/gitaly/-/issues/3331
			//
			//targetCommitOK, err := log.GetCommit(ctx, testRepo, testCase.startPointCommit)
			// END TODO
			locator := config.NewLocator(config.Config)
			targetCommitOK, err := log.GetCommit(ctx, locator, testRepo, "1e292f8fedd741b75372e19097c76d327140c312")
			require.NoError(t, err)

			response, err := client.UserCreateBranch(ctx, request)
			require.NoError(t, err)
			responseOk := &gitalypb.UserCreateBranchResponse{
				Branch: &gitalypb.Branch{
					Name:         []byte(testCase.branchName),
					TargetCommit: targetCommitOK,
				},
			}
			require.Equal(t, responseOk, response)
			branches := testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "for-each-ref", "--", "refs/heads/"+testCase.branchName)
			require.Contains(t, string(branches), "refs/heads/"+testCase.branchName)
		})
	}
}

func testFailedUserCreateBranchDueToHooks(t *testing.T, ctx context.Context) {
	testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	serverSocketPath, stop := runOperationServiceServer(t)
	defer stop()

	client, conn := newOperationClient(t, serverSocketPath)
	defer conn.Close()

	request := &gitalypb.UserCreateBranchRequest{
		Repository: testRepo,
		BranchName: []byte("new-branch"),
		StartPoint: []byte("c7fbe50c7c7419d9701eebe64b1fdacc3df5b9dd"),
		User:       testhelper.TestUser,
	}
	// Write a hook that will fail with the environment as the error message
	// so we can check that string for our env variables.
	hookContent := []byte("#!/bin/sh\nprintenv | paste -sd ' ' -\nexit 1")

	for _, hookName := range gitlabPreHooks {
		remove := testhelper.WriteCustomHook(t, testRepoPath, hookName, hookContent)
		defer remove()

		response, err := client.UserCreateBranch(ctx, request)
		require.Nil(t, err)
		require.Contains(t, response.PreReceiveError, "GL_USERNAME="+testhelper.TestUser.GlUsername)
	}
}

func TestFailedUserCreateBranchRequest(t *testing.T) {
	testWithFeature(t, featureflag.GoUserCreateBranch, testFailedUserCreateBranchRequest)
}

func testFailedUserCreateBranchRequest(t *testing.T, ctx context.Context) {
	serverSocketPath, stop := runOperationServiceServer(t)
	defer stop()

	client, conn := newOperationClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	testCases := []struct {
		desc       string
		branchName string
		startPoint string
		user       *gitalypb.User
		err        error
	}{
		{
			desc:       "empty start_point",
			branchName: "shiny-new-branch",
			startPoint: "",
			user:       testhelper.TestUser,
			err:        status.Error(codes.InvalidArgument, "empty start point"),
		},
		{
			desc:       "empty user",
			branchName: "shiny-new-branch",
			startPoint: "master",
			user:       nil,
			err:        status.Error(codes.InvalidArgument, "empty user"),
		},
		{
			desc:       "non-existing starting point",
			branchName: "new-branch",
			startPoint: "i-dont-exist",
			user:       testhelper.TestUser,
			err:        status.Errorf(codes.FailedPrecondition, "revspec '%s' not found", "i-dont-exist"),
		},

		{
			desc:       "branch exists",
			branchName: "master",
			startPoint: "master",
			user:       testhelper.TestUser,
			err:        status.Errorf(codes.FailedPrecondition, "Could not update %s. Please refresh and try again.", "master"),
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			request := &gitalypb.UserCreateBranchRequest{
				Repository: testRepo,
				BranchName: []byte(testCase.branchName),
				StartPoint: []byte(testCase.startPoint),
				User:       testCase.user,
			}

			response, err := client.UserCreateBranch(ctx, request)
			require.Equal(t, testCase.err, err)
			require.Empty(t, response)
		})
	}
}

func TestSuccessfulUserDeleteBranchRequest(t *testing.T) {
	testhelper.NewFeatureSets([]featureflag.FeatureFlag{
		featureflag.ReferenceTransactions,
	}).Run(t, testSuccessfulUserDeleteBranchRequest)
}

func testSuccessfulUserDeleteBranchRequest(t *testing.T, ctx context.Context) {
	testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	serverSocketPath, stop := runOperationServiceServer(t)
	defer stop()

	client, conn := newOperationClient(t, serverSocketPath)
	defer conn.Close()

	testCases := []struct {
		desc            string
		branchNameInput string
		branchCommit    string
		user            *gitalypb.User
		response        *gitalypb.UserDeleteBranchResponse
		err             error
	}{
		{
			desc:            "simple successful deletion",
			branchNameInput: "to-attempt-to-delete-soon-branch",
			branchCommit:    "c7fbe50c7c7419d9701eebe64b1fdacc3df5b9dd",
			user:            testhelper.TestUser,
			response:        &gitalypb.UserDeleteBranchResponse{},
			err:             nil,
		},
		{
			desc:            "partially prefixed successful deletion",
			branchNameInput: "heads/to-attempt-to-delete-soon-branch",
			branchCommit:    "9a944d90955aaf45f6d0c88f30e27f8d2c41cec0",
			user:            testhelper.TestUser,
			response:        &gitalypb.UserDeleteBranchResponse{},
			err:             nil,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "branch", testCase.branchNameInput, testCase.branchCommit)
			defer exec.Command(config.Config.Git.BinPath, "-C", testRepoPath, "branch", "-d", testCase.branchNameInput).Run()

			request := &gitalypb.UserDeleteBranchRequest{
				Repository: testRepo,
				BranchName: []byte(testCase.branchNameInput),
				User:       testCase.user,
			}

			response, err := client.UserDeleteBranch(ctx, request)
			require.Equal(t, testCase.err, err)
			testhelper.ProtoEqual(t, testCase.response, response)

			refs := testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "for-each-ref", "--", "refs/heads/"+testCase.branchNameInput)
			require.NotContains(t, string(refs), testCase.branchCommit, "branch deleted from refs")
		})
	}
}

func TestSuccessfulGitHooksForUserDeleteBranchRequest(t *testing.T) {
	testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	serverSocketPath, stop := runOperationServiceServer(t)
	defer stop()

	client, conn := newOperationClient(t, serverSocketPath)
	defer conn.Close()

	branchNameInput := "to-be-deleted-soon-branch"
	defer exec.Command(config.Config.Git.BinPath, "-C", testRepoPath, "branch", "-d", branchNameInput).Run()

	request := &gitalypb.UserDeleteBranchRequest{
		Repository: testRepo,
		BranchName: []byte(branchNameInput),
		User:       testhelper.TestUser,
	}

	for _, hookName := range GitlabHooks {
		t.Run(hookName, func(t *testing.T) {
			testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "branch", branchNameInput)

			hookOutputTempPath, cleanup := testhelper.WriteEnvToCustomHook(t, testRepoPath, hookName)
			defer cleanup()

			ctx, cancel := testhelper.Context()
			defer cancel()

			_, err := client.UserDeleteBranch(ctx, request)
			require.NoError(t, err)

			output := testhelper.MustReadFile(t, hookOutputTempPath)
			require.Contains(t, string(output), "GL_USERNAME="+testhelper.TestUser.GlUsername)
		})
	}
}

func TestFailedUserDeleteBranchDueToValidation(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	serverSocketPath, stop := runOperationServiceServer(t)
	defer stop()

	client, conn := newOperationClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	testCases := []struct {
		desc     string
		request  *gitalypb.UserDeleteBranchRequest
		response *gitalypb.UserDeleteBranchResponse
		err      error
	}{
		{
			desc: "empty user",
			request: &gitalypb.UserDeleteBranchRequest{
				Repository: testRepo,
				BranchName: []byte("does-matter-the-name-if-user-is-empty"),
			},
			response: nil,
			err:      status.Error(codes.InvalidArgument, "Bad Request (empty user)"),
		},
		{
			desc: "empty branch name",
			request: &gitalypb.UserDeleteBranchRequest{
				Repository: testRepo,
				User:       testhelper.TestUser,
			},
			response: nil,
			err:      status.Error(codes.InvalidArgument, "Bad Request (empty branch name)"),
		},
		{
			desc: "non-existent branch name",
			request: &gitalypb.UserDeleteBranchRequest{
				Repository: testRepo,
				User:       testhelper.TestUser,
				BranchName: []byte("i-do-not-exist"),
			},
			response: nil,
			err:      status.Errorf(codes.FailedPrecondition, "branch not found: %s", "i-do-not-exist"),
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			response, err := client.UserDeleteBranch(ctx, testCase.request)
			require.Equal(t, testCase.err, err)
			testhelper.ProtoEqual(t, testCase.response, response)
		})
	}
}

func TestUserDeleteBranchFailedDueToRefsHeadsPrefix(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	serverSocketPath, stop := runOperationServiceServer(t)
	defer stop()

	client, conn := newOperationClient(t, serverSocketPath)
	defer conn.Close()

	testCases := []struct {
		desc            string
		branchNameInput string
		branchCommit    string
		user            *gitalypb.User
		response        *gitalypb.UserDeleteBranchResponse
		err             error
	}{
		{
			// TODO: Make this possible. See
			// https://gitlab.com/gitlab-org/gitaly/-/issues/3343
			desc:            "not possible to delete a branch called refs/heads/something",
			branchNameInput: "refs/heads/can-not-find-this",
			branchCommit:    "c642fe9b8b9f28f9225d7ea953fe14e74748d53b",
			user:            testhelper.TestUser,
			response:        nil,
			err:             status.Errorf(codes.FailedPrecondition, "branch not found: %s", "refs/heads/can-not-find-this"),
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "branch", testCase.branchNameInput, testCase.branchCommit)
			defer exec.Command(config.Config.Git.BinPath, "-C", testRepoPath, "branch", "-d", testCase.branchNameInput).Run()

			request := &gitalypb.UserDeleteBranchRequest{
				Repository: testRepo,
				BranchName: []byte(testCase.branchNameInput),
				User:       testCase.user,
			}

			response, err := client.UserDeleteBranch(ctx, request)
			require.Equal(t, testCase.err, err)
			testhelper.ProtoEqual(t, testCase.response, response)

			refs := testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "for-each-ref", "--", "refs/heads/"+testCase.branchNameInput)
			require.Contains(t, string(refs), testCase.branchCommit, "branch kept because we stripped off refs/heads/*")
		})
	}
}

func TestFailedUserDeleteBranchDueToHooks(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	serverSocketPath, stop := runOperationServiceServer(t)
	defer stop()

	client, conn := newOperationClient(t, serverSocketPath)
	defer conn.Close()

	branchNameInput := "to-be-deleted-soon-branch"
	testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "branch", branchNameInput)
	defer exec.Command(config.Config.Git.BinPath, "-C", testRepoPath, "branch", "-d", branchNameInput).Run()

	request := &gitalypb.UserDeleteBranchRequest{
		Repository: testRepo,
		BranchName: []byte(branchNameInput),
		User:       testhelper.TestUser,
	}

	hookContent := []byte("#!/bin/sh\necho GL_ID=$GL_ID\nexit 1")

	for _, hookName := range gitlabPreHooks {
		t.Run(hookName, func(t *testing.T) {
			remove := testhelper.WriteCustomHook(t, testRepoPath, hookName, hookContent)
			defer remove()

			response, err := client.UserDeleteBranch(ctx, request)
			require.NoError(t, err)
			require.Contains(t, response.PreReceiveError, "GL_ID="+testhelper.TestUser.GlId)

			branches := testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "for-each-ref", "--", "refs/heads/"+branchNameInput)
			require.Contains(t, string(branches), branchNameInput, "branch name does not exist in branches list")
		})
	}
}

func TestBranchHookOutput(t *testing.T) {
	testWithFeature(t, featureflag.GoUserCreateBranch, testBranchHookOutput)
}

func testBranchHookOutput(t *testing.T, ctx context.Context) {
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
				branchNameInput := "some-branch"
				createRequest := &gitalypb.UserCreateBranchRequest{
					Repository: testRepo,
					BranchName: []byte(branchNameInput),
					StartPoint: []byte("master"),
					User:       testhelper.TestUser,
				}
				deleteRequest := &gitalypb.UserDeleteBranchRequest{
					Repository: testRepo,
					BranchName: []byte(branchNameInput),
					User:       testhelper.TestUser,
				}

				remove := testhelper.WriteCustomHook(t, testRepoPath, hookName, []byte(testCase.hookContent))
				defer remove()

				createResponse, err := client.UserCreateBranch(ctx, createRequest)
				require.NoError(t, err)
				require.Equal(t, testCase.output, createResponse.PreReceiveError)

				testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "branch", branchNameInput)
				defer exec.Command(config.Config.Git.BinPath, "-C", testRepoPath, "branch", "-d", branchNameInput).Run()

				deleteResponse, err := client.UserDeleteBranch(ctx, deleteRequest)
				require.NoError(t, err)
				require.Equal(t, testCase.output, deleteResponse.PreReceiveError)
			})
		}
	}
}
