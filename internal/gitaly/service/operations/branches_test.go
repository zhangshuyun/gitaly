package operations

import (
	"context"
	"fmt"
	"net"
	"os/exec"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v13/internal/command"
	"gitlab.com/gitlab-org/gitaly/v13/internal/git/log"
	"gitlab.com/gitlab-org/gitaly/v13/internal/gitaly/config"
	gitalyhook "gitlab.com/gitlab-org/gitaly/v13/internal/gitaly/hook"
	"gitlab.com/gitlab-org/gitaly/v13/internal/gitaly/service/hook"
	"gitlab.com/gitlab-org/gitaly/v13/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v13/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/v13/internal/praefect/metadata"
	"gitlab.com/gitlab-org/gitaly/v13/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v13/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
)

type testTransactionServer struct {
	called int
}

func (s *testTransactionServer) VoteTransaction(ctx context.Context, in *gitalypb.VoteTransactionRequest) (*gitalypb.VoteTransactionResponse, error) {
	s.called++
	return &gitalypb.VoteTransactionResponse{
		State: gitalypb.VoteTransactionResponse_COMMIT,
	}, nil
}

func TestSuccessfulCreateBranchRequest(t *testing.T) {
	testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	serverSocketPath, stop := runOperationServiceServer(t)
	defer stop()

	client, conn := newOperationClient(t, serverSocketPath)
	defer conn.Close()

	ctx, cancel := testhelper.Context()
	defer cancel()

	startPoint := "c7fbe50c7c7419d9701eebe64b1fdacc3df5b9dd"
	startPointCommit, err := log.GetCommit(ctx, testRepo, startPoint)
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
				defer exec.Command(command.GitPath(), "-C", testRepoPath, "branch", "-D", branchName).Run()
			}

			require.NoError(t, err)
			require.Equal(t, testCase.expectedBranch, response.Branch)
			require.Empty(t, response.PreReceiveError)

			branches := testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "branch")
			require.Contains(t, string(branches), branchName)
		})
	}
}

func TestSuccessfulGitHooksForUserCreateBranchRequest(t *testing.T) {
	featureSets, err := testhelper.NewFeatureSets([]featureflag.FeatureFlag{featureflag.ReferenceTransactions})
	require.NoError(t, err)

	for _, featureSet := range featureSets {
		ctx, cancel := testhelper.Context()
		defer cancel()

		ctx = featureSet.Disable(ctx)

		testSuccessfulGitHooksForUserCreateBranchRequest(t, ctx)
	}
}

func TestUserCreateBranchWithTransaction(t *testing.T) {
	t.Run("with reftx hook", func(t *testing.T) {
		testUserCreateBranchWithTransaction(t, true)
	})
	t.Run("without reftx hook ", func(t *testing.T) {
		testUserCreateBranchWithTransaction(t, false)
	})
}

func testUserCreateBranchWithTransaction(t *testing.T, withRefTxHook bool) {
	testRepo, testRepoPath, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()

	internalSocket := config.GitalyInternalSocketPath()
	internalListener, err := net.Listen("unix", internalSocket)
	require.NoError(t, err)

	tcpSocket, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	transactionServer := &testTransactionServer{}
	srv := testhelper.NewServerWithAuth(t, nil, nil, config.Config.Auth.Token)
	gitalypb.RegisterOperationServiceServer(srv.GrpcServer(), &server{ruby: RubyServer})
	gitalypb.RegisterHookServiceServer(srv.GrpcServer(), hook.NewServer(gitalyhook.NewManager(gitalyhook.GitlabAPIStub, config.Config)))
	gitalypb.RegisterRefTransactionServer(srv.GrpcServer(), transactionServer)

	require.NoError(t, srv.Start())
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
			defer exec.Command(command.GitPath(), "-C", testRepoPath, "branch", "-D", "new-branch").Run()

			client, conn := newOperationClient(t, tc.address)
			defer conn.Close()

			ctx, cancel := testhelper.Context()
			defer cancel()
			ctx, err = tc.server.Inject(ctx)
			require.NoError(t, err)
			ctx, err = metadata.InjectTransaction(ctx, 1, "node", true)
			require.NoError(t, err)
			ctx = helper.IncomingToOutgoing(ctx)

			ctx = featureflag.OutgoingCtxWithFeatureFlagValue(
				ctx,
				featureflag.RubyReferenceTransactionHook,
				strconv.FormatBool(withRefTxHook),
			)

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

			if withRefTxHook {
				require.Equal(t, 1, transactionServer.called)
			} else {
				require.Equal(t, 0, transactionServer.called)
			}
		})
	}
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
			defer exec.Command(command.GitPath(), "-C", testRepoPath, "branch", "-D", branchName).Run()

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
		remove, err := testhelper.WriteCustomHook(testRepoPath, hookName, hookContent)
		require.NoError(t, err)
		defer remove()

		ctx, cancel := testhelper.Context()
		defer cancel()

		response, err := client.UserCreateBranch(ctx, request)
		require.Nil(t, err)
		require.Contains(t, response.PreReceiveError, "GL_USERNAME="+testhelper.TestUser.GlUsername)
	}
}

func TestFailedUserCreateBranchRequest(t *testing.T) {
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
		code       codes.Code
	}{
		{
			desc:       "empty start_point",
			branchName: "shiny-new-branch",
			startPoint: "",
			user:       testhelper.TestUser,
			code:       codes.InvalidArgument,
		},
		{
			desc:       "empty user",
			branchName: "shiny-new-branch",
			startPoint: "master",
			user:       nil,
			code:       codes.InvalidArgument,
		},
		{
			desc:       "non-existing starting point",
			branchName: "new-branch",
			startPoint: "i-dont-exist",
			user:       testhelper.TestUser,
			code:       codes.FailedPrecondition,
		},

		{
			desc:       "branch exists",
			branchName: "master",
			startPoint: "master",
			user:       testhelper.TestUser,
			code:       codes.FailedPrecondition,
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

			ctx, cancel := testhelper.Context()
			defer cancel()

			_, err := client.UserCreateBranch(ctx, request)
			testhelper.RequireGrpcError(t, err, testCase.code)
		})
	}
}

func TestSuccessfulUserDeleteBranchRequest(t *testing.T) {
	featureSets, err := testhelper.NewFeatureSets([]featureflag.FeatureFlag{
		featureflag.ReferenceTransactions,
		featureflag.GoUserDeleteBranch,
	})
	require.NoError(t, err)

	for _, featureSet := range featureSets {
		t.Run("disabled "+featureSet.String(), func(t *testing.T) {
			ctx, cancel := testhelper.Context()
			defer cancel()

			ctx = featureSet.Disable(ctx)

			testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepo(t)
			defer cleanupFn()

			serverSocketPath, stop := runOperationServiceServer(t)
			defer stop()

			client, conn := newOperationClient(t, serverSocketPath)
			defer conn.Close()

			branchNameInput := "to-be-deleted-soon-branch"

			defer exec.Command(command.GitPath(), "-C", testRepoPath, "branch", "-d", branchNameInput).Run()

			testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "branch", branchNameInput)

			request := &gitalypb.UserDeleteBranchRequest{
				Repository: testRepo,
				BranchName: []byte(branchNameInput),
				User:       testhelper.TestUser,
			}

			_, err := client.UserDeleteBranch(ctx, request)
			require.NoError(t, err)

			branches := testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "branch")
			require.NotContains(t, string(branches), branchNameInput, "branch name still exists in branches list")
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
	defer exec.Command(command.GitPath(), "-C", testRepoPath, "branch", "-d", branchNameInput).Run()

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
	serverSocketPath, stop := runOperationServiceServer(t)
	defer stop()

	client, conn := newOperationClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	featureSets, err := testhelper.NewFeatureSets([]featureflag.FeatureFlag{
		featureflag.GoUserDeleteBranch,
	})
	require.NoError(t, err)

	testCases := []struct {
		desc    string
		request *gitalypb.UserDeleteBranchRequest
		code    codes.Code
	}{
		{
			desc: "empty user",
			request: &gitalypb.UserDeleteBranchRequest{
				Repository: testRepo,
				BranchName: []byte("does-matter-the-name-if-user-is-empty"),
			},
			code: codes.InvalidArgument,
		},
		{
			desc: "empty branch name",
			request: &gitalypb.UserDeleteBranchRequest{
				Repository: testRepo,
				User:       testhelper.TestUser,
			},
			code: codes.InvalidArgument,
		},
		{
			desc: "non-existent branch name",
			request: &gitalypb.UserDeleteBranchRequest{
				Repository: testRepo,
				User:       testhelper.TestUser,
				BranchName: []byte("i-do-not-exist"),
			},
			code: codes.FailedPrecondition,
		},
	}

	for _, featureSet := range featureSets {
		t.Run("disabled "+featureSet.String(), func(t *testing.T) {
			for _, testCase := range testCases {
				t.Run(testCase.desc, func(t *testing.T) {
					ctx, cancel := testhelper.Context()
					defer cancel()

					ctx = featureSet.Disable(ctx)

					_, err := client.UserDeleteBranch(ctx, testCase.request)
					testhelper.RequireGrpcError(t, err, testCase.code)
				})
			}
		})
	}
}

func TestFailedUserDeleteBranchDueToHooks(t *testing.T) {
	testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	serverSocketPath, stop := runOperationServiceServer(t)
	defer stop()

	client, conn := newOperationClient(t, serverSocketPath)
	defer conn.Close()

	branchNameInput := "to-be-deleted-soon-branch"
	testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "branch", branchNameInput)
	defer exec.Command(command.GitPath(), "-C", testRepoPath, "branch", "-d", branchNameInput).Run()

	featureSets, err := testhelper.NewFeatureSets([]featureflag.FeatureFlag{
		featureflag.GoUserDeleteBranch,
	})
	require.NoError(t, err)

	request := &gitalypb.UserDeleteBranchRequest{
		Repository: testRepo,
		BranchName: []byte(branchNameInput),
		User:       testhelper.TestUser,
	}

	hookContent := []byte("#!/bin/sh\necho GL_ID=$GL_ID\nexit 1")

	for _, featureSet := range featureSets {
		t.Run("disabled "+featureSet.String(), func(t *testing.T) {
			for _, hookName := range gitlabPreHooks {
				t.Run(hookName, func(t *testing.T) {
					remove, err := testhelper.WriteCustomHook(testRepoPath, hookName, hookContent)
					require.NoError(t, err)
					defer remove()

					ctx, cancel := testhelper.Context()
					defer cancel()

					ctx = featureSet.Disable(ctx)

					response, err := client.UserDeleteBranch(ctx, request)
					require.NoError(t, err)
					require.Contains(t, response.PreReceiveError, "GL_ID="+testhelper.TestUser.GlId)

					branches := testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "branch")
					require.Contains(t, string(branches), branchNameInput, "branch name does not exist in branches list")
				})
			}
		})
	}
}
