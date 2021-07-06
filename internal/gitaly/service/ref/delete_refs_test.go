package ref

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/service"
	hookservice "gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/service/hook"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testserver"
	"gitlab.com/gitlab-org/gitaly/v14/internal/transaction/txinfo"
	"gitlab.com/gitlab-org/gitaly/v14/internal/transaction/voting"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

func TestSuccessfulDeleteRefs(t *testing.T) {
	cfg, client := setupRefServiceWithoutRepo(t)

	testCases := []struct {
		desc    string
		request *gitalypb.DeleteRefsRequest
	}{
		{
			desc: "delete all except refs with certain prefixes",
			request: &gitalypb.DeleteRefsRequest{
				ExceptWithPrefix: [][]byte{[]byte("refs/keep"), []byte("refs/also-keep"), []byte("refs/heads/")},
			},
		},
		{
			desc: "delete certain refs",
			request: &gitalypb.DeleteRefsRequest{
				Refs: [][]byte{[]byte("refs/delete/a"), []byte("refs/also-delete/b")},
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.desc, func(t *testing.T) {
			repo, repoPath, cleanupFn := gittest.CloneRepoAtStorage(t, cfg, cfg.Storages[0], testCase.desc)
			defer cleanupFn()

			gittest.Exec(t, cfg, "-C", repoPath, "update-ref", "refs/delete/a", "b83d6e391c22777fca1ed3012fce84f633d7fed0")
			gittest.Exec(t, cfg, "-C", repoPath, "update-ref", "refs/also-delete/b", "1b12f15a11fc6e62177bef08f47bc7b5ce50b141")
			gittest.Exec(t, cfg, "-C", repoPath, "update-ref", "refs/keep/c", "498214de67004b1da3d820901307bed2a68a8ef6")
			gittest.Exec(t, cfg, "-C", repoPath, "update-ref", "refs/also-keep/d", "b83d6e391c22777fca1ed3012fce84f633d7fed0")

			ctx, cancel := testhelper.Context()
			defer cancel()

			testCase.request.Repository = repo
			_, err := client.DeleteRefs(ctx, testCase.request)
			require.NoError(t, err)

			// Ensure that the internal refs are gone, but the others still exist
			refs, err := localrepo.NewTestRepo(t, cfg, repo).GetReferences(ctx, "refs/")
			require.NoError(t, err)

			refNames := make([]string, len(refs))
			for i, branch := range refs {
				refNames[i] = branch.Name.String()
			}

			require.NotContains(t, refNames, "refs/delete/a")
			require.NotContains(t, refNames, "refs/also-delete/b")
			require.Contains(t, refNames, "refs/keep/c")
			require.Contains(t, refNames, "refs/also-keep/d")
			require.Contains(t, refNames, "refs/heads/master")
		})
	}
}

func TestDeleteRefs_transaction(t *testing.T) {
	cfg := testcfg.Build(t)

	testhelper.ConfigureGitalyHooksBin(t, cfg)

	var votes int
	txManager := &transaction.MockManager{
		VoteFn: func(context.Context, txinfo.Transaction, voting.Vote) error {
			votes++
			return nil
		},
	}

	addr := testserver.RunGitalyServer(t, cfg, nil, func(srv grpc.ServiceRegistrar, deps *service.Dependencies) {
		gitalypb.RegisterRefServiceServer(srv, NewServer(
			deps.GetCfg(),
			deps.GetLocator(),
			deps.GetGitCmdFactory(),
			deps.GetTxManager(),
			deps.GetCatfileCache(),
		))
		gitalypb.RegisterHookServiceServer(srv, hookservice.NewServer(deps.GetCfg(), deps.GetHookManager(), deps.GetGitCmdFactory()))
	}, testserver.WithTransactionManager(txManager))

	client, conn := newRefServiceClient(t, addr)
	t.Cleanup(func() { conn.Close() })

	ctx, cancel := testhelper.Context()
	t.Cleanup(cancel)

	ctx, err := txinfo.InjectTransaction(ctx, 1, "node", true)
	require.NoError(t, err)
	ctx = helper.IncomingToOutgoing(ctx)

	for _, tc := range []struct {
		desc          string
		request       *gitalypb.DeleteRefsRequest
		expectedVotes int
	}{
		{
			desc: "delete nothing",
			request: &gitalypb.DeleteRefsRequest{
				ExceptWithPrefix: [][]byte{[]byte("refs/")},
			},
			expectedVotes: 1,
		},
		{
			desc: "delete all refs",
			request: &gitalypb.DeleteRefsRequest{
				ExceptWithPrefix: [][]byte{[]byte("nonexisting/prefix/")},
			},
			expectedVotes: 1,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			votes = 0

			repo, _, cleanup := gittest.CloneRepoAtStorage(t, cfg, cfg.Storages[0], tc.desc)
			t.Cleanup(cleanup)
			tc.request.Repository = repo

			response, err := client.DeleteRefs(ctx, tc.request)
			require.NoError(t, err)
			require.Empty(t, response.GitError)

			require.Equal(t, tc.expectedVotes, votes)
		})
	}
}

func TestFailedDeleteRefsRequestDueToGitError(t *testing.T) {
	_, repo, _, client := setupRefService(t)

	ctx, cancel := testhelper.Context()
	defer cancel()

	request := &gitalypb.DeleteRefsRequest{
		Repository: repo,
		Refs:       [][]byte{[]byte(`refs\tails\invalid-ref-format`)},
	}

	response, err := client.DeleteRefs(ctx, request)
	require.NoError(t, err)

	assert.Contains(t, response.GitError, "unable to delete refs")
}

func TestFailedDeleteRefsDueToValidation(t *testing.T) {
	_, repo, _, client := setupRefService(t)

	testCases := []struct {
		desc    string
		request *gitalypb.DeleteRefsRequest
		// repo     *gitalypb.Repository
		// prefixes [][]byte
		code codes.Code
	}{
		{
			desc: "Invalid repository",
			request: &gitalypb.DeleteRefsRequest{
				Repository:       &gitalypb.Repository{StorageName: "fake", RelativePath: "path"},
				ExceptWithPrefix: [][]byte{[]byte("exclude-this")},
			},
			code: codes.InvalidArgument,
		},
		{
			desc: "Repository is nil",
			request: &gitalypb.DeleteRefsRequest{
				Repository:       nil,
				ExceptWithPrefix: [][]byte{[]byte("exclude-this")},
			},
			code: codes.InvalidArgument,
		},
		{
			desc: "No prefixes nor refs",
			request: &gitalypb.DeleteRefsRequest{
				Repository: repo,
			},
			code: codes.InvalidArgument,
		},
		{
			desc: "prefixes with refs",
			request: &gitalypb.DeleteRefsRequest{
				Repository:       repo,
				ExceptWithPrefix: [][]byte{[]byte("exclude-this")},
				Refs:             [][]byte{[]byte("delete-this")},
			},
			code: codes.InvalidArgument,
		},
		{
			desc: "Empty prefix",
			request: &gitalypb.DeleteRefsRequest{
				Repository:       repo,
				ExceptWithPrefix: [][]byte{[]byte("exclude-this"), []byte{}},
			},
			code: codes.InvalidArgument,
		},
		{
			desc: "Empty ref",
			request: &gitalypb.DeleteRefsRequest{
				Repository: repo,
				Refs:       [][]byte{[]byte("delete-this"), []byte{}},
			},
			code: codes.InvalidArgument,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			ctx, cancel := testhelper.Context()
			defer cancel()

			_, err := client.DeleteRefs(ctx, tc.request)
			testhelper.RequireGrpcError(t, err, tc.code)
		})
	}
}
