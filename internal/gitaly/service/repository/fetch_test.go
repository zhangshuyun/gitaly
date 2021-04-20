package repository

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
)

func TestFetchSourceBranchSourceRepositorySuccess(t *testing.T) {
	cfg, sourceRepo, sourcePath, client := setupRepositoryService(t)

	ctx, cancel := testhelper.Context()
	defer cancel()

	md := testhelper.GitalyServersMetadataFromCfg(t, cfg)
	ctx = testhelper.MergeOutgoingMetadata(ctx, md)

	targetRepoProto, _, cleanup := gittest.CloneRepoAtStorage(t, cfg.Storages[0], "fetch-source-target.git")
	defer cleanup()
	targetRepo := localrepo.New(git.NewExecCommandFactory(cfg), targetRepoProto, cfg)

	sourceBranch := "fetch-source-branch-test-branch"
	newCommitID := gittest.CreateCommit(t, cfg, sourcePath, sourceBranch, nil)

	targetRef := "refs/tmp/fetch-source-branch-test"
	req := &gitalypb.FetchSourceBranchRequest{
		Repository:       targetRepoProto,
		SourceRepository: sourceRepo,
		SourceBranch:     []byte(sourceBranch),
		TargetRef:        []byte(targetRef),
	}

	resp, err := client.FetchSourceBranch(ctx, req)
	require.NoError(t, err)
	require.True(t, resp.Result, "response.Result should be true")

	fetchedCommit, err := targetRepo.ReadCommit(ctx, git.Revision(targetRef))
	require.NoError(t, err)
	require.Equal(t, newCommitID, fetchedCommit.GetId())
}

func TestFetchSourceBranchSameRepositorySuccess(t *testing.T) {
	cfg, repoProto, repoPath, client := setupRepositoryService(t)

	ctx, cancel := testhelper.Context()
	defer cancel()

	md := testhelper.GitalyServersMetadataFromCfg(t, cfg)
	ctx = testhelper.MergeOutgoingMetadata(ctx, md)

	repo := localrepo.New(git.NewExecCommandFactory(cfg), repoProto, cfg)

	sourceBranch := "fetch-source-branch-test-branch"
	newCommitID := gittest.CreateCommit(t, cfg, repoPath, sourceBranch, nil)

	targetRef := "refs/tmp/fetch-source-branch-test"
	req := &gitalypb.FetchSourceBranchRequest{
		Repository:       repoProto,
		SourceRepository: repoProto,
		SourceBranch:     []byte(sourceBranch),
		TargetRef:        []byte(targetRef),
	}

	resp, err := client.FetchSourceBranch(ctx, req)
	require.NoError(t, err)
	require.True(t, resp.Result, "response.Result should be true")

	fetchedCommit, err := repo.ReadCommit(ctx, git.Revision(targetRef))
	require.NoError(t, err)
	require.Equal(t, newCommitID, fetchedCommit.GetId())
}

func TestFetchSourceBranchBranchNotFound(t *testing.T) {
	cfg, targetRepo, _, client := setupRepositoryService(t)

	ctx, cancel := testhelper.Context()
	defer cancel()

	md := testhelper.GitalyServersMetadataFromCfg(t, cfg)
	ctx = testhelper.MergeOutgoingMetadata(ctx, md)

	sourceRepo, _, cleanup := gittest.CloneRepoAtStorage(t, cfg.Storages[0], "fetch-source-source.git")
	t.Cleanup(cleanup)

	sourceBranch := "does-not-exist"
	targetRef := "refs/tmp/fetch-source-branch-test"

	testCases := []struct {
		req  *gitalypb.FetchSourceBranchRequest
		desc string
	}{
		{
			desc: "target different from source",
			req: &gitalypb.FetchSourceBranchRequest{
				Repository:       targetRepo,
				SourceRepository: sourceRepo,
				SourceBranch:     []byte(sourceBranch),
				TargetRef:        []byte(targetRef),
			},
		},
		{
			desc: "target same as source",
			req: &gitalypb.FetchSourceBranchRequest{
				Repository:       sourceRepo,
				SourceRepository: sourceRepo,
				SourceBranch:     []byte(sourceBranch),
				TargetRef:        []byte(targetRef),
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			resp, err := client.FetchSourceBranch(ctx, tc.req)
			require.NoError(t, err)
			require.False(t, resp.Result, "response.Result should be false")
		})
	}
}

func TestFetchSourceBranchWrongRef(t *testing.T) {
	cfg, targetRepo, _, client := setupRepositoryService(t)

	ctx, cancel := testhelper.Context()
	defer cancel()

	md := testhelper.GitalyServersMetadataFromCfg(t, cfg)
	ctx = testhelper.MergeOutgoingMetadata(ctx, md)

	sourceRepo, sourceRepoPath, cleanup := gittest.CloneRepoAtStorage(t, cfg.Storages[0], "fetch-source-source.git")
	defer cleanup()

	sourceBranch := "fetch-source-branch-testmas-branch"
	gittest.CreateCommit(t, cfg, sourceRepoPath, sourceBranch, nil)

	targetRef := "refs/tmp/fetch-source-branch-test"

	testCases := []struct {
		req  *gitalypb.FetchSourceBranchRequest
		desc string
	}{
		{
			desc: "source branch empty",
			req: &gitalypb.FetchSourceBranchRequest{
				Repository:       targetRepo,
				SourceRepository: sourceRepo,
				SourceBranch:     []byte(""),
				TargetRef:        []byte(targetRef),
			},
		},
		{
			desc: "source branch blank",
			req: &gitalypb.FetchSourceBranchRequest{
				Repository:       targetRepo,
				SourceRepository: sourceRepo,
				SourceBranch:     []byte("   "),
				TargetRef:        []byte(targetRef),
			},
		},
		{
			desc: "source branch starts with -",
			req: &gitalypb.FetchSourceBranchRequest{
				Repository:       targetRepo,
				SourceRepository: sourceRepo,
				SourceBranch:     []byte("-ref"),
				TargetRef:        []byte(targetRef),
			},
		},
		{
			desc: "source branch with :",
			req: &gitalypb.FetchSourceBranchRequest{
				Repository:       targetRepo,
				SourceRepository: sourceRepo,
				SourceBranch:     []byte("some:ref"),
				TargetRef:        []byte(targetRef),
			},
		},
		{
			desc: "source branch with NULL",
			req: &gitalypb.FetchSourceBranchRequest{
				Repository:       targetRepo,
				SourceRepository: sourceRepo,
				SourceBranch:     []byte("some\x00ref"),
				TargetRef:        []byte(targetRef),
			},
		},
		{
			desc: "target branch empty",
			req: &gitalypb.FetchSourceBranchRequest{
				Repository:       targetRepo,
				SourceRepository: sourceRepo,
				SourceBranch:     []byte(sourceBranch),
				TargetRef:        []byte(""),
			},
		},
		{
			desc: "target branch blank",
			req: &gitalypb.FetchSourceBranchRequest{
				Repository:       targetRepo,
				SourceRepository: sourceRepo,
				SourceBranch:     []byte(sourceBranch),
				TargetRef:        []byte("   "),
			},
		},
		{
			desc: "target branch starts with -",
			req: &gitalypb.FetchSourceBranchRequest{
				Repository:       targetRepo,
				SourceRepository: sourceRepo,
				SourceBranch:     []byte(sourceBranch),
				TargetRef:        []byte("-ref"),
			},
		},
		{
			desc: "target branch with :",
			req: &gitalypb.FetchSourceBranchRequest{
				Repository:       targetRepo,
				SourceRepository: sourceRepo,
				SourceBranch:     []byte(sourceBranch),
				TargetRef:        []byte("some:ref"),
			},
		},
		{
			desc: "target branch with NULL",
			req: &gitalypb.FetchSourceBranchRequest{
				Repository:       targetRepo,
				SourceRepository: sourceRepo,
				SourceBranch:     []byte(sourceBranch),
				TargetRef:        []byte("some\x00ref"),
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			_, err := client.FetchSourceBranch(ctx, tc.req)
			testhelper.RequireGrpcError(t, err, codes.InvalidArgument)
		})
	}
}
