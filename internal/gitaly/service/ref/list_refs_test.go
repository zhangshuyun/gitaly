package ref

import (
	"io"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
)

func TestServer_ListRefs(t *testing.T) {
	cfg, _, _, client := setupRefService(t)
	ctx := testhelper.Context(t)

	repo, repoPath := gittest.CreateRepository(ctx, t, cfg)
	// Checking out a worktree in an empty repository is not possible, so we must first write an empty commit.
	gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch(git.DefaultBranch), gittest.WithParents())
	gittest.AddWorktree(t, cfg, repoPath, "worktree")
	repoPath = filepath.Join(repoPath, "worktree")
	// The worktree is detached, checkout the main so the branch pointer advances
	// as we commit.
	gittest.Exec(t, cfg, "-C", repoPath, "checkout", "main")

	oldCommit := text.ChompBytes(gittest.Exec(t, cfg, "-C", repoPath, "rev-parse", "refs/heads/main"))

	gittest.Exec(t, cfg, "-C", repoPath, "commit", "--allow-empty", "-m", "commit message")
	gittest.Exec(t, cfg, "-C", repoPath, "commit", "--amend", "--date", "Wed Feb 16 14:01 2011 +0100", "--allow-empty", "--no-edit")
	commit := text.ChompBytes(gittest.Exec(t, cfg, "-C", repoPath, "rev-parse", "refs/heads/main"))

	for _, cmd := range [][]string{
		{"update-ref", "refs/heads/main", commit},
		{"tag", "lightweight-tag", commit},
		{"tag", "-m", "tag message", "annotated-tag", "refs/heads/main"},
		{"symbolic-ref", "refs/heads/symbolic", "refs/heads/main"},
		{"update-ref", "refs/remote/remote-name/remote-branch", commit},
		{"symbolic-ref", "HEAD", "refs/heads/main"},
		{"update-ref", "refs/heads/old", oldCommit},
	} {
		gittest.Exec(t, cfg, append([]string{"-C", repoPath}, cmd...)...)
	}

	annotatedTagOID := text.ChompBytes(gittest.Exec(t, cfg, "-C", repoPath, "rev-parse", "annotated-tag"))

	for _, tc := range []struct {
		desc              string
		request           *gitalypb.ListRefsRequest
		expectedGrpcError codes.Code
		expectedError     string
		expected          []*gitalypb.ListRefsResponse_Reference
	}{
		{
			desc: "no repo",
			request: &gitalypb.ListRefsRequest{
				Patterns: [][]byte{[]byte("refs/")},
			},
			expectedGrpcError: codes.InvalidArgument,
			expectedError:     "", // Ideally we would test the message but it changes when running through praefect
		},
		{
			desc: "no patterns",
			request: &gitalypb.ListRefsRequest{
				Repository: repo,
			},
			expectedGrpcError: codes.InvalidArgument,
			expectedError:     "rpc error: code = InvalidArgument desc = patterns must have at least one entry",
		},
		{
			desc: "bad sorting key",
			request: &gitalypb.ListRefsRequest{
				Repository: repo,
				Patterns:   [][]byte{[]byte("refs/")},
				SortBy: &gitalypb.ListRefsRequest_SortBy{
					Key: gitalypb.ListRefsRequest_SortBy_Key(100),
				},
			},
			expectedGrpcError: codes.InvalidArgument,
			expectedError:     `rpc error: code = InvalidArgument desc = sorting key "100" is not supported`,
		},
		{
			desc: "bad sorting direction",
			request: &gitalypb.ListRefsRequest{
				Repository: repo,
				Patterns:   [][]byte{[]byte("refs/")},
				SortBy: &gitalypb.ListRefsRequest_SortBy{
					Direction: gitalypb.SortDirection(100),
				},
			},
			expectedGrpcError: codes.InvalidArgument,
			expectedError:     "rpc error: code = InvalidArgument desc = sorting direction is not supported",
		},
		{
			desc: "not found",
			request: &gitalypb.ListRefsRequest{
				Repository: repo,
				Patterns:   [][]byte{[]byte("this-pattern-does-not-match-anything")},
			},
		},
		{
			desc: "not found and main",
			request: &gitalypb.ListRefsRequest{
				Repository: repo,
				Patterns: [][]byte{
					[]byte("this-pattern-does-not-match-anything"),
					[]byte("refs/heads/main"),
				},
			},
			expected: []*gitalypb.ListRefsResponse_Reference{
				{Name: []byte("refs/heads/main"), Target: commit},
			},
		},
		{
			desc: "all",
			request: &gitalypb.ListRefsRequest{
				Repository: repo,
				Patterns:   [][]byte{[]byte("refs/")},
			},
			expected: []*gitalypb.ListRefsResponse_Reference{
				{Name: []byte("refs/heads/main"), Target: commit},
				{Name: []byte("refs/heads/old"), Target: oldCommit},
				{Name: []byte("refs/heads/symbolic"), Target: commit},
				{Name: []byte("refs/remote/remote-name/remote-branch"), Target: commit},
				{Name: []byte("refs/tags/annotated-tag"), Target: annotatedTagOID},
				{Name: []byte("refs/tags/lightweight-tag"), Target: commit},
			},
		},
		{
			desc: "sort by authordate desc",
			request: &gitalypb.ListRefsRequest{
				Repository: repo,
				Patterns:   [][]byte{[]byte("refs/heads")},
				SortBy: &gitalypb.ListRefsRequest_SortBy{
					Direction: gitalypb.SortDirection_DESCENDING,
					Key:       gitalypb.ListRefsRequest_SortBy_AUTHORDATE,
				},
			},
			expected: []*gitalypb.ListRefsResponse_Reference{
				{Name: []byte("refs/heads/old"), Target: oldCommit},
				{Name: []byte("refs/heads/main"), Target: commit},
				{Name: []byte("refs/heads/symbolic"), Target: commit},
			},
		},
		{
			desc: "branches and tags only",
			request: &gitalypb.ListRefsRequest{
				Repository: repo,
				Patterns:   [][]byte{[]byte("refs/heads/*"), []byte("refs/tags/*")},
			},
			expected: []*gitalypb.ListRefsResponse_Reference{
				{Name: []byte("refs/heads/main"), Target: commit},
				{Name: []byte("refs/heads/old"), Target: oldCommit},
				{Name: []byte("refs/heads/symbolic"), Target: commit},
				{Name: []byte("refs/tags/annotated-tag"), Target: annotatedTagOID},
				{Name: []byte("refs/tags/lightweight-tag"), Target: commit},
			},
		},
		{
			desc: "head and branches and tags only",
			request: &gitalypb.ListRefsRequest{
				Repository: repo,
				Head:       true,
				Patterns:   [][]byte{[]byte("refs/heads/*"), []byte("refs/tags/*")},
			},
			expected: []*gitalypb.ListRefsResponse_Reference{
				{Name: []byte("HEAD"), Target: commit},
				{Name: []byte("refs/heads/main"), Target: commit},
				{Name: []byte("refs/heads/old"), Target: oldCommit},
				{Name: []byte("refs/heads/symbolic"), Target: commit},
				{Name: []byte("refs/tags/annotated-tag"), Target: annotatedTagOID},
				{Name: []byte("refs/tags/lightweight-tag"), Target: commit},
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			c, err := client.ListRefs(ctx, tc.request)
			require.NoError(t, err)

			var refs []*gitalypb.ListRefsResponse_Reference
			for {
				r, err := c.Recv()
				if err == io.EOF {
					break
				}
				if tc.expectedError == "" && tc.expectedGrpcError == 0 {
					require.NoError(t, err)
				} else {
					if tc.expectedError != "" {
						require.EqualError(t, err, tc.expectedError)
					}

					if tc.expectedGrpcError != 0 {
						testhelper.RequireGrpcCode(t, err, tc.expectedGrpcError)
					}

					return
				}

				refs = append(refs, r.GetReferences()...)
			}

			testhelper.ProtoEqual(t, tc.expected, refs)
		})
	}
}
