package ref

import (
	"io"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testassert"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
)

func TestServer_ListRefs(t *testing.T) {
	cfg, _, _, client := setupRefService(t)

	ctx, cancel := testhelper.Context()
	defer cancel()

	storagePath, ok := cfg.StoragePath("default")
	require.True(t, ok)

	const relativePath = "repository-1"
	repoPath := filepath.Join(storagePath, relativePath)

	gittest.Exec(t, cfg, "init", repoPath)
	gittest.Exec(t, cfg, "-C", repoPath, "commit", "--allow-empty", "-m", "commit message")
	commit := text.ChompBytes(gittest.Exec(t, cfg, "-C", repoPath, "rev-parse", "refs/heads/master"))

	for _, cmd := range [][]string{
		{"update-ref", "refs/heads/master", commit},
		{"tag", "lightweight-tag", commit},
		{"tag", "-m", "tag message", "annotated-tag", "refs/heads/master"},
		{"symbolic-ref", "refs/heads/symbolic", "refs/heads/master"},
		{"update-ref", "refs/remote/remote-name/remote-branch", commit},
		{"symbolic-ref", "HEAD", "refs/heads/master"},
	} {
		gittest.Exec(t, cfg, append([]string{"-C", repoPath}, cmd...)...)
	}

	annotatedTagOID := text.ChompBytes(gittest.Exec(t, cfg, "-C", repoPath, "rev-parse", "annotated-tag"))

	repo := &gitalypb.Repository{StorageName: "default", RelativePath: filepath.Join(relativePath, ".git")}

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
			desc: "not found",
			request: &gitalypb.ListRefsRequest{
				Repository: repo,
				Patterns:   [][]byte{[]byte("this-pattern-does-not-match-anything")},
			},
		},
		{
			desc: "not found and master",
			request: &gitalypb.ListRefsRequest{
				Repository: repo,
				Patterns: [][]byte{
					[]byte("this-pattern-does-not-match-anything"),
					[]byte("refs/heads/master"),
				},
			},
			expected: []*gitalypb.ListRefsResponse_Reference{
				{Name: []byte("refs/heads/master"), Target: commit},
			},
		},
		{
			desc: "all",
			request: &gitalypb.ListRefsRequest{
				Repository: repo,
				Patterns:   [][]byte{[]byte("refs/")},
			},
			expected: []*gitalypb.ListRefsResponse_Reference{
				{Name: []byte("refs/heads/master"), Target: commit},
				{Name: []byte("refs/heads/symbolic"), Target: commit},
				{Name: []byte("refs/remote/remote-name/remote-branch"), Target: commit},
				{Name: []byte("refs/tags/annotated-tag"), Target: annotatedTagOID},
				{Name: []byte("refs/tags/lightweight-tag"), Target: commit},
			},
		},
		{
			desc: "branches and tags only",
			request: &gitalypb.ListRefsRequest{
				Repository: repo,
				Patterns:   [][]byte{[]byte("refs/heads/*"), []byte("refs/tags/*")},
			},
			expected: []*gitalypb.ListRefsResponse_Reference{
				{Name: []byte("refs/heads/master"), Target: commit},
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
				{Name: []byte("refs/heads/master"), Target: commit},
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
						testhelper.RequireGrpcError(t, err, tc.expectedGrpcError)
					}

					return
				}

				refs = append(refs, r.GetReferences()...)
			}

			testassert.ProtoEqual(t, tc.expected, refs)
		})
	}
}
