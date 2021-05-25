package commit

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
)

func TestSuccessfulCommitsBetween(t *testing.T) {
	_, repo, _, client := setupCommitServiceWithRepo(t, true)

	from := []byte("498214de67004b1da3d820901307bed2a68a8ef6") // branch-merged
	to := []byte("ba3343bc4fa403a8dfbfcab7fc1a8c29ee34bd69")   // spooky-stuff
	fakeHash := []byte("f63f41fe459e62e1228fcef60d7189127aeba95a")
	fakeRef := []byte("non-existing-ref")
	expectedCommits := []*gitalypb.GitCommit{
		testhelper.GitLabTestCommit("b83d6e391c22777fca1ed3012fce84f633d7fed0"),
		testhelper.GitLabTestCommit("4a24d82dbca5c11c61556f3b35ca472b7463187e"),
		testhelper.GitLabTestCommit("e63f41fe459e62e1228fcef60d7189127aeba95a"),
		testhelper.GitLabTestCommit("ba3343bc4fa403a8dfbfcab7fc1a8c29ee34bd69"),
	}
	testCases := []struct {
		description      string
		from             []byte
		to               []byte
		paginationParams *gitalypb.PaginationParameter
		expectedCommits  []*gitalypb.GitCommit
	}{
		{
			description:     "From hash to hash",
			from:            from,
			to:              to,
			expectedCommits: expectedCommits,
		},
		{
			description:     "From hash to ref",
			from:            from,
			to:              []byte("gitaly-test-ref"),
			expectedCommits: expectedCommits[:3],
		},
		{
			description:     "From ref to hash",
			from:            []byte("branch-merged"),
			to:              to,
			expectedCommits: expectedCommits,
		},
		{
			description:     "From ref to ref",
			from:            []byte("branch-merged"),
			to:              []byte("gitaly-test-ref"),
			expectedCommits: expectedCommits[:3],
		},
		{
			description:     "To hash doesn't exist",
			from:            from,
			to:              fakeHash,
			expectedCommits: []*gitalypb.GitCommit{},
		},
		{
			description:     "From hash doesn't exist",
			from:            fakeHash,
			to:              to,
			expectedCommits: []*gitalypb.GitCommit{},
		},
		{
			description:     "To ref doesn't exist",
			from:            from,
			to:              fakeRef,
			expectedCommits: []*gitalypb.GitCommit{},
		},
		{
			description:     "From ref doesn't exist",
			from:            fakeRef,
			to:              to,
			expectedCommits: []*gitalypb.GitCommit{},
		},
		{
			description: "using pagination to limit and offset results",
			from:        from,
			to:          to,
			paginationParams: &gitalypb.PaginationParameter{
				Limit:     3,
				PageToken: "b83d6e391c22777fca1ed3012fce84f633d7fed0",
			},
			expectedCommits: expectedCommits[1:4],
		},
	}
	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			rpcRequest := gitalypb.CommitsBetweenRequest{
				Repository: repo, From: tc.from, To: tc.to, PaginationParams: tc.paginationParams,
			}

			ctx, cancel := testhelper.Context()
			defer cancel()

			c, err := client.CommitsBetween(ctx, &rpcRequest)
			require.NoError(t, err)

			commits := getAllCommits(t, func() (gitCommitsGetter, error) { return c.Recv() })

			require.Len(t, commits, len(tc.expectedCommits))
			for i, commit := range commits {
				testhelper.ProtoEqual(t, tc.expectedCommits[i], commit)
			}
		})
	}
}

func TestFailedCommitsBetweenRequest(t *testing.T) {
	_, repo, _, client := setupCommitServiceWithRepo(t, true)

	invalidRepo := &gitalypb.Repository{StorageName: "fake", RelativePath: "path"}
	from := []byte("498214de67004b1da3d820901307bed2a68a8ef6")
	to := []byte("e63f41fe459e62e1228fcef60d7189127aeba95a")

	testCases := []struct {
		description string
		repository  *gitalypb.Repository
		from        []byte
		to          []byte
		code        codes.Code
	}{
		{
			description: "Invalid repository",
			repository:  invalidRepo,
			from:        from,
			to:          to,
			code:        codes.InvalidArgument,
		},
		{
			description: "Repository is nil",
			repository:  nil,
			from:        from,
			to:          to,
			code:        codes.InvalidArgument,
		},
		{
			description: "From is empty",
			repository:  repo,
			from:        nil,
			to:          to,
			code:        codes.InvalidArgument,
		},
		{
			description: "To is empty",
			repository:  repo,
			from:        from,
			to:          nil,
			code:        codes.InvalidArgument,
		},
		{
			description: "From begins with '-'",
			from:        append([]byte("-"), from...),
			to:          to,
			code:        codes.InvalidArgument,
		},
		{
			description: "To begins with '-'",
			from:        from,
			to:          append([]byte("-"), to...),
			code:        codes.InvalidArgument,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			rpcRequest := gitalypb.CommitsBetweenRequest{
				Repository: tc.repository, From: tc.from, To: tc.to,
			}

			ctx, cancel := testhelper.Context()
			defer cancel()
			c, err := client.CommitsBetween(ctx, &rpcRequest)
			require.NoError(t, err)

			err = drainCommitsBetweenResponse(c)
			testhelper.RequireGrpcError(t, err, tc.code)
		})
	}
}

func drainCommitsBetweenResponse(c gitalypb.CommitService_CommitsBetweenClient) error {
	var err error
	for err == nil {
		_, err = c.Recv()
	}
	return err
}
