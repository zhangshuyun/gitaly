package commit

import (
	"context"
	"io"
	"testing"

	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
)

func TestSuccessfulCommitsBetween(t *testing.T) {
	stop, serverSocketPath := startTestServices(t)
	defer stop()

	client, conn := newCommitServiceClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	from := []byte("498214de67004b1da3d820901307bed2a68a8ef6") // branch-merged
	to := []byte("ba3343bc4fa403a8dfbfcab7fc1a8c29ee34bd69")   // spooky-stuff
	fakeHash := []byte("f63f41fe459e62e1228fcef60d7189127aeba95a")
	fakeRef := []byte("non-existing-ref")
	expectedCommits := []*gitalypb.GitCommit{
		{
			Id:      "b83d6e391c22777fca1ed3012fce84f633d7fed0",
			Subject: []byte("Merge branch 'branch-merged' into 'master'"),
			Body:    []byte("Merge branch 'branch-merged' into 'master'\r\n\r\nadds bar folder and branch-test text file to check Repository merged_to_root_ref method\r\n\r\n\r\n\r\nSee merge request !12"),
			Author: &gitalypb.CommitAuthor{
				Name:     []byte("Job van der Voort"),
				Email:    []byte("job@gitlab.com"),
				Date:     &timestamp.Timestamp{Seconds: 1474987066},
				Timezone: []byte("+0000"),
			},
			Committer: &gitalypb.CommitAuthor{
				Name:     []byte("Job van der Voort"),
				Email:    []byte("job@gitlab.com"),
				Date:     &timestamp.Timestamp{Seconds: 1474987066},
				Timezone: []byte("+0000"),
			},
			ParentIds: []string{
				"1b12f15a11fc6e62177bef08f47bc7b5ce50b141",
				"498214de67004b1da3d820901307bed2a68a8ef6",
			},
			BodySize: 162,
		},
		{
			Id:      "4a24d82dbca5c11c61556f3b35ca472b7463187e",
			Subject: []byte("Update README.md to include `Usage in testing and development`"),
			Body:    []byte("Update README.md to include `Usage in testing and development`"),
			Author: &gitalypb.CommitAuthor{
				Name:     []byte("Luke \"Jared\" Bennett"),
				Email:    []byte("lbennett@gitlab.com"),
				Date:     &timestamp.Timestamp{Seconds: 1491905339},
				Timezone: []byte("+0000"),
			},
			Committer: &gitalypb.CommitAuthor{
				Name:     []byte("Luke \"Jared\" Bennett"),
				Email:    []byte("lbennett@gitlab.com"),
				Date:     &timestamp.Timestamp{Seconds: 1491905339},
				Timezone: []byte("+0000"),
			},
			ParentIds: []string{"b83d6e391c22777fca1ed3012fce84f633d7fed0"},
			BodySize:  62,
		},
		{
			Id:      "e63f41fe459e62e1228fcef60d7189127aeba95a",
			Subject: []byte("Merge branch 'gitlab-test-usage-dev-testing-docs' into 'master'"),
			Body:    []byte("Merge branch 'gitlab-test-usage-dev-testing-docs' into 'master'\r\n\r\nUpdate README.md to include `Usage in testing and development`\r\n\r\nSee merge request !21"),
			Author: &gitalypb.CommitAuthor{
				Name:     []byte("Sean McGivern"),
				Email:    []byte("sean@mcgivern.me.uk"),
				Date:     &timestamp.Timestamp{Seconds: 1491906794},
				Timezone: []byte("+0000"),
			},
			Committer: &gitalypb.CommitAuthor{
				Name:     []byte("Sean McGivern"),
				Email:    []byte("sean@mcgivern.me.uk"),
				Date:     &timestamp.Timestamp{Seconds: 1491906794},
				Timezone: []byte("+0000"),
			},
			ParentIds: []string{
				"b83d6e391c22777fca1ed3012fce84f633d7fed0",
				"4a24d82dbca5c11c61556f3b35ca472b7463187e",
			},
			BodySize: 154,
		},
		{
			Id:      "ba3343bc4fa403a8dfbfcab7fc1a8c29ee34bd69",
			Subject: []byte("Weird commit date"),
			Body:    []byte("Weird commit date\n"),
			Author: &gitalypb.CommitAuthor{
				Name:  []byte("Alejandro Rodríguez"),
				Email: []byte("alejorro70@gmail.com"),
				// Not the actual commit date, but the biggest we can represent
				Date:     &timestamp.Timestamp{Seconds: 9223371974719179007},
				Timezone: []byte("+0000"),
			},
			Committer: &gitalypb.CommitAuthor{
				Name:     []byte("Alejandro Rodríguez"),
				Email:    []byte("alejorro70@gmail.com"),
				Date:     &timestamp.Timestamp{Seconds: 9223371974719179007},
				Timezone: []byte("+0000"),
			},
			ParentIds: []string{"e63f41fe459e62e1228fcef60d7189127aeba95a"},
			BodySize:  18,
		},
	}
	testCases := []struct {
		description     string
		from            []byte
		to              []byte
		expectedCommits []*gitalypb.GitCommit
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
			expectedCommits: expectedCommits,
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
			expectedCommits: expectedCommits,
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
	}
	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			commits := []*gitalypb.GitCommit{}
			rpcRequest := gitalypb.CommitsBetweenRequest{
				Repository: testRepo, From: tc.from, To: tc.to,
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			c, err := client.CommitsBetween(ctx, &rpcRequest)
			if err != nil {
				t.Fatal(err)
			}

			for {
				resp, err := c.Recv()
				if err == io.EOF {
					break
				} else if err != nil {
					t.Fatal(err)
				}
				commits = append(commits, resp.GetCommits()...)
			}

			for i, commit := range commits {
				require.Equal(t, expectedCommits[i], commit, "mismatched commits")
			}
		})
	}
}

func TestFailedCommitsBetweenRequest(t *testing.T) {
	stop, serverSocketPath := startTestServices(t)
	defer stop()

	client, conn := newCommitServiceClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

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
			repository:  testRepo,
			from:        nil,
			to:          to,
			code:        codes.InvalidArgument,
		},
		{
			description: "To is empty",
			repository:  testRepo,
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

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			c, err := client.CommitsBetween(ctx, &rpcRequest)
			if err != nil {
				t.Fatal(err)
			}

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
