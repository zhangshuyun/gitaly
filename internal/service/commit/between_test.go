package commit

import (
	"io"
	"testing"

	"github.com/golang/protobuf/ptypes/timestamp"

	pb "gitlab.com/gitlab-org/gitaly-proto/go"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
)

func TestSuccessfulCommitsBetween(t *testing.T) {
	server := startTestServices(t)
	defer server.Stop()

	client, conn := newCommitServiceClient(t, serverSocketPath)
	defer conn.Close()
	from := []byte("498214de67004b1da3d820901307bed2a68a8ef6") // branch-merged
	to := []byte("e63f41fe459e62e1228fcef60d7189127aeba95a")   // master
	fakeHash := []byte("f63f41fe459e62e1228fcef60d7189127aeba95a")
	fakeRef := []byte("non-existing-ref")
	expectedCommits := []*pb.GitCommit{
		{
			Id:      "b83d6e391c22777fca1ed3012fce84f633d7fed0",
			Subject: []byte("Merge branch 'branch-merged' into 'master'"),
			Body:    []byte("Merge branch 'branch-merged' into 'master'\r\n\r\nadds bar folder and branch-test text file to check Repository merged_to_root_ref method\r\n\r\n\r\n\r\nSee merge request !12"),
			Author: &pb.CommitAuthor{
				Name:  []byte("Job van der Voort"),
				Email: []byte("job@gitlab.com"),
				Date:  &timestamp.Timestamp{Seconds: 1474987066},
			},
			Committer: &pb.CommitAuthor{
				Name:  []byte("Job van der Voort"),
				Email: []byte("job@gitlab.com"),
				Date:  &timestamp.Timestamp{Seconds: 1474987066},
			},
			ParentIds: []string{
				"1b12f15a11fc6e62177bef08f47bc7b5ce50b141",
				"498214de67004b1da3d820901307bed2a68a8ef6",
			},
		},
		{
			Id:      "4a24d82dbca5c11c61556f3b35ca472b7463187e",
			Subject: []byte("Update README.md to include `Usage in testing and development`"),
			Body:    []byte("Update README.md to include `Usage in testing and development`"),
			Author: &pb.CommitAuthor{
				Name:  []byte("Luke \"Jared\" Bennett"),
				Email: []byte("lbennett@gitlab.com"),
				Date:  &timestamp.Timestamp{Seconds: 1491905339},
			},
			Committer: &pb.CommitAuthor{
				Name:  []byte("Luke \"Jared\" Bennett"),
				Email: []byte("lbennett@gitlab.com"),
				Date:  &timestamp.Timestamp{Seconds: 1491905339},
			},
			ParentIds: []string{"b83d6e391c22777fca1ed3012fce84f633d7fed0"},
		},
		{
			Id:      "e63f41fe459e62e1228fcef60d7189127aeba95a",
			Subject: []byte("Merge branch 'gitlab-test-usage-dev-testing-docs' into 'master'"),
			Body:    []byte("Merge branch 'gitlab-test-usage-dev-testing-docs' into 'master'\r\n\r\nUpdate README.md to include `Usage in testing and development`\r\n\r\nSee merge request !21"),
			Author: &pb.CommitAuthor{
				Name:  []byte("Sean McGivern"),
				Email: []byte("sean@mcgivern.me.uk"),
				Date:  &timestamp.Timestamp{Seconds: 1491906794},
			},
			Committer: &pb.CommitAuthor{
				Name:  []byte("Sean McGivern"),
				Email: []byte("sean@mcgivern.me.uk"),
				Date:  &timestamp.Timestamp{Seconds: 1491906794},
			},
			ParentIds: []string{
				"b83d6e391c22777fca1ed3012fce84f633d7fed0",
				"4a24d82dbca5c11c61556f3b35ca472b7463187e",
			},
		},
	}
	testCases := []struct {
		description     string
		from            []byte
		to              []byte
		expectedCommits []*pb.GitCommit
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
			to:              []byte("master"),
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
			to:              []byte("master"),
			expectedCommits: expectedCommits,
		},
		{
			description:     "To hash doesn't exist",
			from:            from,
			to:              fakeHash,
			expectedCommits: []*pb.GitCommit{},
		},
		{
			description:     "From hash doesn't exist",
			from:            fakeHash,
			to:              to,
			expectedCommits: []*pb.GitCommit{},
		},
		{
			description:     "To ref doesn't exist",
			from:            from,
			to:              fakeRef,
			expectedCommits: []*pb.GitCommit{},
		},
		{
			description:     "From ref doesn't exist",
			from:            fakeRef,
			to:              to,
			expectedCommits: []*pb.GitCommit{},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			commits := []*pb.GitCommit{}
			rpcRequest := pb.CommitsBetweenRequest{
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
				if !testhelper.CommitsEqual(commit, expectedCommits[i]) {
					t.Fatalf("Expected commit\n%v\ngot\n%v", expectedCommits[i], commit)
				}
			}
		})
	}
}

func TestFailedCommitsBetweenRequest(t *testing.T) {
	server := startTestServices(t)
	defer server.Stop()

	client, conn := newCommitServiceClient(t, serverSocketPath)
	defer conn.Close()

	invalidRepo := &pb.Repository{StorageName: "fake", RelativePath: "path"}
	from := []byte("498214de67004b1da3d820901307bed2a68a8ef6")
	to := []byte("e63f41fe459e62e1228fcef60d7189127aeba95a")

	testCases := []struct {
		description string
		repository  *pb.Repository
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
			rpcRequest := pb.CommitsBetweenRequest{
				Repository: tc.repository, From: tc.from, To: tc.to,
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			c, err := client.CommitsBetween(ctx, &rpcRequest)
			if err != nil {
				t.Fatal(err)
			}

			err = drainCommitsBetweenResponse(c)
			testhelper.AssertGrpcError(t, err, tc.code, "")
		})
	}
}

func drainCommitsBetweenResponse(c pb.CommitService_CommitsBetweenClient) error {
	var err error
	for err == nil {
		_, err = c.Recv()
	}
	return err
}
