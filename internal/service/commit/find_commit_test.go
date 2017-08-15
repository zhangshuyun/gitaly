package commit

import (
	"testing"

	"github.com/golang/protobuf/ptypes/timestamp"
	pb "gitlab.com/gitlab-org/gitaly-proto/go"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
)

func TestSuccessfulFindCommitRequest(t *testing.T) {
	server := startTestServices(t)
	defer server.Stop()

	client, conn := newCommitServiceClient(t, serverSocketPath)
	defer conn.Close()

	testCases := []struct {
		description string
		revision    string
		commit      *pb.GitCommit
	}{
		{
			description: "With a branch name",
			revision:    "branch-merged",
			commit: &pb.GitCommit{
				Id:      "498214de67004b1da3d820901307bed2a68a8ef6",
				Subject: []byte("adds bar folder and branch-test text file to check Repository merged_to_root_ref method"),
				Body:    []byte("adds bar folder and branch-test text file to check Repository merged_to_root_ref method\n"),
				Author: &pb.CommitAuthor{
					Name:  []byte("tiagonbotelho"),
					Email: []byte("tiagonbotelho@hotmail.com"),
					Date:  &timestamp.Timestamp{Seconds: 1474470806},
				},
				Committer: &pb.CommitAuthor{
					Name:  []byte("tiagonbotelho"),
					Email: []byte("tiagonbotelho@hotmail.com"),
					Date:  &timestamp.Timestamp{Seconds: 1474470806},
				},
				ParentIds: []string{"1b12f15a11fc6e62177bef08f47bc7b5ce50b141"},
			},
		},
		{
			description: "With a tag name",
			revision:    "v1.0.0",
			commit: &pb.GitCommit{
				Id:      "6f6d7e7ed97bb5f0054f2b1df789b39ca89b6ff9",
				Subject: []byte("More submodules"),
				Body:    []byte("More submodules\n\nSigned-off-by: Dmitriy Zaporozhets <dmitriy.zaporozhets@gmail.com>\n"),
				Author: &pb.CommitAuthor{
					Name:  []byte("Dmitriy Zaporozhets"),
					Email: []byte("dmitriy.zaporozhets@gmail.com"),
					Date:  &timestamp.Timestamp{Seconds: 1393491261},
				},
				Committer: &pb.CommitAuthor{
					Name:  []byte("Dmitriy Zaporozhets"),
					Email: []byte("dmitriy.zaporozhets@gmail.com"),
					Date:  &timestamp.Timestamp{Seconds: 1393491261},
				},
				ParentIds: []string{"d14d6c0abdd253381df51a723d58691b2ee1ab08"},
			},
		},
		{
			description: "With a hash",
			revision:    "b83d6e391c22777fca1ed3012fce84f633d7fed0",
			commit: &pb.GitCommit{
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
		},
		{
			description: "With an initial commit",
			revision:    "1a0b36b3cdad1d2ee32457c102a8c0b7056fa863",
			commit: &pb.GitCommit{
				Id:      "1a0b36b3cdad1d2ee32457c102a8c0b7056fa863",
				Subject: []byte("Initial commit"),
				Body:    []byte("Initial commit\n"),
				Author: &pb.CommitAuthor{
					Name:  []byte("Dmitriy Zaporozhets"),
					Email: []byte("dmitriy.zaporozhets@gmail.com"),
					Date:  &timestamp.Timestamp{Seconds: 1393488198},
				},
				Committer: &pb.CommitAuthor{
					Name:  []byte("Dmitriy Zaporozhets"),
					Email: []byte("dmitriy.zaporozhets@gmail.com"),
					Date:  &timestamp.Timestamp{Seconds: 1393488198},
				},
				// In practice, same as `[]string{}`... but not for `DeepEquals`
				// (which is used to check the ParentIds in `CommitsEqual`)
				ParentIds: nil,
			},
		},
		{
			description: "With a non-existing ref name",
			revision:    "this-doesnt-exists",
			commit:      nil,
		},
		{
			description: "With a non-existing hash",
			revision:    "f48214de67004b1da3d820901307bed2a68a8ef6",
			commit:      nil,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.description, func(t *testing.T) {
			request := &pb.FindCommitRequest{
				Repository: testRepo,
				Revision:   []byte(testCase.revision),
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			response, err := client.FindCommit(ctx, request)
			if err != nil {
				t.Fatal(err)
			}

			if !testhelper.CommitsEqual(testCase.commit, response.Commit) {
				t.Fatalf("Expected commit %v, got %v", testCase.commit, response.Commit)
			}
		})
	}
}

func TestFailedFindCommitRequest(t *testing.T) {
	server := startTestServices(t)
	defer server.Stop()

	client, conn := newCommitServiceClient(t, serverSocketPath)
	defer conn.Close()
	invalidRepo := &pb.Repository{StorageName: "fake", RelativePath: "path"}

	testCases := []struct {
		description string
		revision    []byte
		repo        *pb.Repository
	}{
		{repo: invalidRepo, revision: []byte("master"), description: "Invalid repo"},
		{repo: testRepo, revision: []byte(""), description: "Empty revision"},
		{repo: testRepo, revision: []byte("-master"), description: "Invalid revision"},
	}

	for _, testCase := range testCases {
		t.Run(testCase.description, func(t *testing.T) {
			request := &pb.FindCommitRequest{
				Repository: testCase.repo,
				Revision:   testCase.revision,
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			_, err := client.FindCommit(ctx, request)
			testhelper.AssertGrpcError(t, err, codes.InvalidArgument, "")
		})
	}
}
