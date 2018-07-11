package ref

import (
	"testing"

	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/stretchr/testify/require"
	pb "gitlab.com/gitlab-org/gitaly-proto/go"
)

func TestBuildLocalBranch(t *testing.T) {
	testCases := []struct {
		desc string
		in   *pb.GitCommit
		out  *pb.FindLocalBranchResponse
	}{
		{
			desc: "all required fields present",
			in: &pb.GitCommit{
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
				BodySize: 162,
			},
			out: &pb.FindLocalBranchResponse{
				Name:          []byte("my-branch"),
				CommitId:      "b83d6e391c22777fca1ed3012fce84f633d7fed0",
				CommitSubject: []byte("Merge branch 'branch-merged' into 'master'"),
				CommitAuthor: &pb.FindLocalBranchCommitAuthor{
					Name:  []byte("Job van der Voort"),
					Email: []byte("job@gitlab.com"),
					Date:  &timestamp.Timestamp{Seconds: 1474987066},
				},
				CommitCommitter: &pb.FindLocalBranchCommitAuthor{
					Name:  []byte("Job van der Voort"),
					Email: []byte("job@gitlab.com"),
					Date:  &timestamp.Timestamp{Seconds: 1474987066},
				},
			},
		},
		{
			desc: "missing author",
			in: &pb.GitCommit{
				Id:      "b83d6e391c22777fca1ed3012fce84f633d7fed0",
				Subject: []byte("Merge branch 'branch-merged' into 'master'"),
				Body:    []byte("Merge branch 'branch-merged' into 'master'\r\n\r\nadds bar folder and branch-test text file to check Repository merged_to_root_ref method\r\n\r\n\r\n\r\nSee merge request !12"),
				Committer: &pb.CommitAuthor{
					Name:  []byte("Job van der Voort"),
					Email: []byte("job@gitlab.com"),
					Date:  &timestamp.Timestamp{Seconds: 1474987066},
				},
				ParentIds: []string{
					"1b12f15a11fc6e62177bef08f47bc7b5ce50b141",
					"498214de67004b1da3d820901307bed2a68a8ef6",
				},
				BodySize: 162,
			},
			out: &pb.FindLocalBranchResponse{
				Name:          []byte("my-branch"),
				CommitId:      "b83d6e391c22777fca1ed3012fce84f633d7fed0",
				CommitSubject: []byte("Merge branch 'branch-merged' into 'master'"),
				CommitAuthor:  nil,
				CommitCommitter: &pb.FindLocalBranchCommitAuthor{
					Name:  []byte("Job van der Voort"),
					Email: []byte("job@gitlab.com"),
					Date:  &timestamp.Timestamp{Seconds: 1474987066},
				},
			},
		},
		{
			desc: "missing committer",
			in: &pb.GitCommit{
				Id:      "b83d6e391c22777fca1ed3012fce84f633d7fed0",
				Subject: []byte("Merge branch 'branch-merged' into 'master'"),
				Body:    []byte("Merge branch 'branch-merged' into 'master'\r\n\r\nadds bar folder and branch-test text file to check Repository merged_to_root_ref method\r\n\r\n\r\n\r\nSee merge request !12"),
				Author: &pb.CommitAuthor{
					Name:  []byte("Job van der Voort"),
					Email: []byte("job@gitlab.com"),
					Date:  &timestamp.Timestamp{Seconds: 1474987066},
				},
				ParentIds: []string{
					"1b12f15a11fc6e62177bef08f47bc7b5ce50b141",
					"498214de67004b1da3d820901307bed2a68a8ef6",
				},
				BodySize: 162,
			},
			out: &pb.FindLocalBranchResponse{
				Name:          []byte("my-branch"),
				CommitId:      "b83d6e391c22777fca1ed3012fce84f633d7fed0",
				CommitSubject: []byte("Merge branch 'branch-merged' into 'master'"),
				CommitAuthor: &pb.FindLocalBranchCommitAuthor{
					Name:  []byte("Job van der Voort"),
					Email: []byte("job@gitlab.com"),
					Date:  &timestamp.Timestamp{Seconds: 1474987066},
				},
				CommitCommitter: nil,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			require.Equal(t, *tc.out, *buildLocalBranch([]byte("my-branch"), tc.in))
		})
	}
}
