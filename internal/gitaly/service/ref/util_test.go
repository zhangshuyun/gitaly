package ref

import (
	"testing"

	"github.com/golang/protobuf/ptypes/timestamp"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testassert"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

func TestBuildLocalBranch(t *testing.T) {
	testCases := []struct {
		desc string
		in   *gitalypb.GitCommit
		out  *gitalypb.FindLocalBranchResponse
	}{
		{
			desc: "all required fields present",
			in: &gitalypb.GitCommit{
				Id:      "b83d6e391c22777fca1ed3012fce84f633d7fed0",
				Subject: []byte("Merge branch 'branch-merged' into 'master'"),
				Body:    []byte("Merge branch 'branch-merged' into 'master'\r\n\r\nadds bar folder and branch-test text file to check Repository merged_to_root_ref method\r\n\r\n\r\n\r\nSee merge request !12"),
				Author: &gitalypb.CommitAuthor{
					Name:     []byte("Job van der Voort"),
					Email:    []byte("job@gitlab.com"),
					Date:     &timestamp.Timestamp{Seconds: 1474987066},
					Timezone: []byte("+0200"),
				},
				Committer: &gitalypb.CommitAuthor{
					Name:     []byte("Job van der Voort"),
					Email:    []byte("job@gitlab.com"),
					Date:     &timestamp.Timestamp{Seconds: 1474987066},
					Timezone: []byte("+0200"),
				},
				ParentIds: []string{
					"1b12f15a11fc6e62177bef08f47bc7b5ce50b141",
					"498214de67004b1da3d820901307bed2a68a8ef6",
				},
				BodySize: 162,
			},
			out: &gitalypb.FindLocalBranchResponse{
				Name:          []byte("my-branch"),
				CommitId:      "b83d6e391c22777fca1ed3012fce84f633d7fed0",
				CommitSubject: []byte("Merge branch 'branch-merged' into 'master'"),
				CommitAuthor: &gitalypb.FindLocalBranchCommitAuthor{
					Name:     []byte("Job van der Voort"),
					Email:    []byte("job@gitlab.com"),
					Date:     &timestamp.Timestamp{Seconds: 1474987066},
					Timezone: []byte("+0200"),
				},
				CommitCommitter: &gitalypb.FindLocalBranchCommitAuthor{
					Name:     []byte("Job van der Voort"),
					Email:    []byte("job@gitlab.com"),
					Date:     &timestamp.Timestamp{Seconds: 1474987066},
					Timezone: []byte("+0200"),
				},
				Commit: &gitalypb.GitCommit{
					Id:      "b83d6e391c22777fca1ed3012fce84f633d7fed0",
					Subject: []byte("Merge branch 'branch-merged' into 'master'"),
					Body:    []byte("Merge branch 'branch-merged' into 'master'\r\n\r\nadds bar folder and branch-test text file to check Repository merged_to_root_ref method\r\n\r\n\r\n\r\nSee merge request !12"),
					Author: &gitalypb.CommitAuthor{
						Name:     []byte("Job van der Voort"),
						Email:    []byte("job@gitlab.com"),
						Date:     &timestamp.Timestamp{Seconds: 1474987066},
						Timezone: []byte("+0200"),
					},
					Committer: &gitalypb.CommitAuthor{
						Name:     []byte("Job van der Voort"),
						Email:    []byte("job@gitlab.com"),
						Date:     &timestamp.Timestamp{Seconds: 1474987066},
						Timezone: []byte("+0200"),
					},
					ParentIds: []string{"1b12f15a11fc6e62177bef08f47bc7b5ce50b141", "498214de67004b1da3d820901307bed2a68a8ef6"},
					BodySize:  162,
				},
			},
		},
		{
			desc: "missing author",
			in: &gitalypb.GitCommit{
				Id:      "b83d6e391c22777fca1ed3012fce84f633d7fed0",
				Subject: []byte("Merge branch 'branch-merged' into 'master'"),
				Body:    []byte("Merge branch 'branch-merged' into 'master'\r\n\r\nadds bar folder and branch-test text file to check Repository merged_to_root_ref method\r\n\r\n\r\n\r\nSee merge request !12"),
				Committer: &gitalypb.CommitAuthor{
					Name:     []byte("Job van der Voort"),
					Email:    []byte("job@gitlab.com"),
					Date:     &timestamp.Timestamp{Seconds: 1474987066},
					Timezone: []byte("+0200"),
				},
				ParentIds: []string{
					"1b12f15a11fc6e62177bef08f47bc7b5ce50b141",
					"498214de67004b1da3d820901307bed2a68a8ef6",
				},
				BodySize: 162,
			},
			out: &gitalypb.FindLocalBranchResponse{
				Name:          []byte("my-branch"),
				CommitId:      "b83d6e391c22777fca1ed3012fce84f633d7fed0",
				CommitSubject: []byte("Merge branch 'branch-merged' into 'master'"),
				CommitAuthor:  nil,
				CommitCommitter: &gitalypb.FindLocalBranchCommitAuthor{
					Name:     []byte("Job van der Voort"),
					Email:    []byte("job@gitlab.com"),
					Date:     &timestamp.Timestamp{Seconds: 1474987066},
					Timezone: []byte("+0200"),
				},
				Commit: &gitalypb.GitCommit{
					Id:      "b83d6e391c22777fca1ed3012fce84f633d7fed0",
					Subject: []byte("Merge branch 'branch-merged' into 'master'"),
					Body:    []byte("Merge branch 'branch-merged' into 'master'\r\n\r\nadds bar folder and branch-test text file to check Repository merged_to_root_ref method\r\n\r\n\r\n\r\nSee merge request !12"),
					Committer: &gitalypb.CommitAuthor{
						Name:     []byte("Job van der Voort"),
						Email:    []byte("job@gitlab.com"),
						Date:     &timestamp.Timestamp{Seconds: 1474987066},
						Timezone: []byte("+0200"),
					},
					ParentIds: []string{"1b12f15a11fc6e62177bef08f47bc7b5ce50b141", "498214de67004b1da3d820901307bed2a68a8ef6"},
					BodySize:  162,
				},
			},
		},
		{
			desc: "missing committer",
			in: &gitalypb.GitCommit{
				Id:      "b83d6e391c22777fca1ed3012fce84f633d7fed0",
				Subject: []byte("Merge branch 'branch-merged' into 'master'"),
				Body:    []byte("Merge branch 'branch-merged' into 'master'\r\n\r\nadds bar folder and branch-test text file to check Repository merged_to_root_ref method\r\n\r\n\r\n\r\nSee merge request !12"),
				Author: &gitalypb.CommitAuthor{
					Name:     []byte("Job van der Voort"),
					Email:    []byte("job@gitlab.com"),
					Date:     &timestamp.Timestamp{Seconds: 1474987066},
					Timezone: []byte("+0200"),
				},
				ParentIds: []string{
					"1b12f15a11fc6e62177bef08f47bc7b5ce50b141",
					"498214de67004b1da3d820901307bed2a68a8ef6",
				},
				BodySize: 162,
			},
			out: &gitalypb.FindLocalBranchResponse{
				Name:          []byte("my-branch"),
				CommitId:      "b83d6e391c22777fca1ed3012fce84f633d7fed0",
				CommitSubject: []byte("Merge branch 'branch-merged' into 'master'"),
				CommitAuthor: &gitalypb.FindLocalBranchCommitAuthor{
					Name:     []byte("Job van der Voort"),
					Email:    []byte("job@gitlab.com"),
					Date:     &timestamp.Timestamp{Seconds: 1474987066},
					Timezone: []byte("+0200"),
				},
				CommitCommitter: nil,
				Commit: &gitalypb.GitCommit{
					Id:      "b83d6e391c22777fca1ed3012fce84f633d7fed0",
					Subject: []byte("Merge branch 'branch-merged' into 'master'"),
					Body:    []byte("Merge branch 'branch-merged' into 'master'\r\n\r\nadds bar folder and branch-test text file to check Repository merged_to_root_ref method\r\n\r\n\r\n\r\nSee merge request !12"),
					Author: &gitalypb.CommitAuthor{
						Name:     []byte("Job van der Voort"),
						Email:    []byte("job@gitlab.com"),
						Date:     &timestamp.Timestamp{Seconds: 1474987066},
						Timezone: []byte("+0200"),
					},
					ParentIds: []string{"1b12f15a11fc6e62177bef08f47bc7b5ce50b141", "498214de67004b1da3d820901307bed2a68a8ef6"},
					BodySize:  162,
				},
			},
		},
		{
			desc: "nil commit",
			in:   nil,
			out: &gitalypb.FindLocalBranchResponse{
				Name: []byte("my-branch"),
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			testassert.ProtoEqual(t, tc.out, buildLocalBranch([]byte("my-branch"), tc.in))
		})
	}
}
