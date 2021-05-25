package ref

import (
	"testing"

	"gitlab.com/gitlab-org/gitaly/v14/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
)

func TestRefExists(t *testing.T) {
	_, repo, _, client := setupRefService(t)

	badRepo := &gitalypb.Repository{StorageName: "invalid", RelativePath: "/etc/"}

	tests := []struct {
		name    string
		ref     string
		want    bool
		repo    *gitalypb.Repository
		wantErr codes.Code
	}{
		{"master", "refs/heads/master", true, repo, codes.OK},
		{"v1.0.0", "refs/tags/v1.0.0", true, repo, codes.OK},
		{"quoted", "refs/heads/'test'", true, repo, codes.OK},
		{"unicode exists", "refs/heads/ʕ•ᴥ•ʔ", true, repo, codes.OK},
		{"unicode missing", "refs/tags/अस्तित्वहीन", false, repo, codes.OK},
		{"spaces", "refs/ /heads", false, repo, codes.OK},
		{"haxxors", "refs/; rm -rf /tmp/*", false, repo, codes.OK},
		{"dashes", "--", false, repo, codes.InvalidArgument},
		{"blank", "", false, repo, codes.InvalidArgument},
		{"not tags or branches", "refs/heads/remotes/origin", false, repo, codes.OK},
		{"wildcards", "refs/heads/*", false, repo, codes.OK},
		{"invalid repos", "refs/heads/master", false, badRepo, codes.InvalidArgument},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := testhelper.Context()
			defer cancel()

			req := &gitalypb.RefExistsRequest{Repository: tt.repo, Ref: []byte(tt.ref)}

			got, err := client.RefExists(ctx, req)

			if helper.GrpcCode(err) != tt.wantErr {
				t.Errorf("server.RefExists() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if tt.wantErr != codes.OK {
				if got != nil {
					t.Errorf("server.RefExists() = %v, want null", got)
				}
				return
			}

			if got.Value != tt.want {
				t.Errorf("server.RefExists() = %v, want %v", got.Value, tt.want)
			}
		})
	}
}
