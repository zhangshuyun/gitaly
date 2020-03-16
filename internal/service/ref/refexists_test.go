package ref

import (
	"testing"

	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
)

func TestRefExists(t *testing.T) {
	testRepo, _, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	badRepo := &gitalypb.Repository{StorageName: "invalid", RelativePath: "/etc/"}

	tests := []struct {
		name    string
		ref     string
		want    bool
		repo    *gitalypb.Repository
		wantErr codes.Code
	}{
		{"master", "refs/heads/master", true, testRepo, codes.OK},
		{"v1.0.0", "refs/tags/v1.0.0", true, testRepo, codes.OK},
		{"quoted", "refs/heads/'test'", true, testRepo, codes.OK},
		{"unicode exists", "refs/heads/ʕ•ᴥ•ʔ", true, testRepo, codes.OK},
		{"unicode missing", "refs/tags/अस्तित्वहीन", false, testRepo, codes.OK},
		{"spaces", "refs/ /heads", false, testRepo, codes.OK},
		{"haxxors", "refs/; rm -rf /tmp/*", false, testRepo, codes.OK},
		{"dashes", "--", false, testRepo, codes.InvalidArgument},
		{"blank", "", false, testRepo, codes.InvalidArgument},
		{"not tags or branches", "refs/heads/remotes/origin", false, testRepo, codes.OK},
		{"wildcards", "refs/heads/*", false, testRepo, codes.OK},
		{"invalid repos", "refs/heads/master", false, badRepo, codes.InvalidArgument},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := testhelper.Context()
			defer cancel()

			stop, serverSocketPath := runRefServiceServer(t)
			defer stop()

			client, conn := newRefServiceClient(t, serverSocketPath)
			defer conn.Close()

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
