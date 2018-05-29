package rubyserver

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	pb "gitlab.com/gitlab-org/gitaly-proto/go"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"

	"google.golang.org/grpc/codes"
)

func TestStopSafe(t *testing.T) {
	badServers := []*Server{
		nil,
		&Server{},
	}

	for _, bs := range badServers {
		bs.Stop()
	}
}

func TestSetHeaders(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	testCases := []struct {
		desc    string
		repo    *pb.Repository
		errType codes.Code
		setter  func(context.Context, *pb.Repository) (context.Context, error)
	}{
		{
			desc:    "SetHeaders invalid storage",
			repo:    &pb.Repository{StorageName: "foo", RelativePath: "bar.git"},
			errType: codes.InvalidArgument,
			setter:  SetHeaders,
		},
		{
			desc:    "SetHeaders invalid rel path",
			repo:    &pb.Repository{StorageName: testRepo.StorageName, RelativePath: "bar.git"},
			errType: codes.NotFound,
			setter:  SetHeaders,
		},
		{
			desc:    "SetHeaders OK",
			repo:    testRepo,
			errType: codes.OK,
			setter:  SetHeaders,
		},
		{
			desc:    "SetHeadersWithoutRepoCheck invalid storage",
			repo:    &pb.Repository{StorageName: "foo", RelativePath: "bar.git"},
			errType: codes.InvalidArgument,
			setter:  SetHeadersWithoutRepoCheck,
		},
		{
			desc:    "SetHeadersWithoutRepoCheck invalid relative path",
			repo:    &pb.Repository{StorageName: testRepo.StorageName, RelativePath: "bar.git"},
			errType: codes.OK,
			setter:  SetHeadersWithoutRepoCheck,
		},
		{
			desc:    "SetHeadersWithoutRepoCheck OK",
			repo:    testRepo,
			errType: codes.OK,
			setter:  SetHeadersWithoutRepoCheck,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			clientCtx, err := tc.setter(ctx, tc.repo)

			if tc.errType != codes.OK {
				testhelper.RequireGrpcError(t, err, tc.errType)
				assert.Nil(t, clientCtx)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, clientCtx)
			}
		})
	}
}
