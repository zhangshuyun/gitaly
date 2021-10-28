package catfile

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"google.golang.org/grpc/metadata"
)

func TestGetCommit(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	_, objectReader, _ := setupObjectReader(t, ctx)

	ctx = metadata.NewIncomingContext(ctx, metadata.MD{})

	const commitSha = "2d1db523e11e777e49377cfb22d368deec3f0793"
	const commitMsg = "Correct test_env.rb path for adding branch\n"
	const blobSha = "c60514b6d3d6bf4bec1030f70026e34dfbd69ad5"

	testCases := []struct {
		desc     string
		revision string
		errStr   string
	}{
		{
			desc:     "commit",
			revision: commitSha,
		},
		{
			desc:     "not existing commit",
			revision: "not existing revision",
			errStr:   "object not found",
		},
		{
			desc:     "blob sha",
			revision: blobSha,
			errStr:   "object not found",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			c, err := GetCommit(ctx, objectReader, git.Revision(tc.revision))

			if tc.errStr == "" {
				require.NoError(t, err)
				require.Equal(t, commitMsg, string(c.Body))
			} else {
				require.EqualError(t, err, tc.errStr)
			}
		})
	}
}

func TestGetCommitWithTrailers(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	cfg, objectReader, testRepo := setupObjectReader(t, ctx)

	ctx = metadata.NewIncomingContext(ctx, metadata.MD{})

	commit, err := GetCommitWithTrailers(ctx, git.NewExecCommandFactory(cfg), testRepo,
		objectReader, "5937ac0a7beb003549fc5fd26fc247adbce4a52e")

	require.NoError(t, err)

	require.Equal(t, commit.Trailers, []*gitalypb.CommitTrailer{
		{
			Key:   []byte("Signed-off-by"),
			Value: []byte("Dmitriy Zaporozhets <dmitriy.zaporozhets@gmail.com>"),
		},
	})
}
