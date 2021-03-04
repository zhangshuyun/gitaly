package log

import (
	"bytes"
	"context"
	"os"
	"testing"

	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/metadata"
)

func TestMain(m *testing.M) {
	os.Exit(testMain(m))
}

func testMain(m *testing.M) int {
	defer testhelper.MustHaveNoChildProcess()
	cleanup := testhelper.Configure()
	defer cleanup()
	return m.Run()
}

func setupBatch(t *testing.T, ctx context.Context) (config.Cfg, catfile.Batch, *gitalypb.Repository, testhelper.Cleanup) {
	t.Helper()

	var deferrer testhelper.Deferrer
	defer deferrer.Call()

	cfg, repo, _, cleanup := testcfg.BuildWithRepo(t)
	deferrer.Add(cleanup)

	c, err := catfile.New(ctx, git.NewExecCommandFactory(cfg), repo)
	require.NoError(t, err)

	cleaner := deferrer.Relocate()
	return cfg, c, repo, cleaner.Call
}

func TestParseRawCommit(t *testing.T) {
	info := &catfile.ObjectInfo{
		Oid:  "a984dfa4dee018c6d5f5f57ffec0d0e22763df16",
		Type: "commit",
	}

	// Valid-but-interesting commits should be test at the FindCommit level.
	// Invalid objects (that Git would complain about during fsck) can be
	// tested here.
	//
	// Once a repository contains a pathological object it can be hard to get
	// rid of it. Because of this I think it's nicer to ignore such objects
	// than to throw hard errors.
	testCases := []struct {
		desc string
		in   []byte
		out  *gitalypb.GitCommit
	}{
		{
			desc: "empty commit object",
			in:   []byte{},
			out:  &gitalypb.GitCommit{Id: info.Oid},
		},
		{
			desc: "no email",
			in:   []byte("author Jane Doe"),
			out: &gitalypb.GitCommit{
				Id:     info.Oid,
				Author: &gitalypb.CommitAuthor{Name: []byte("Jane Doe")},
			},
		},
		{
			desc: "unmatched <",
			in:   []byte("author Jane Doe <janedoe@example.com"),
			out: &gitalypb.GitCommit{
				Id:     info.Oid,
				Author: &gitalypb.CommitAuthor{Name: []byte("Jane Doe")},
			},
		},
		{
			desc: "unmatched >",
			in:   []byte("author Jane Doe janedoe@example.com>"),
			out: &gitalypb.GitCommit{
				Id:     info.Oid,
				Author: &gitalypb.CommitAuthor{Name: []byte("Jane Doe janedoe@example.com>")},
			},
		},
		{
			desc: "missing date",
			in:   []byte("author Jane Doe <janedoe@example.com> "),
			out: &gitalypb.GitCommit{
				Id:     info.Oid,
				Author: &gitalypb.CommitAuthor{Name: []byte("Jane Doe"), Email: []byte("janedoe@example.com")},
			},
		},
		{
			desc: "date too high",
			in:   []byte("author Jane Doe <janedoe@example.com> 9007199254740993 +0200"),
			out: &gitalypb.GitCommit{
				Id: info.Oid,
				Author: &gitalypb.CommitAuthor{
					Name:     []byte("Jane Doe"),
					Email:    []byte("janedoe@example.com"),
					Date:     &timestamp.Timestamp{Seconds: 9223371974719179007},
					Timezone: []byte("+0200"),
				},
			},
		},
		{
			desc: "date negative",
			in:   []byte("author Jane Doe <janedoe@example.com> -1 +0200"),
			out: &gitalypb.GitCommit{
				Id: info.Oid,
				Author: &gitalypb.CommitAuthor{
					Name:     []byte("Jane Doe"),
					Email:    []byte("janedoe@example.com"),
					Date:     &timestamp.Timestamp{Seconds: 9223371974719179007},
					Timezone: []byte("+0200"),
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			info.Size = int64(len(tc.in))
			out, err := parseRawCommit(bytes.NewBuffer(tc.in), info)
			require.NoError(t, err, "parse error")
			require.Equal(t, tc.out, out)
		})
	}
}

func TestGetCommitCatfile(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	_, c, _, cleanup := setupBatch(t, ctx)
	defer cleanup()

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
			c, err := GetCommitCatfile(ctx, c, git.Revision(tc.revision))

			if tc.errStr == "" {
				require.NoError(t, err)
				require.Equal(t, commitMsg, string(c.Body))
			} else {
				require.EqualError(t, err, tc.errStr)
			}
		})
	}
}

func TestGetCommitCatfileWithTrailers(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	cfg, c, testRepo, cleanup := setupBatch(t, ctx)
	defer cleanup()

	ctx = metadata.NewIncomingContext(ctx, metadata.MD{})

	commit, err := GetCommitCatfileWithTrailers(ctx, git.NewExecCommandFactory(cfg), testRepo, c, "5937ac0a7beb003549fc5fd26fc247adbce4a52e")

	require.NoError(t, err)

	require.Equal(t, commit.Trailers, []*gitalypb.CommitTrailer{
		{
			Key:   []byte("Signed-off-by"),
			Value: []byte("Dmitriy Zaporozhets <dmitriy.zaporozhets@gmail.com>"),
		},
	})
}
