package log

import (
	"bytes"
	"os"
	"testing"

	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v13/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v13/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v13/proto/go/gitalypb"
	"google.golang.org/grpc/metadata"
)

func TestMain(m *testing.M) {
	testhelper.Configure()
	os.Exit(m.Run())
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
	ctx = metadata.NewIncomingContext(ctx, metadata.MD{})
	defer cancel()

	testRepo, _, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()

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

	c, err := catfile.New(ctx, testRepo)
	require.NoError(t, err)
	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			c, err := GetCommitCatfile(c, tc.revision)

			if tc.errStr == "" {
				require.NoError(t, err)
				require.Equal(t, commitMsg, string(c.Body))
			} else {
				require.EqualError(t, err, tc.errStr)
			}
		})
	}
}
