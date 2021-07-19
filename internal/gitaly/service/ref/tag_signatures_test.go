package ref

import (
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testassert"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestGetTagSignatures(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	cfg, repoProto, repoPath, client := setupRefService(t)

	message1 := strings.Repeat("a", helper.MaxCommitOrTagMessageSize) + "\n"
	signature1 := string(testhelper.MustReadFile(t, "testdata/tag-1e292f8fedd741b75372e19097c76d327140c312-signature"))
	tag1ID := gittest.CreateTag(t, cfg, repoPath, "big-tag-1", "master", &gittest.CreateTagOpts{Message: message1 + signature1})
	content1 := "object 1e292f8fedd741b75372e19097c76d327140c312\ntype commit\ntag big-tag-1\ntagger Scrooge McDuck <scrooge@mcduck.com> 1572776879 +0100\n\n" + message1

	message2 := strings.Repeat("b", helper.MaxCommitOrTagMessageSize) + "\n"
	signature2 := string(testhelper.MustReadFile(t, "testdata/tag-7975be0116940bf2ad4321f79d02a55c5f7779aa-signature"))
	tag2ID := gittest.CreateTag(t, cfg, repoPath, "big-tag-2", "master~", &gittest.CreateTagOpts{Message: message2 + signature2})
	content2 := "object 7975be0116940bf2ad4321f79d02a55c5f7779aa\ntype commit\ntag big-tag-2\ntagger Scrooge McDuck <scrooge@mcduck.com> 1572776879 +0100\n\n" + message2

	message3 := "tag message\n"
	tag3ID := gittest.CreateTag(t, cfg, repoPath, "tag-3", "master~~", &gittest.CreateTagOpts{Message: message3})
	content3 := "object 60ecb67744cb56576c30214ff52294f8ce2def98\ntype commit\ntag tag-3\ntagger Scrooge McDuck <scrooge@mcduck.com> 1572776879 +0100\n\n" + message3

	for _, tc := range []struct {
		desc               string
		revisions          []string
		expectedErr        error
		expectedSignatures []*gitalypb.GetTagSignaturesResponse_TagSignature
	}{
		{
			desc:        "missing revisions",
			revisions:   []string{},
			expectedErr: status.Error(codes.InvalidArgument, "missing revisions"),
		},
		{
			desc: "invalid revision",
			revisions: []string{
				"--foobar",
			},
			expectedErr: status.Error(codes.InvalidArgument, "invalid revision: \"--foobar\""),
		},
		{
			desc: "unknown id",
			revisions: []string{
				"b10ff336f3fbfb131431c4959915cdfd1b49c635",
			},
			expectedErr: status.Error(codes.Internal, "rev-list pipeline command: exit status 128"),
		},
		{
			desc: "commit id",
			revisions: []string{
				"1e292f8fedd741b75372e19097c76d327140c312",
			},
			expectedSignatures: nil,
		},
		{
			desc: "commit ref",
			revisions: []string{
				"refs/heads/master",
			},
			expectedSignatures: nil,
		},
		{
			desc: "single tag signature",
			revisions: []string{
				tag1ID,
			},
			expectedSignatures: []*gitalypb.GetTagSignaturesResponse_TagSignature{
				{
					TagId:     tag1ID,
					Signature: []byte(signature1),
					Content:   []byte(content1),
				},
			},
		},
		{
			desc: "single tag signature by short SHA",
			revisions: []string{
				tag1ID[:7],
			},
			expectedSignatures: []*gitalypb.GetTagSignaturesResponse_TagSignature{
				{
					TagId:     tag1ID,
					Signature: []byte(signature1),
					Content:   []byte(content1),
				},
			},
		},
		{
			desc: "single tag signature by ref",
			revisions: []string{
				"refs/tags/big-tag-1",
			},
			expectedSignatures: []*gitalypb.GetTagSignaturesResponse_TagSignature{
				{
					TagId:     tag1ID,
					Signature: []byte(signature1),
					Content:   []byte(content1),
				},
			},
		},
		{
			desc: "multiple tag signatures",
			revisions: []string{
				tag1ID,
				tag2ID,
			},
			expectedSignatures: []*gitalypb.GetTagSignaturesResponse_TagSignature{
				{
					TagId:     tag1ID,
					Signature: []byte(signature1),
					Content:   []byte(content1),
				},
				{
					TagId:     tag2ID,
					Signature: []byte(signature2),
					Content:   []byte(content2),
				},
			},
		},
		{
			desc: "tag without signature",
			revisions: []string{
				tag3ID,
			},
			expectedSignatures: []*gitalypb.GetTagSignaturesResponse_TagSignature{
				{
					TagId:     tag3ID,
					Signature: []byte(""),
					Content:   []byte(content3),
				},
			},
		},
		{
			desc: "pseudorevisions",
			revisions: []string{
				"--not",
				"--all",
			},
			expectedSignatures: nil,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			stream, err := client.GetTagSignatures(ctx, &gitalypb.GetTagSignaturesRequest{
				Repository:   repoProto,
				TagRevisions: tc.revisions,
			})
			require.NoError(t, err)

			var signatures []*gitalypb.GetTagSignaturesResponse_TagSignature
			for {
				resp, err := stream.Recv()
				if err != nil {
					if !errors.Is(err, io.EOF) {
						testassert.GrpcEqualErr(t, tc.expectedErr, err)
					}
					break
				}

				signatures = append(signatures, resp.Signatures...)
			}

			testassert.ProtoEqual(t, tc.expectedSignatures, signatures)
		})
	}
}
