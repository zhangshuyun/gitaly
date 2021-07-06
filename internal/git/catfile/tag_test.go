package catfile

import (
	"bytes"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestGetTag(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	cfg, c, testRepo := setupBatch(t, ctx)

	testRepoPath := filepath.Join(cfg.Storages[0].Path, testRepo.RelativePath)

	testCases := []struct {
		tagName string
		rev     string
		message string
		trim    bool
	}{
		{
			tagName: fmt.Sprintf("%s-v1.0.2", t.Name()),
			rev:     "master^^^^",
			message: strings.Repeat("a", helper.MaxCommitOrTagMessageSize+1),
			trim:    false,
		},
		{
			tagName: fmt.Sprintf("%s-v1.0.0", t.Name()),
			rev:     "master^^^",
			message: "Prod Release v1.0.0",
			trim:    true,
		},
		{
			tagName: fmt.Sprintf("%s-v1.0.1", t.Name()),
			rev:     "master^^",
			message: strings.Repeat("a", helper.MaxCommitOrTagMessageSize+1),
			trim:    true,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.tagName, func(t *testing.T) {
			tagID := gittest.CreateTag(t, cfg, testRepoPath, testCase.tagName, testCase.rev, &gittest.CreateTagOpts{Message: testCase.message})

			tag, err := GetTag(ctx, c, git.Revision(tagID), testCase.tagName, testCase.trim, true)
			require.NoError(t, err)
			if testCase.trim && len(testCase.message) >= helper.MaxCommitOrTagMessageSize {
				testCase.message = testCase.message[:helper.MaxCommitOrTagMessageSize]
			}

			require.Equal(t, testCase.message, string(tag.Message))
			require.Equal(t, testCase.tagName, string(tag.GetName()))
		})
	}
}

func TestParseTag(t *testing.T) {
	for _, tc := range []struct {
		desc        string
		oid         git.ObjectID
		contents    string
		expectedTag *gitalypb.Tag
	}{
		{
			desc:     "tag without a message",
			contents: "object c92faf3e0a557270141be67f206d7cdb99bfc3a2\ntype commit\ntag v2.6.16.28\ntagger Adrian Bunk <bunk@stusta.de> 1156539089 +0200",
			oid:      "1234",
			expectedTag: &gitalypb.Tag{
				Id:   "1234",
				Name: []byte("v2.6.16.28"),
				Tagger: &gitalypb.CommitAuthor{
					Name:  []byte("Adrian Bunk"),
					Email: []byte("bunk@stusta.de"),
					Date: &timestamppb.Timestamp{
						Seconds: 1156539089,
					},
					Timezone: []byte("+0200"),
				},
			},
		},
		{
			desc:     "tag with message",
			contents: "object c92faf3e0a557270141be67f206d7cdb99bfc3a2\ntype commit\ntag v2.6.16.28\ntagger Adrian Bunk <bunk@stusta.de> 1156539089 +0200\n\nmessage",
			oid:      "1234",
			expectedTag: &gitalypb.Tag{
				Id:          "1234",
				Name:        []byte("v2.6.16.28"),
				Message:     []byte("message"),
				MessageSize: 7,
				Tagger: &gitalypb.CommitAuthor{
					Name:  []byte("Adrian Bunk"),
					Email: []byte("bunk@stusta.de"),
					Date: &timestamppb.Timestamp{
						Seconds: 1156539089,
					},
					Timezone: []byte("+0200"),
				},
			},
		},
		{
			desc:     "tag with empty message",
			oid:      "1234",
			contents: "object c92faf3e0a557270141be67f206d7cdb99bfc3a2\ntype commit\ntag v2.6.16.28\ntagger Adrian Bunk <bunk@stusta.de> 1156539089 +0200\n\n",
			expectedTag: &gitalypb.Tag{
				Id:      "1234",
				Name:    []byte("v2.6.16.28"),
				Message: []byte{},
				Tagger: &gitalypb.CommitAuthor{
					Name:  []byte("Adrian Bunk"),
					Email: []byte("bunk@stusta.de"),
					Date: &timestamppb.Timestamp{
						Seconds: 1156539089,
					},
					Timezone: []byte("+0200"),
				},
			},
		},
		{
			desc:     "tag with message with empty line",
			oid:      "1234",
			contents: "object c92faf3e0a557270141be67f206d7cdb99bfc3a2\ntype commit\ntag v2.6.16.28\ntagger Adrian Bunk <bunk@stusta.de> 1156539089 +0200\n\nHello world\n\nThis is a message",
			expectedTag: &gitalypb.Tag{
				Id:          "1234",
				Name:        []byte("v2.6.16.28"),
				Message:     []byte("Hello world\n\nThis is a message"),
				MessageSize: 30,
				Tagger: &gitalypb.CommitAuthor{
					Name:  []byte("Adrian Bunk"),
					Email: []byte("bunk@stusta.de"),
					Date: &timestamppb.Timestamp{
						Seconds: 1156539089,
					},
					Timezone: []byte("+0200"),
				},
			},
		},
		{
			desc:     "tag with message with empty line and right side new line trimming",
			contents: "object c92faf3e0a557270141be67f206d7cdb99bfc3a2\ntype commit\ntag v2.6.16.28\ntagger Adrian Bunk <bunk@stusta.de> 1156539089 +0200\n\nHello world\n\nThis is a message\n\n",
			oid:      "1234",
			expectedTag: &gitalypb.Tag{
				Id:          "1234",
				Name:        []byte("v2.6.16.28"),
				Message:     []byte("Hello world\n\nThis is a message"),
				MessageSize: 30,
				Tagger: &gitalypb.CommitAuthor{
					Name:  []byte("Adrian Bunk"),
					Email: []byte("bunk@stusta.de"),
					Date: &timestamppb.Timestamp{
						Seconds: 1156539089,
					},
					Timezone: []byte("+0200"),
				},
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			tag, err := ParseTag(strings.NewReader(tc.contents), tc.oid)
			require.NoError(t, err)
			require.Equal(t, tc.expectedTag, tag)
		})
	}
}

func TestSplitRawTag(t *testing.T) {
	testCases := []struct {
		description string
		tagContent  string
		header      tagHeader
		body        []byte
		trimNewLine bool
	}{
		{
			description: "tag without a message",
			tagContent:  "object c92faf3e0a557270141be67f206d7cdb99bfc3a2\ntype commit\ntag v2.6.16.28\ntagger Adrian Bunk <bunk@stusta.de> 1156539089 +0200",
			header: tagHeader{
				oid:     "c92faf3e0a557270141be67f206d7cdb99bfc3a2",
				tagType: "commit",
				tag:     "v2.6.16.28",
				tagger:  "Adrian Bunk <bunk@stusta.de> 1156539089 +0200",
			},
			body: nil,
		},
		{
			description: "tag with message",
			tagContent:  "object c92faf3e0a557270141be67f206d7cdb99bfc3a2\ntype commit\ntag v2.6.16.28\ntagger Adrian Bunk <bunk@stusta.de> 1156539089 +0200\n\nmessage",
			header: tagHeader{
				oid:     "c92faf3e0a557270141be67f206d7cdb99bfc3a2",
				tagType: "commit",
				tag:     "v2.6.16.28",
				tagger:  "Adrian Bunk <bunk@stusta.de> 1156539089 +0200",
			},
			body: []byte("message"),
		},
		{
			description: "tag with empty message",
			tagContent:  "object c92faf3e0a557270141be67f206d7cdb99bfc3a2\ntype commit\ntag v2.6.16.28\ntagger Adrian Bunk <bunk@stusta.de> 1156539089 +0200\n\n",
			header: tagHeader{
				oid:     "c92faf3e0a557270141be67f206d7cdb99bfc3a2",
				tagType: "commit",
				tag:     "v2.6.16.28",
				tagger:  "Adrian Bunk <bunk@stusta.de> 1156539089 +0200",
			},
			body: []byte{},
		},
		{
			description: "tag with message with empty line",
			tagContent:  "object c92faf3e0a557270141be67f206d7cdb99bfc3a2\ntype commit\ntag v2.6.16.28\ntagger Adrian Bunk <bunk@stusta.de> 1156539089 +0200\n\nHello world\n\nThis is a message",
			header: tagHeader{
				oid:     "c92faf3e0a557270141be67f206d7cdb99bfc3a2",
				tagType: "commit",
				tag:     "v2.6.16.28",
				tagger:  "Adrian Bunk <bunk@stusta.de> 1156539089 +0200",
			},
			body: []byte("Hello world\n\nThis is a message"),
		},
		{
			description: "tag with message with empty line and right side new line trimming",
			tagContent:  "object c92faf3e0a557270141be67f206d7cdb99bfc3a2\ntype commit\ntag v2.6.16.28\ntagger Adrian Bunk <bunk@stusta.de> 1156539089 +0200\n\nHello world\n\nThis is a message\n\n",
			header: tagHeader{
				oid:     "c92faf3e0a557270141be67f206d7cdb99bfc3a2",
				tagType: "commit",
				tag:     "v2.6.16.28",
				tagger:  "Adrian Bunk <bunk@stusta.de> 1156539089 +0200",
			},
			body:        []byte("Hello world\n\nThis is a message"),
			trimNewLine: true,
		},
		{
			description: "tag with missing date and body",
			tagContent:  "object 422081655f743e03b01ee29a2eaf26aab0ee7eda\ntype commit\ntag syslinux-3.11-pre6\ntagger hpa <hpa>\n",
			header: tagHeader{
				oid:     "422081655f743e03b01ee29a2eaf26aab0ee7eda",
				tagType: "commit",
				tag:     "syslinux-3.11-pre6",
				tagger:  "hpa <hpa>",
			},
		},
	}
	for _, tc := range testCases {
		header, body, err := splitRawTag(bytes.NewReader([]byte(tc.tagContent)), tc.trimNewLine)
		assert.Equal(t, tc.header, *header)
		assert.Equal(t, tc.body, body)
		assert.NoError(t, err)
	}
}
