package catfile

import (
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

func TestGetTag(t *testing.T) {
	ctx := testhelper.Context(t)

	cfg, objectReader, testRepo := setupObjectReader(t, ctx)

	testRepoPath := filepath.Join(cfg.Storages[0].Path, testRepo.RelativePath)

	testCases := []struct {
		tagName string
		rev     git.Revision
		message string
	}{
		{
			tagName: fmt.Sprintf("%s-v1.0.2", t.Name()),
			rev:     "master^^^^",
			message: strings.Repeat("a", helper.MaxCommitOrTagMessageSize+1) + "\n",
		},
		{
			tagName: fmt.Sprintf("%s-v1.0.0", t.Name()),
			rev:     "master^^^",
			message: "Prod Release v1.0.0\n",
		},
		{
			tagName: fmt.Sprintf("%s-v1.0.1", t.Name()),
			rev:     "master^^",
			message: strings.Repeat("a", helper.MaxCommitOrTagMessageSize+1) + "\n",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.tagName, func(t *testing.T) {
			tagID := gittest.WriteTag(t, cfg, testRepoPath, testCase.tagName, testCase.rev, gittest.WriteTagConfig{Message: testCase.message})

			tag, err := GetTag(ctx, objectReader, git.Revision(tagID), testCase.tagName)
			require.NoError(t, err)
			require.Equal(t, testCase.message, string(tag.Message))
			require.Equal(t, testCase.tagName, string(tag.GetName()))
		})
	}
}

func TestTrimTag(t *testing.T) {
	for _, tc := range []struct {
		desc                string
		message             string
		expectedMessage     string
		expectedMessageSize int
	}{
		{
			desc:                "simple short message",
			message:             "foo",
			expectedMessage:     "foo",
			expectedMessageSize: 3,
		},
		{
			desc:                "trailing newlines",
			message:             "foo\n\n",
			expectedMessage:     "foo",
			expectedMessageSize: 3,
		},
		{
			desc:                "too long",
			message:             strings.Repeat("a", helper.MaxCommitOrTagMessageSize+1),
			expectedMessage:     strings.Repeat("a", helper.MaxCommitOrTagMessageSize),
			expectedMessageSize: helper.MaxCommitOrTagMessageSize + 1,
		},
		{
			desc:                "too long with newlines at cutoff",
			message:             strings.Repeat("a", helper.MaxCommitOrTagMessageSize-1) + "\nsomething",
			expectedMessage:     strings.Repeat("a", helper.MaxCommitOrTagMessageSize-1) + "\n",
			expectedMessageSize: helper.MaxCommitOrTagMessageSize - 1 + len("\nsomething"),
		},
		{
			desc:                "too long with trailing newline",
			message:             strings.Repeat("a", helper.MaxCommitOrTagMessageSize) + "foo\n",
			expectedMessage:     strings.Repeat("a", helper.MaxCommitOrTagMessageSize),
			expectedMessageSize: helper.MaxCommitOrTagMessageSize + len("foo"),
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			tag := &gitalypb.Tag{
				Message: []byte(tc.message),
			}
			TrimTagMessage(tag)
			require.Equal(t, tc.expectedMessage, string(tag.Message))
			require.Equal(t, int64(tc.expectedMessageSize), tag.MessageSize)
		})
	}
}
