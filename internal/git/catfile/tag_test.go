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
)

func TestGetTag(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	cfg, objectReader, testRepo := setupObjectReader(t, ctx)

	testRepoPath := filepath.Join(cfg.Storages[0].Path, testRepo.RelativePath)

	testCases := []struct {
		tagName string
		rev     git.Revision
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
			tagID := gittest.WriteTag(t, cfg, testRepoPath, testCase.tagName, testCase.rev, gittest.WriteTagConfig{Message: testCase.message})

			tag, err := GetTag(ctx, objectReader, git.Revision(tagID), testCase.tagName, testCase.trim, true)
			require.NoError(t, err)
			if testCase.trim && len(testCase.message) >= helper.MaxCommitOrTagMessageSize {
				testCase.message = testCase.message[:helper.MaxCommitOrTagMessageSize]
			}

			require.Equal(t, testCase.message, string(tag.Message))
			require.Equal(t, testCase.tagName, string(tag.GetName()))
		})
	}
}
