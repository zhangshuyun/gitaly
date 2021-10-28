package gittest

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper/text"
)

// WriteTagConfig holds extra options for WriteTag.
type WriteTagConfig struct {
	// Message is the message of an annotated tag. If left empty, then a lightweight tag will
	// be created.
	Message string
	// Force indicates whether existing tags with the same name shall be overwritten.
	Force bool
}

// WriteTag writes a new tag into the repository. This function either returns the tag ID in case
// an annotated tag was created, or otherwise the target object ID when a lightweight tag was
// created. Takes either no WriteTagConfig, in which case the default values will be used, or
// exactly one.
func WriteTag(
	t testing.TB,
	cfg config.Cfg,
	repoPath string,
	tagName string,
	targetRevision git.Revision,
	optionalConfig ...WriteTagConfig,
) git.ObjectID {
	require.Less(t, len(optionalConfig), 2, "only a single config may be passed")

	var config WriteTagConfig
	if len(optionalConfig) == 1 {
		config = optionalConfig[0]
	}

	committerName := "Scrooge McDuck"
	committerEmail := "scrooge@mcduck.com"

	args := []string{
		"-C", repoPath,
		"-c", fmt.Sprintf("user.name=%s", committerName),
		"-c", fmt.Sprintf("user.email=%s", committerEmail),
		"tag",
	}

	if config.Force {
		args = append(args, "-f")
	}

	// The message can be very large, passing it directly in args would blow things up.
	stdin := bytes.NewBufferString(config.Message)
	if config.Message != "" {
		args = append(args, "-F", "-")
	}
	args = append(args, tagName, targetRevision.String())

	ExecOpts(t, cfg, ExecConfig{Stdin: stdin}, args...)

	tagID := Exec(t, cfg, "-C", repoPath, "show-ref", "-s", tagName)

	objectID, err := git.NewObjectIDFromHex(text.ChompBytes(tagID))
	require.NoError(t, err)

	return objectID
}
