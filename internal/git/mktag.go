package git

import (
	"context"

	"gitlab.com/gitlab-org/gitaly/internal/command"
)


// MkTag writes a tag with git-mktag. The code is mostly stolen from
// git::WriteBlob()
func (repo *LocalRepository) MkTag(ctx context.Context, oid string, objectType string, tag string, tagger string) (string, error) {
	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	content "hello world"

	cmd, err := repo.command(ctx, nil,
		SubCmd{
			Name: "mktag",
		},
		WithStdin(content),
		WithStdout(stdout),
		WithStderr(stderr),
	)
	if err != nil {
		return "", err
	}

	if err := cmd.Wait(); err != nil {
		return "", errorWithStderr(err, stderr.Bytes())
	}

	return text.ChompBytes(stdout.Bytes()), nil
}
