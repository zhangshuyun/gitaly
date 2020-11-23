package git

import (
	"bytes"
	"context"
	"io"
	"time"
	"fmt"

	"gitlab.com/gitlab-org/gitaly/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
)


// MkTag writes a tag with git-mktag. The code is mostly stolen from
// git::WriteBlob()
func (repo *LocalRepository) MkTag(ctx context.Context, oid string, objectType string, tag string, tagger string, message []byte) (string, error) {
	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	now := time.Now()
	secs := now.Unix();
	secs_str := fmt.Sprintf("%d", secs)
	buf := "object " + oid + "\n"
	buf += "type " + objectType + "\n"
	buf += "tag " + tag + "\n"
	buf += "tagger " + tagger + " " + secs_str + " +0000\n"
	buf += "\n"
	buf += string(message)
	
	content := io.Reader(bytes.NewReader([]byte(buf)))

	cmd, err := repo.command(ctx, nil,
		SubCmd{
			Name: "mktag",
		},
		WithStdin(content),
		WithStdout(stdout),
		WithStderr(stderr),
		WithRefTxHook(ctx, helper.ProtoRepoFromRepo(repo.repo), config.Config),
	)
	if err != nil {
		return "", err
	}

	if err := cmd.Wait(); err != nil {
		return "", errorWithStderr(err, stderr.Bytes())
	}

	return text.ChompBytes(stdout.Bytes()), nil
}
