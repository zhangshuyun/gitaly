package commit

import (
	"context"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/log"
	"gitlab.com/gitlab-org/gitaly/internal/helper/chunk"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

func sendCommits(ctx context.Context, sender chunk.Sender, repo *gitalypb.Repository, revisionRange []string, paths []string, extraArgs ...git.Option) error {
	cmd, err := log.GitLogCommand(ctx, repo, revisionRange, paths, extraArgs...)
	if err != nil {
		return err
	}

	logParser, err := log.NewLogParser(ctx, repo, cmd)
	if err != nil {
		return err
	}

	chunker := chunk.New(sender)
	for logParser.Parse() {
		if err := chunker.Send(logParser.Commit()); err != nil {
			return err
		}
	}

	if err := logParser.Err(); err != nil {
		return err
	}

	if err := chunker.Flush(); err != nil {
		return err
	}

	if err := cmd.Wait(); err != nil {
		// We expect this error to be caused by non-existing references. In that
		// case, we just log the error and send no commits to the `sender`.
		ctxlogrus.Extract(ctx).WithError(err).Info("ignoring git-log error")
	}

	return nil
}
