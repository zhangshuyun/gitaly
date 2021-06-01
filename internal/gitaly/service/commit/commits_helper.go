package commit

import (
	"context"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/log"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper/chunk"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

func (s *server) sendCommits(
	ctx context.Context,
	sender chunk.Sender,
	repo git.RepositoryExecutor,
	revisionRange []string,
	paths []string,
	options *gitalypb.GlobalOptions,
	extraArgs ...git.Option,
) error {
	revisions := make([]git.Revision, len(revisionRange))
	for i, revision := range revisionRange {
		revisions[i] = git.Revision(revision)
	}

	cmd, err := log.GitLogCommand(ctx, s.gitCmdFactory, repo, revisions, paths, options, extraArgs...)
	if err != nil {
		return err
	}

	logParser, err := log.NewParser(ctx, s.catfileCache, repo, cmd)
	if err != nil {
		return err
	}

	chunker := chunk.New(sender)
	for logParser.Parse(ctx) {
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
