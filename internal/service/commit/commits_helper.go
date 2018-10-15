package commit

import (
	"context"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus"
	"gitlab.com/gitlab-org/gitaly-proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/internal/git/log"
)

type commitsSender interface {
	Send([]*gitalypb.GitCommit) error
}

const commitsPerChunk = 20

func sendCommits(ctx context.Context, sender commitsSender, repo *gitalypb.Repository, revisionRange []string, paths []string, extraArgs ...string) error {
	cmd, err := log.GitLogCommand(ctx, repo, revisionRange, paths, extraArgs...)
	if err != nil {
		return err
	}

	logParser, err := log.NewLogParser(ctx, repo, cmd)
	if err != nil {
		return err
	}

	var commits []*gitalypb.GitCommit

	for logParser.Parse() {
		commit := logParser.Commit()

		if len(commits) >= commitsPerChunk {
			if err := sender.Send(commits); err != nil {
				return err
			}
			commits = nil
		}

		commits = append(commits, commit)
	}

	if err := logParser.Err(); err != nil {
		return err
	}

	if err := sender.Send(commits); err != nil {
		return err
	}

	if err := cmd.Wait(); err != nil {
		// We expect this error to be caused by non-existing references. In that
		// case, we just log the error and send no commits to the `sender`.
		grpc_logrus.Extract(ctx).WithError(err).Info("ignoring git-log error")
	}

	return nil
}
