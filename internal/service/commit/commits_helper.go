package commit

import (
	"bytes"
	"context"
	"strings"

	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/helper/lines"

	pb "gitlab.com/gitlab-org/gitaly-proto/go"

	log "github.com/Sirupsen/logrus"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

type commitsSender interface {
	Send([]*pb.GitCommit) error
}

var commitLogFormatFields = []string{
	"%H",  // commit hash
	"%s",  // subject
	"%B",  // raw body (subject + body)
	"%an", // author name
	"%ae", // author email
	"%aI", // author date, strict ISO 8601 format
	"%cn", // committer name
	"%ce", // committer email
	"%cI", // committer date, strict ISO 8601 format
	"%P",  // parent hashes
}

const (
	fieldDelimiter                = "\x1f"
	fieldDelimiterGitFormatString = "%x1f"
)

func gitLog(ctx context.Context, sender lines.Sender, repo *pb.Repository, revisions []string, paths []string, extraOptions ...string) error {
	grpc_logrus.Extract(ctx).WithFields(log.Fields{
		"Revision Range": revisions,
	}).Debug("GitLog")

	repoPath, err := helper.GetRepoPath(repo)
	if err != nil {
		return err
	}

	formatFlag := "--pretty=format:" + strings.Join(commitLogFormatFields, fieldDelimiterGitFormatString)

	args := []string{
		"--git-dir",
		repoPath,
		"log",
		"-z", // use 0x00 as the entry terminator (instead of \n)
		formatFlag,
	}

	args = append(args, extraOptions...)
	args = append(args, revisions...)
	args = append(args, "--")
	args = append(args, paths...)

	cmd, err := helper.GitCommandReader(ctx, args...)
	if err != nil {
		return err
	}
	defer cmd.Kill(ctx)

	split := lines.ScanWithDelimiter([]byte("\x00"))
	if err := lines.Send(cmd, sender, split); err != nil {
		return err
	}

	if err := cmd.Wait(); err != nil {
		// We expect this error to be caused by non-existing references. In that
		// case, we just log the error and send no commits to the `sender`.
		grpc_logrus.Extract(ctx).WithError(err).Info("ignoring git-log error")
	}

	return nil
}

func newCommitsWriter(sender commitsSender) lines.Sender {
	return func(refs [][]byte) error {
		var commits []*pb.GitCommit

		for _, ref := range refs {
			elements := bytes.Split(ref, []byte(fieldDelimiter))
			if len(elements) != 10 {
				return grpc.Errorf(codes.Internal, "error parsing ref %q", ref)
			}
			var parentIds []string
			if len(elements[9]) > 0 { // Any parents?
				parentIds = strings.Split(string(elements[9]), " ")
			}

			commit, err := git.NewCommit(elements[0], elements[1], elements[2],
				elements[3], elements[4], elements[5], elements[6], elements[7],
				elements[8], parentIds...)
			if err != nil {
				return err
			}

			commits = append(commits, commit)
		}

		return sender.Send(commits)
	}
}
