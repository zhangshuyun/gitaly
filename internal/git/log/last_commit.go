package log

import (
	"context"
	"io/ioutil"

	"gitlab.com/gitlab-org/gitaly/internal/command"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/internal/git/repository"
	"gitlab.com/gitlab-org/gitaly/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

// LastCommitForPath returns the last commit which modified path.
func LastCommitForPath(ctx context.Context, gitCmdFactory git.CommandFactory, batch catfile.Batch, repo repository.GitRepo, revision git.Revision, path string, options *gitalypb.GlobalOptions) (*gitalypb.GitCommit, error) {
	cmd, err := gitCmdFactory.New(ctx, repo, git.SubCmd{
		Name:        "log",
		Flags:       []git.Option{git.Flag{Name: "--format=%H"}, git.Flag{Name: "--max-count=1"}},
		Args:        []string{revision.String()},
		PostSepArgs: []string{path},
	}, git.ConvertGlobalOptions(options)...)
	if err != nil {
		return nil, err
	}

	commitID, err := ioutil.ReadAll(cmd)
	if err != nil {
		return nil, err
	}

	return GetCommitCatfile(ctx, batch, git.Revision(text.ChompBytes(commitID)))
}

// GitLogCommand returns a Command that executes git log with the given the arguments
func GitLogCommand(ctx context.Context, gitCmdFactory git.CommandFactory, repo repository.GitRepo, revisions []git.Revision, paths []string, options *gitalypb.GlobalOptions, extraArgs ...git.Option) (*command.Command, error) {
	args := make([]string, len(revisions))
	for i, revision := range revisions {
		args[i] = revision.String()
	}

	return gitCmdFactory.New(ctx, repo, git.SubCmd{
		Name:        "log",
		Flags:       append([]git.Option{git.Flag{Name: "--pretty=%H"}}, extraArgs...),
		Args:        args,
		PostSepArgs: paths,
	}, git.ConvertGlobalOptions(options)...)
}
