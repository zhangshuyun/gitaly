package log

import (
	"context"
	"io/ioutil"

	"gitlab.com/gitlab-org/gitaly/internal/command"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

// LastCommitForPath returns the last commit which modified path.
func LastCommitForPath(ctx context.Context, batch catfile.Batch, repo *gitalypb.Repository, revision git.Revision, path string, options *gitalypb.GlobalOptions) (*gitalypb.GitCommit, error) {
	cmd, err := git.NewCommand(ctx, repo, git.ConvertGlobalOptions(options), git.SubCmd{
		Name:        "log",
		Flags:       []git.Option{git.Flag{Name: "--format=%H"}, git.Flag{Name: "--max-count=1"}},
		Args:        []string{revision.String()},
		PostSepArgs: []string{path}})
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
func GitLogCommand(ctx context.Context, factory git.CommandFactory, repo *gitalypb.Repository, revisions []git.Revision, paths []string, options *gitalypb.GlobalOptions, extraArgs ...git.Option) (*command.Command, error) {
	args := make([]string, len(revisions))
	for i, revision := range revisions {
		args[i] = revision.String()
	}

	return git.NewCommand(ctx, repo, git.ConvertGlobalOptions(options), git.SubCmd{
		Name:        "log",
		Flags:       append([]git.Option{git.Flag{Name: "--pretty=%H"}}, extraArgs...),
		Args:        args,
		PostSepArgs: paths,
	})
}
