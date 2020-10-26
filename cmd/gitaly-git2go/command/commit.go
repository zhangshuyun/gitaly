// +build static,system_libgit2

package command

import (
	"context"
	"flag"
	"io"

	"gitlab.com/gitlab-org/gitaly/cmd/gitaly-git2go/command/commit"
)

type commitSubcommand struct{}

func (commitSubcommand) Flags() *flag.FlagSet { return flag.NewFlagSet("commit", flag.ExitOnError) }

func (commitSubcommand) Run(ctx context.Context, stdin io.Reader, stdout io.Writer) error {
	return commit.Run(ctx, stdin, stdout)
}
