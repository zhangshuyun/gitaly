// +build static,system_libgit2

package main

import (
	"context"
	"flag"
	"io"

	"gitlab.com/gitlab-org/gitaly/v14/cmd/gitaly-git2go/commit"
)

type commitSubcommand struct{}

func (commitSubcommand) Flags() *flag.FlagSet { return flag.NewFlagSet("commit", flag.ExitOnError) }

func (commitSubcommand) Run(ctx context.Context, stdin io.Reader, stdout io.Writer) error {
	return commit.Run(ctx, stdin, stdout)
}
