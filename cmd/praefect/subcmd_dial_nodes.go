package main

import (
	"context"
	"flag"
	"io"

	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/nodes"
)

const dialNodesCmdName = "dial-nodes"

func newDialNodesSubcommand(w io.Writer) *dialNodesSubcommand {
	return &dialNodesSubcommand{
		w: w,
	}
}

type dialNodesSubcommand struct {
	w io.Writer
}

func (s *dialNodesSubcommand) FlagSet() *flag.FlagSet {
	return flag.NewFlagSet(dialNodesCmdName, flag.ExitOnError)
}

func (s *dialNodesSubcommand) Exec(flags *flag.FlagSet, conf config.Config) error {
	ctx := context.Background()
	return nodes.PingAll(ctx, conf, nodes.NewTextPrinter(s.w), false)
}
