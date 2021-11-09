package main

import (
	"context"
	"flag"

	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/nodes"
)

const dialNodesCmdName = "dial-nodes"

func newDialNodesSubcommand(p nodes.Printer) *dialNodesSubcommand {
	return &dialNodesSubcommand{p}
}

type dialNodesSubcommand struct {
	p nodes.Printer
}

func (s *dialNodesSubcommand) FlagSet() *flag.FlagSet {
	return flag.NewFlagSet(dialNodesCmdName, flag.ExitOnError)
}

func (s *dialNodesSubcommand) Exec(flags *flag.FlagSet, conf config.Config) error {
	ctx := context.Background()
	return nodes.PingAll(ctx, conf, s.p)
}
