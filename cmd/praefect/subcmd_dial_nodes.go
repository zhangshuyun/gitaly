package main

import (
	"context"
	"flag"
	"io"
	"time"

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
	w       io.Writer
	timeout time.Duration
}

func (s *dialNodesSubcommand) FlagSet() *flag.FlagSet {
	fs := flag.NewFlagSet(dialNodesCmdName, flag.ExitOnError)
	fs.DurationVar(&s.timeout, "timeout", 0, "timeout for dialing gitaly nodes")
	fs.Usage = func() {
		printfErr("Description:\n" +
			"	This command attempts to reach all Gitaly nodes.\n")
		fs.PrintDefaults()
	}

	return fs
}

func (s *dialNodesSubcommand) Exec(flags *flag.FlagSet, conf config.Config) error {
	if s.timeout == 0 {
		s.timeout = defaultDialTimeout
	}

	ctx, cancel := context.WithTimeout(context.Background(), s.timeout)
	defer cancel()

	return nodes.PingAll(ctx, conf, nodes.NewTextPrinter(s.w), false)
}
