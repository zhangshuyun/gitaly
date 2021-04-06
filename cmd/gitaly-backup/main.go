package main

import (
	"context"
	"flag"
	"io"
	"os"

	log "github.com/sirupsen/logrus"
	glog "gitlab.com/gitlab-org/gitaly/internal/log"
)

type subcmd interface {
	Flags() *flag.FlagSet
	Run(ctx context.Context, stdin io.Reader, stdout io.Writer) error
}

var subcommands = map[string]subcmd{
	"create": &createSubcommand{},
}

func main() {
	glog.Configure(glog.Loggers, "", "")

	flags := flag.NewFlagSet("gitaly-backup", flag.ExitOnError)
	_ = flags.Parse(os.Args)

	if flags.NArg() < 2 {
		log.Fatal("missing subcommand")
	}

	subcmd, ok := subcommands[flags.Arg(1)]
	if !ok {
		log.Fatalf("unknown subcommand: %q", flags.Arg(1))
	}

	subcmdFlags := subcmd.Flags()
	_ = subcmdFlags.Parse(flags.Args()[2:])

	if err := subcmd.Run(context.Background(), os.Stdin, os.Stdout); err != nil {
		log.Fatalf("%s", err)
	}
}
