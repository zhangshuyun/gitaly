package main

import (
	"context"
	"flag"
	"io"
	"os"

	"gitlab.com/gitlab-org/gitaly/v14/internal/log"
)

type subcmd interface {
	Flags(*flag.FlagSet)
	Run(ctx context.Context, stdin io.Reader, stdout io.Writer) error
}

var subcommands = map[string]subcmd{
	"create":  &createSubcommand{},
	"restore": &restoreSubcommand{},
}

func main() {
	log.Configure(log.Loggers, "json", "")

	logger := log.Default()

	flags := flag.NewFlagSet("gitaly-backup", flag.ExitOnError)
	_ = flags.Parse(os.Args)

	if flags.NArg() < 2 {
		logger.Fatal("missing subcommand")
	}

	subcmdName := flags.Arg(1)
	subcmd, ok := subcommands[subcmdName]
	if !ok {
		logger.Fatalf("unknown subcommand: %q", flags.Arg(1))
	}

	subcmdFlags := flag.NewFlagSet(subcmdName, flag.ExitOnError)
	subcmd.Flags(subcmdFlags)
	_ = subcmdFlags.Parse(flags.Args()[2:])

	if err := subcmd.Run(context.Background(), os.Stdin, os.Stdout); err != nil {
		logger.Fatalf("%s", err)
	}
}
