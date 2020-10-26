// +build static,system_libgit2

package command

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
)

// Process encapsulates process global state.
type Process struct {
	Args   []string
	Stdin  io.Reader
	Stdout io.Writer
	Stderr io.Writer
}

type subcmd interface {
	Flags() *flag.FlagSet
	Run(ctx context.Context, stdin io.Reader, stdout io.Writer) error
}

var subcommands = map[string]subcmd{
	"conflicts": &conflictsSubcommand{},
	"commit":    commitSubcommand{},
	"merge":     &mergeSubcommand{},
	"revert":    &revertSubcommand{},
}

const programName = "gitaly-git2go"

func Main(p Process) int {
	if err := main(p); err != nil {
		fmt.Fprintf(p.Stderr, err.Error()+"\n")
		return 1
	}

	return 0
}

func main(p Process) error {
	flags := flag.NewFlagSet(programName, flag.ExitOnError)
	flags.Parse(p.Args)

	if flags.NArg() < 2 {
		return errors.New("missing subcommand")
	}

	subcmd, ok := subcommands[flags.Arg(1)]
	if !ok {
		return fmt.Errorf("unknown subcommand: %q", flags.Arg(1))
	}

	subcmdFlags := subcmd.Flags()
	subcmdFlags.Parse(flags.Args()[2:])

	if subcmdFlags.NArg() != 0 {
		return fmt.Errorf("%s: trailing arguments", subcmdFlags.Name())
	}

	if err := subcmd.Run(context.Background(), p.Stdin, p.Stdout); err != nil {
		return fmt.Errorf("%s: %s", subcmdFlags.Name(), err)
	}

	return nil
}
