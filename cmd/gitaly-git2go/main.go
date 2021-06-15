// +build static,system_libgit2

package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"

	"gitlab.com/gitlab-org/gitaly/v14/cmd/gitaly-git2go/conflicts"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git2go"
)

type subcmd interface {
	Flags() *flag.FlagSet
	Run(ctx context.Context, stdin io.Reader, stdout io.Writer) error
}

var subcommands = map[string]subcmd{
	"apply":       &applySubcommand{},
	"cherry-pick": &cherryPickSubcommand{},
	"commit":      commitSubcommand{},
	"conflicts":   &conflicts.Subcommand{},
	"merge":       &mergeSubcommand{},
	"rebase":      &rebaseSubcommand{},
	"revert":      &revertSubcommand{},
	"resolve":     &resolveSubcommand{},
	"submodule":   &submoduleSubcommand{},
}

func fatalf(format string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}

func main() {
	flags := flag.NewFlagSet(git2go.BinaryName, flag.ExitOnError)
	flags.Parse(os.Args)

	if flags.NArg() < 2 {
		fatalf("missing subcommand")
	}

	subcmd, ok := subcommands[flags.Arg(1)]
	if !ok {
		fatalf("unknown subcommand: %q", flags.Arg(1))
	}

	subcmdFlags := subcmd.Flags()
	subcmdFlags.Parse(flags.Args()[2:])

	if subcmdFlags.NArg() != 0 {
		fatalf("%s: trailing arguments", subcmdFlags.Name())
	}

	if err := subcmd.Run(context.Background(), os.Stdin, os.Stdout); err != nil {
		fatalf("%s: %s", subcmdFlags.Name(), err)
	}
}
