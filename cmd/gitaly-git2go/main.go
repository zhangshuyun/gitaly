//go:build static && system_libgit2
// +build static,system_libgit2

package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"

	git "github.com/libgit2/git2go/v32"
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
	"conflicts":   &conflictsSubcommand{},
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

	if err := flags.Parse(os.Args); err != nil {
		fatalf("parsing flags: %s", err)
	}

	if flags.NArg() < 2 {
		fatalf("missing subcommand")
	}

	subcmd, ok := subcommands[flags.Arg(1)]
	if !ok {
		fatalf("unknown subcommand: %q", flags.Arg(1))
	}

	subcmdFlags := subcmd.Flags()
	if err := subcmdFlags.Parse(flags.Args()[2:]); err != nil {
		fatalf("parsing flags of %q: %s", subcmdFlags.Name(), err)
	}

	if subcmdFlags.NArg() != 0 {
		fatalf("%s: trailing arguments", subcmdFlags.Name())
	}

	if err := git.EnableFsyncGitDir(true); err != nil {
		fatalf("enable fsync: %s", err)
	}

	if err := subcmd.Run(context.Background(), os.Stdin, os.Stdout); err != nil {
		fatalf("%s: %s", subcmdFlags.Name(), err)
	}
}
