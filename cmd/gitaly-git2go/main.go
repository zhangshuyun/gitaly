//go:build static && system_libgit2
// +build static,system_libgit2

package main

import (
	"context"
	"encoding/gob"
	"flag"
	"fmt"
	"os"

	git "github.com/libgit2/git2go/v33"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git2go"
)

type subcmd interface {
	Flags() *flag.FlagSet
	Run(ctx context.Context, decoder *gob.Decoder, encoder *gob.Encoder) error
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

func fatalf(encoder *gob.Encoder, format string, args ...interface{}) {
	// Once logging has been implemented these encoding errors can be logged.
	// Until then these will be ignored as we can no longer use stderr.
	// https://gitlab.com/gitlab-org/gitaly/-/issues/3229
	_ = encoder.Encode(git2go.Result{
		Err: git2go.SerializableError(fmt.Errorf(format, args...)),
	})
	// An exit code of 1 would indicate an error over stderr. Since our errors
	// are encoded over gob, we need to exit cleanly
	os.Exit(0)
}

func main() {
	decoder := gob.NewDecoder(os.Stdin)
	encoder := gob.NewEncoder(os.Stdout)

	flags := flag.NewFlagSet(git2go.BinaryName, flag.ContinueOnError)

	if err := flags.Parse(os.Args); err != nil {
		fatalf(encoder, "parsing flags: %s", err)
	}

	if flags.NArg() < 2 {
		fatalf(encoder, "missing subcommand")
	}

	subcmd, ok := subcommands[flags.Arg(1)]
	if !ok {
		fatalf(encoder, "unknown subcommand: %q", flags.Arg(0))
	}

	subcmdFlags := subcmd.Flags()
	if err := subcmdFlags.Parse(flags.Args()[2:]); err != nil {
		fatalf(encoder, "parsing flags of %q: %s", subcmdFlags.Name(), err)
	}

	if subcmdFlags.NArg() != 0 {
		fatalf(encoder, "%s: trailing arguments", subcmdFlags.Name())
	}

	if err := git.EnableFsyncGitDir(true); err != nil {
		fatalf(encoder, "enable fsync: %s", err)
	}

	if err := subcmd.Run(context.Background(), decoder, encoder); err != nil {
		fatalf(encoder, "%s: %s", subcmdFlags.Name(), err)
	}
}
