//go:build static && system_libgit2
// +build static,system_libgit2

package main

import (
	"context"
	"encoding/gob"
	"flag"

	"gitlab.com/gitlab-org/gitaly/v14/cmd/gitaly-git2go/commit"
)

type commitSubcommand struct{}

func (commitSubcommand) Flags() *flag.FlagSet { return flag.NewFlagSet("commit", flag.ExitOnError) }

func (commitSubcommand) Run(ctx context.Context, decoder *gob.Decoder, encoder *gob.Encoder) error {
	return commit.Run(ctx, decoder, encoder)
}
