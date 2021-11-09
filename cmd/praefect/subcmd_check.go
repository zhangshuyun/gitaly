package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"time"

	"gitlab.com/gitlab-org/gitaly/v14/internal/helper/env"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/config"
)

const (
	skipChecksVar = "PRAEFECT_SKIP_STARTUP_CHECKS"
	checkCmdName  = "check"
)

var errFatalChecksFailed = errors.New("checks failed")

type checkSubcommand struct {
	w          io.Writer
	checkFuncs []praefect.CheckFunc
}

func newCheckSubcommand(writer io.Writer, checkFuncs ...praefect.CheckFunc) *checkSubcommand {
	return &checkSubcommand{
		w:          writer,
		checkFuncs: checkFuncs,
	}
}

func (cmd *checkSubcommand) FlagSet() *flag.FlagSet {
	fs := flag.NewFlagSet(checkCmdName, flag.ExitOnError)
	fs.Usage = func() {
		_, _ = printfErr("Description:\n" +
			"	This command runs startup checks for Praefect.")
		fs.PrintDefaults()
	}

	return fs
}

func (cmd *checkSubcommand) Exec(flags *flag.FlagSet, cfg config.Config) error {
	if skipChecks, _ := env.GetBool(skipChecksVar, false); skipChecks {
		fmt.Fprintf(cmd.w, "Skipping startup checks.\n")
		return nil
	}

	var allChecks []*praefect.Check
	for _, checkFunc := range cmd.checkFuncs {
		allChecks = append(allChecks, checkFunc(cfg))
	}

	bgContext := context.Background()
	passed := true
	var failedChecks int
	for _, check := range allChecks {
		ctx, cancel := context.WithTimeout(bgContext, 5*time.Second)
		defer cancel()

		fmt.Fprintf(cmd.w, "Checking %s...", check.Name)
		if err := check.Run(ctx); err != nil {
			failedChecks++
			if check.Severity == praefect.Fatal {
				passed = false
			}
			fmt.Fprintf(cmd.w, "Failed (%s) error: %s\n", check.Severity, err.Error())
			continue
		}
		fmt.Fprintf(cmd.w, "Passed\n")
	}

	fmt.Fprintf(cmd.w, "\n")

	if !passed {
		fmt.Fprintf(cmd.w, "%d check(s) failed, at least one was fatal.\n", failedChecks)
		return errFatalChecksFailed
	}

	if failedChecks > 0 {
		fmt.Fprintf(cmd.w, "%d check(s) failed, but none are fatal.\n", failedChecks)
	} else {
		fmt.Fprintf(cmd.w, "All checks passed.\n")
	}

	return nil
}
