package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"time"

	gitalyauth "gitlab.com/gitlab-org/gitaly/auth"
	"gitlab.com/gitlab-org/gitaly/client"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/datastore"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/datastore/glsql"
	"google.golang.org/grpc"
)

type subcmd interface {
	FlagSet() *flag.FlagSet
	Exec(flags *flag.FlagSet, config config.Config) error
}

var (
	subcommands = map[string]subcmd{
		"sql-ping":         &sqlPingSubcommand{},
		"sql-migrate":      &sqlMigrateSubcommand{},
		"dial-nodes":       &dialNodesSubcommand{},
		"reconcile":        &reconcileSubcommand{},
		"sql-migrate-down": &sqlMigrateDownSubcommand{},
	}
)

const invocationPrefix = progname + " -config CONFIG_TOML"

// subCommand returns an exit code, to be fed into os.Exit.
func subCommand(conf config.Config, arg0 string, argRest []string) int {
	interrupt := make(chan os.Signal)
	signal.Notify(interrupt, os.Interrupt)

	go func() {
		<-interrupt
		os.Exit(130) // indicates program was interrupted
	}()

	subcmd, ok := subcommands[arg0]
	if !ok {
		printfErr("%s: unknown subcommand: %q\n", progname, arg0)
		return 1
	}

	flags := subcmd.FlagSet()

	if err := flags.Parse(argRest); err != nil {
		printfErr("%s\n", err)
		return 1
	}

	if err := subcmd.Exec(flags, conf); err != nil {
		printfErr("%s\n", err)
		return 1
	}

	return 0
}

type sqlPingSubcommand struct{}

func (s *sqlPingSubcommand) FlagSet() *flag.FlagSet {
	return flag.NewFlagSet("sql-ping", flag.ExitOnError)
}

func (s *sqlPingSubcommand) Exec(flags *flag.FlagSet, conf config.Config) error {
	const subCmd = progname + " sql-ping"

	db, clean, err := openDB(conf.DB)
	if err != nil {
		return err
	}
	defer clean()

	if err := datastore.CheckPostgresVersion(db); err != nil {
		return fmt.Errorf("%s: fail: %v\n", subCmd, err)
	}

	fmt.Printf("%s: OK\n", subCmd)
	return nil
}

type sqlMigrateSubcommand struct{}

func (s *sqlMigrateSubcommand) FlagSet() *flag.FlagSet {
	return flag.NewFlagSet("sql-migrate", flag.ExitOnError)
}

func (s *sqlMigrateSubcommand) Exec(flags *flag.FlagSet, conf config.Config) error {
	const subCmd = progname + " sql-migrate"

	db, clean, err := openDB(conf.DB)
	if err != nil {
		return err
	}
	defer clean()

	n, err := glsql.Migrate(db)
	if err != nil {
		return fmt.Errorf("%s: fail: %v\n", subCmd, err)
	}

	fmt.Printf("%s: OK (applied %d migrations)\n", subCmd, n)
	return nil
}

func openDB(conf config.DB) (*sql.DB, func(), error) {
	db, err := glsql.OpenDB(conf)
	if err != nil {
		return nil, nil, fmt.Errorf("sql open: %v\n", err)
	}

	clean := func() {
		if err := db.Close(); err != nil {
			printfErr("sql close: %v\n", err)
		}
	}

	return db, clean, nil
}

func printfErr(format string, a ...interface{}) (int, error) {
	return fmt.Fprintf(os.Stderr, format, a...)
}

func subCmdDial(addr, token string, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	ctx, cancel := context.WithTimeout(context.TODO(), 30*time.Second)
	defer cancel()

	opts = append(opts,
		grpc.WithBlock(),
	)

	if len(token) > 0 {
		opts = append(opts,
			grpc.WithPerRPCCredentials(
				gitalyauth.RPCCredentialsV2(token),
			),
		)
	}

	return client.DialContext(ctx, addr, opts)
}
