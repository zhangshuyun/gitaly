package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"strings"

	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

type unexpectedPositionalArgsError struct{ Command string }

func (err unexpectedPositionalArgsError) Error() string {
	return fmt.Sprintf("%s doesn't accept positional arguments", err.Command)
}

type datalossSubcommand struct {
	output                     io.Writer
	virtualStorage             string
	includePartiallyReplicated bool
}

func newDatalossSubcommand() *datalossSubcommand {
	return &datalossSubcommand{output: os.Stdout}
}

func (cmd *datalossSubcommand) FlagSet() *flag.FlagSet {
	fs := flag.NewFlagSet("dataloss", flag.ContinueOnError)
	fs.StringVar(&cmd.virtualStorage, "virtual-storage", "", "virtual storage to check for data loss")
	fs.BoolVar(&cmd.includePartiallyReplicated, "partially-replicated", false, strings.TrimSpace(`
Additionally include repositories which are fully up to date on the
primary but outdated on some secondaries. Such repositories are writable
and do not suffer from data loss. The data on the primary is not fully
replicated to all secondaries which leads to increased risk of data loss
following a failover.`))
	return fs
}

func (cmd *datalossSubcommand) println(indent int, msg string, args ...interface{}) {
	fmt.Fprint(cmd.output, strings.Repeat("  ", indent))
	fmt.Fprintf(cmd.output, msg, args...)
	fmt.Fprint(cmd.output, "\n")
}

func (cmd *datalossSubcommand) Exec(flags *flag.FlagSet, cfg config.Config) error {
	if flags.NArg() > 0 {
		return unexpectedPositionalArgsError{Command: flags.Name()}
	}

	virtualStorages := []string{cmd.virtualStorage}
	if cmd.virtualStorage == "" {
		virtualStorages = make([]string, len(cfg.VirtualStorages))
		for i := range cfg.VirtualStorages {
			virtualStorages[i] = cfg.VirtualStorages[i].Name
		}
	}
	sort.Strings(virtualStorages)

	nodeAddr, err := getNodeAddress(cfg)
	if err != nil {
		return err
	}

	conn, err := subCmdDial(nodeAddr, cfg.Auth.Token)
	if err != nil {
		return fmt.Errorf("error dialing: %v", err)
	}
	defer func() {
		if err := conn.Close(); err != nil {
			log.Printf("error closing connection: %v", err)
		}
	}()

	client := gitalypb.NewPraefectInfoServiceClient(conn)

	for _, vs := range virtualStorages {
		resp, err := client.DatalossCheck(context.Background(), &gitalypb.DatalossCheckRequest{
			VirtualStorage:             vs,
			IncludePartiallyReplicated: cmd.includePartiallyReplicated,
		})
		if err != nil {
			return fmt.Errorf("error checking: %v", err)
		}

		cmd.println(0, "Virtual storage: %s", vs)
		if len(resp.Repositories) == 0 {
			msg := "All repositories are writable!"
			if cmd.includePartiallyReplicated {
				msg = "All repositories are up to date!"
			}

			cmd.println(1, msg)
			continue
		}

		cmd.println(1, "Outdated repositories:")
		for _, repo := range resp.Repositories {
			mode := "writable"
			if repo.ReadOnly {
				mode = "read-only"
			}

			cmd.println(2, "%s (%s):", repo.RelativePath, mode)

			primary := repo.Primary
			if primary == "" {
				primary = "No Primary"
			}
			cmd.println(3, "Primary: %s", primary)

			cmd.println(3, "In-Sync Storages:")
			for _, storage := range repo.Storages {
				if storage.BehindBy != 0 {
					continue
				}

				cmd.println(4, "%s%s", storage.Name, assignedMessage(storage.Assigned))
			}

			cmd.println(3, "Outdated Storages:")
			for _, storage := range repo.Storages {
				if storage.BehindBy == 0 {
					continue
				}

				plural := ""
				if storage.BehindBy > 1 {
					plural = "s"
				}

				cmd.println(4, "%s is behind by %d change%s or less%s", storage.Name, storage.BehindBy, plural, assignedMessage(storage.Assigned))
			}
		}
	}

	return nil
}

func assignedMessage(assigned bool) string {
	assignedMsg := ""
	if assigned {
		assignedMsg = ", assigned host"
	}

	return assignedMsg
}
