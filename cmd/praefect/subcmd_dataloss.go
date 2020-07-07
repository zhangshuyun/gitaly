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

	"gitlab.com/gitlab-org/gitaly/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

type datalossSubcommand struct {
	output         io.Writer
	virtualStorage string
}

func newDatalossSubcommand() *datalossSubcommand {
	return &datalossSubcommand{output: os.Stdout}
}

func (cmd *datalossSubcommand) FlagSet() *flag.FlagSet {
	fs := flag.NewFlagSet("dataloss", flag.ContinueOnError)
	fs.StringVar(&cmd.virtualStorage, "virtual-storage", "", "virtual storage to check for data loss")
	return fs
}

func (cmd *datalossSubcommand) println(indent int, msg string, args ...interface{}) {
	fmt.Fprint(cmd.output, strings.Repeat("  ", indent))
	fmt.Fprintf(cmd.output, msg, args...)
	fmt.Fprint(cmd.output, "\n")
}

func (cmd *datalossSubcommand) Exec(flags *flag.FlagSet, cfg config.Config) error {
	if flags.NArg() > 0 {
		return UnexpectedPositionalArgsError{Command: flags.Name()}
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
			VirtualStorage: vs,
		})
		if err != nil {
			return fmt.Errorf("error checking: %v", err)
		}

		cmd.println(0, "Virtual storage: %s", vs)
		cmd.println(1, "Primary: %s", resp.Primary)
		if len(resp.Repositories) == 0 {
			cmd.println(1, "All repositories are consistent!")
			return nil
		}

		cmd.println(1, "Outdated repositories:")
		for _, r := range resp.Repositories {
			cmd.println(2, "%s:", r.RelativePath)
			for _, s := range r.Storages {
				plural := ""
				if s.BehindBy > 1 {
					plural = "s"
				}

				cmd.println(3, "%s is behind by %d generation%s or less", s.Name, s.BehindBy, plural)
			}
		}
	}

	return nil
}
