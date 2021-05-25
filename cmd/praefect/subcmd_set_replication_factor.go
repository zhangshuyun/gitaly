package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"strings"

	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

const paramReplicationFactor = "replication-factor"

type setReplicationFactorSubcommand struct {
	stdout            io.Writer
	virtualStorage    string
	relativePath      string
	replicationFactor int
}

func newSetReplicatioFactorSubcommand(stdout io.Writer) *setReplicationFactorSubcommand {
	return &setReplicationFactorSubcommand{stdout: stdout}
}

func (cmd *setReplicationFactorSubcommand) FlagSet() *flag.FlagSet {
	fs := flag.NewFlagSet("set-replication-factor", flag.ContinueOnError)
	fs.StringVar(&cmd.virtualStorage, paramVirtualStorage, "", "name of the repository's virtual storage")
	fs.StringVar(&cmd.relativePath, paramRelativePath, "", "repository to set the replication factor for")
	fs.IntVar(&cmd.replicationFactor, paramReplicationFactor, -1, "desired replication factor")
	return fs
}

func (cmd *setReplicationFactorSubcommand) Exec(flags *flag.FlagSet, cfg config.Config) error {
	if flags.NArg() > 0 {
		return unexpectedPositionalArgsError{Command: flags.Name()}
	} else if cmd.virtualStorage == "" {
		return requiredParameterError(paramVirtualStorage)
	} else if cmd.relativePath == "" {
		return requiredParameterError(paramRelativePath)
	} else if cmd.replicationFactor < 0 {
		return requiredParameterError(paramReplicationFactor)
	}

	nodeAddr, err := getNodeAddress(cfg)
	if err != nil {
		return err
	}

	conn, err := subCmdDial(nodeAddr, cfg.Auth.Token)
	if err != nil {
		return fmt.Errorf("error dialing: %w", err)
	}
	defer conn.Close()

	client := gitalypb.NewPraefectInfoServiceClient(conn)
	resp, err := client.SetReplicationFactor(context.TODO(), &gitalypb.SetReplicationFactorRequest{
		VirtualStorage:    cmd.virtualStorage,
		RelativePath:      cmd.relativePath,
		ReplicationFactor: int32(cmd.replicationFactor),
	})
	if err != nil {
		return err
	}

	fmt.Fprintf(cmd.stdout, "current assignments: %v\n", strings.Join(resp.Storages, ", "))

	return nil
}
