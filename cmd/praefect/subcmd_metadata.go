package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"

	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

const metadataCmdName = "metadata"

type metadataSubcommand struct {
	stdout         io.Writer
	repositoryID   int64
	virtualStorage string
	relativePath   string
}

func newMetadataSubcommand(stdout io.Writer) *metadataSubcommand {
	return &metadataSubcommand{stdout: stdout}
}

func (cmd *metadataSubcommand) FlagSet() *flag.FlagSet {
	fs := flag.NewFlagSet(metadataCmdName, flag.ContinueOnError)
	fs.Int64Var(&cmd.repositoryID, "repository-id", 0, "the repository's ID")
	fs.StringVar(&cmd.virtualStorage, "virtual-storage", "", "the repository's virtual storage")
	fs.StringVar(&cmd.relativePath, "relative-path", "", "the repository's relative path in the virtual storage")
	return fs
}

func (cmd *metadataSubcommand) println(format string, args ...interface{}) {
	fmt.Fprintf(cmd.stdout, format+"\n", args...)
}

func (cmd *metadataSubcommand) Exec(flags *flag.FlagSet, cfg config.Config) error {
	if flags.NArg() > 0 {
		return unexpectedPositionalArgsError{Command: flags.Name()}
	}

	var request gitalypb.GetRepositoryMetadataRequest
	switch {
	case cmd.repositoryID != 0:
		if cmd.virtualStorage != "" || cmd.relativePath != "" {
			return errors.New("virtual storage and relative path can't be provided with a repository ID")
		}
		request.Query = &gitalypb.GetRepositoryMetadataRequest_RepositoryId{RepositoryId: cmd.repositoryID}
	case cmd.virtualStorage != "" || cmd.relativePath != "":
		if cmd.virtualStorage == "" {
			return errors.New("virtual storage is required with relative path")
		} else if cmd.relativePath == "" {
			return errors.New("relative path is required with virtual storage")
		}
		request.Query = &gitalypb.GetRepositoryMetadataRequest_Path_{
			Path: &gitalypb.GetRepositoryMetadataRequest_Path{
				VirtualStorage: cmd.virtualStorage,
				RelativePath:   cmd.relativePath,
			},
		}
	default:
		return errors.New("repository id or virtual storage and relative path required")
	}

	nodeAddr, err := getNodeAddress(cfg)
	if err != nil {
		return fmt.Errorf("get node address: %w", err)
	}

	ctx := context.TODO()
	conn, err := subCmdDial(ctx, nodeAddr, cfg.Auth.Token, defaultDialTimeout)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()

	metadata, err := gitalypb.NewPraefectInfoServiceClient(conn).GetRepositoryMetadata(ctx, &request)
	if err != nil {
		return fmt.Errorf("get metadata: %w", err)
	}

	cmd.println("Repository ID: %d", metadata.RepositoryId)
	cmd.println("Virtual Storage: %q", metadata.VirtualStorage)
	cmd.println("Relative Path: %q", metadata.RelativePath)
	cmd.println("Replica Path: %q", metadata.ReplicaPath)
	cmd.println("Primary: %q", metadata.Primary)
	cmd.println("Generation: %d", metadata.Generation)
	cmd.println("Replicas:")
	for _, replica := range metadata.Replicas {
		cmd.println("- Storage: %q", replica.Storage)
		cmd.println("  Assigned: %v", replica.Assigned)

		generationText := fmt.Sprintf("%d, fully up to date", replica.Generation)
		if replica.Generation == -1 {
			generationText = "replica not yet created"
		} else if replica.Generation < metadata.Generation {
			generationText = fmt.Sprintf("%d, behind by %d changes", replica.Generation, metadata.Generation-replica.Generation)
		}

		cmd.println("  Generation: %s", generationText)
		cmd.println("  Healthy: %v", replica.Healthy)
		cmd.println("  Valid Primary: %v", replica.ValidPrimary)
	}
	return nil
}
