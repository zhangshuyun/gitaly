package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"time"

	"github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/datastore"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/datastore/glsql"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/repocleaner"
	"gitlab.com/gitlab-org/labkit/correlation"
	"google.golang.org/grpc/metadata"
)

const (
	listUntrackedRepositoriesName = "list-untracked-repositories"
)

var errNoConnectionToGitalies = errors.New("no connection established to gitaly nodes")

type listUntrackedRepositories struct {
	logger    logrus.FieldLogger
	delimiter string
	out       io.Writer
}

func newListUntrackedRepositories(logger logrus.FieldLogger, out io.Writer) *listUntrackedRepositories {
	return &listUntrackedRepositories{logger: logger, out: out}
}

func (cmd *listUntrackedRepositories) FlagSet() *flag.FlagSet {
	fs := flag.NewFlagSet(listUntrackedRepositoriesName, flag.ExitOnError)
	fs.StringVar(&cmd.delimiter, "delimiter", "\n", "string used as a delimiter in output")
	fs.Usage = func() {
		printfErr("Description:\n" +
			"	This command checks if all repositories on all gitaly nodes tracked by praefect.\n" +
			"	If repository is found on the disk, but it is not known to praefect the location of\n" +
			"	that repository will be written into stdout stream in JSON format.\n")
		fs.PrintDefaults()
		printfErr("NOTE:\n" +
			"	All errors and log messages directed to the stderr stream.\n" +
			"	The output is produced as the new data appears, it doesn't wait\n" +
			"	for the completion of the processing to produce the result.\n")
	}
	return fs
}

func (cmd listUntrackedRepositories) Exec(flags *flag.FlagSet, cfg config.Config) error {
	if flags.NArg() > 0 {
		return unexpectedPositionalArgsError{Command: flags.Name()}
	}

	ctx := correlation.ContextWithCorrelation(context.Background(), correlation.SafeRandomID())
	ctx = metadata.AppendToOutgoingContext(ctx, "client_name", listUntrackedRepositoriesName)

	logger := cmd.logger.WithField("correlation_id", correlation.ExtractFromContext(ctx))
	logger.Debugf("starting %s command", cmd.FlagSet().Name())

	logger.Debug("dialing to gitaly nodes...")
	nodeSet, err := dialGitalyStorages(ctx, cfg, defaultDialTimeout)
	if err != nil {
		return fmt.Errorf("dial nodes: %w", err)
	}
	defer nodeSet.Close()
	logger.Debug("connected to gitaly nodes")

	logger.Debug("connecting to praefect database...")
	openDBCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	db, err := glsql.OpenDB(openDBCtx, cfg.DB)
	if err != nil {
		return fmt.Errorf("connect to database: %w", err)
	}
	defer func() { _ = db.Close() }()
	logger.Debug("connected to praefect database")

	walker := repocleaner.NewWalker(nodeSet.Connections(), 16)
	reporter := reportUntrackedRepositories{
		ctx:         ctx,
		checker:     datastore.NewStorageCleanup(db),
		delimiter:   cmd.delimiter,
		out:         cmd.out,
		printHeader: true,
	}
	for _, vs := range cfg.VirtualStorages {
		for _, node := range vs.Nodes {
			logger.Debugf("check %q/%q storage repositories", vs.Name, node.Storage)
			if err := walker.ExecOnRepositories(ctx, vs.Name, node.Storage, reporter.Report); err != nil {
				return fmt.Errorf("exec on %q/%q: %w", vs.Name, node.Storage, err)
			}
		}
	}
	logger.Debug("completed")
	return nil
}

func dialGitalyStorages(ctx context.Context, cfg config.Config, timeout time.Duration) (praefect.NodeSet, error) {
	nodeSet := praefect.NodeSet{}
	for _, vs := range cfg.VirtualStorages {
		for _, node := range vs.Nodes {
			conn, err := subCmdDial(ctx, node.Address, node.Token, timeout)
			if err != nil {
				return nil, fmt.Errorf("dial with %q gitaly at %q", node.Storage, node.Address)
			}
			if _, found := nodeSet[vs.Name]; !found {
				nodeSet[vs.Name] = map[string]praefect.Node{}
			}
			nodeSet[vs.Name][node.Storage] = praefect.Node{
				Storage:    node.Storage,
				Address:    node.Address,
				Token:      node.Token,
				Connection: conn,
			}
		}
	}
	if len(nodeSet.Connections()) == 0 {
		return nil, errNoConnectionToGitalies
	}
	return nodeSet, nil
}

type reportUntrackedRepositories struct {
	ctx         context.Context
	checker     *datastore.StorageCleanup
	out         io.Writer
	delimiter   string
	printHeader bool
}

// Report method accepts a list of repositories, checks if they exist in the praefect database
// and writes JSON serialized location of each untracked repository using the configured delimiter
// and writer.
func (r *reportUntrackedRepositories) Report(virtualStorage, storage string, replicaPaths []string) error {
	if len(replicaPaths) == 0 {
		return nil
	}

	missing, err := r.checker.DoesntExist(r.ctx, virtualStorage, storage, replicaPaths)
	if err != nil {
		return fmt.Errorf("existence check: %w", err)
	}

	if len(missing) > 0 && r.printHeader {
		if _, err := fmt.Fprintf(r.out, "The following repositories were found on disk, but missing from the tracking database:\n"); err != nil {
			return fmt.Errorf("write header to output: %w", err)
		}
		r.printHeader = false
	}

	for _, replicaPath := range missing {
		d, err := json.Marshal(map[string]string{
			"virtual_storage": virtualStorage,
			"storage":         storage,
			"relative_path":   replicaPath,
		})
		if err != nil {
			return fmt.Errorf("serialize: %w", err)
		}
		if _, err := r.out.Write(d); err != nil {
			return fmt.Errorf("write serialized data to output: %w", err)
		}
		if _, err := r.out.Write([]byte(r.delimiter)); err != nil {
			return fmt.Errorf("write serialized data to output: %w", err)
		}
	}

	return nil
}
