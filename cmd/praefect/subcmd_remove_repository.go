package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/datastore"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/datastore/glsql"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"gitlab.com/gitlab-org/labkit/correlation"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const (
	removeRepositoryCmdName = "remove-repository"
	paramApply              = "apply"
)

type writer struct {
	m sync.Mutex
	w io.Writer
}

func (w *writer) Write(b []byte) (int, error) {
	w.m.Lock()
	defer w.m.Unlock()
	return w.w.Write(b)
}

type removeRepository struct {
	logger         logrus.FieldLogger
	virtualStorage string
	relativePath   string
	apply          bool
	dialTimeout    time.Duration
	w              io.Writer
}

func newRemoveRepository(logger logrus.FieldLogger, w io.Writer) *removeRepository {
	return &removeRepository{logger: logger, w: &writer{w: w}, dialTimeout: defaultDialTimeout}
}

func (cmd *removeRepository) FlagSet() *flag.FlagSet {
	fs := flag.NewFlagSet(removeRepositoryCmdName, flag.ExitOnError)
	fs.StringVar(&cmd.virtualStorage, paramVirtualStorage, "", "name of the repository's virtual storage")
	fs.BoolVar(&cmd.apply, paramApply, false, "physically remove the repository from disk and the database")
	fs.StringVar(&cmd.relativePath, paramRelativePath, "", "relative path to the repository")
	fs.Usage = func() {
		printfErr("Description:\n" +
			"	This command removes all state associated with a given repository from the Gitaly Cluster.\n" +
			"	This includes both on-disk repositories on all relevant Gitaly nodes as well as any potential\n" +
			"	database state as tracked by Praefect.\n" +
			"	Runs in dry-run mode by default and lists the gitaly nodes from which the repository will be removed from " +
			"	without actually removing it from the database and disk.\n" +
			"	When -apply is used, the repository will be removed from the database as well as\n" +
			"	the individual gitaly nodes on which they exist.\n")
		fs.PrintDefaults()
		printfErr("NOTE:\n" +
			"	It may happen that parts of the repository continue to exist after this command, either because\n" +
			"	of an error that happened during deletion or because of in-flight RPC calls targeting the repository.\n" +
			"	It is safe and recommended to re-run this command in such a case.\n")
	}
	return fs
}

func (cmd removeRepository) Exec(flags *flag.FlagSet, cfg config.Config) error {
	switch {
	case flags.NArg() > 0:
		return unexpectedPositionalArgsError{Command: flags.Name()}
	case cmd.virtualStorage == "":
		return requiredParameterError(paramVirtualStorage)
	case cmd.relativePath == "":
		return requiredParameterError(paramRelativePath)
	}
	ctx := context.Background()

	openDBCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	db, err := glsql.OpenDB(openDBCtx, cfg.DB)
	if err != nil {
		return fmt.Errorf("connect to database: %w", err)
	}
	defer func() { _ = db.Close() }()

	ctx = correlation.ContextWithCorrelation(ctx, correlation.SafeRandomID())
	logger := cmd.logger.WithField("correlation_id", correlation.ExtractFromContext(ctx))

	return cmd.exec(ctx, logger, db, cfg)
}

func (cmd *removeRepository) exec(ctx context.Context, logger logrus.FieldLogger, db *sql.DB, cfg config.Config) error {
	// Remove repository explicitly from all storages and clean up database info.
	// This prevents creation of the new replication events.
	logger.WithFields(logrus.Fields{
		"virtual_storage": cmd.virtualStorage,
		"relative_path":   cmd.relativePath,
	}).Debug("remove repository")

	rs := datastore.NewPostgresRepositoryStore(db, cfg.StorageNames())
	exists, err := rs.RepositoryExists(ctx, cmd.virtualStorage, cmd.relativePath)
	if err != nil {
		fmt.Fprintf(cmd.w, "error getting storages from database: %v. Please make sure the database"+
			" parameters are set correctly and the database is accessible.\n", err)
	} else {
		if exists {
			fmt.Fprintln(cmd.w, "Repository found in the database.")
		} else {
			fmt.Fprintln(cmd.w, "Repository is not being tracked in Praefect.")
		}
	}

	storagesOnDisk, err := cmd.getStoragesFromNodes(ctx, cfg)
	if err != nil {
		return err
	}

	if len(storagesOnDisk) == 0 {
		fmt.Fprintln(cmd.w, "Repository not found on any gitaly nodes.")
	} else {
		fmt.Fprintf(cmd.w, "Repository found on the following gitaly nodes: %s.\n", strings.Join(storagesOnDisk, ", "))
	}

	if !cmd.apply {
		fmt.Fprintln(cmd.w, "Re-run the command with -apply to remove repositories from the database and disk.")
		return nil
	}

	fmt.Fprintf(cmd.w, "Attempting to remove %s from the database, and delete it from all gitaly nodes...\n", cmd.relativePath)

	fmt.Fprintln(cmd.w, "Removing repository from the database...")
	removed, err := cmd.removeRepositoryFromDatabase(ctx, db)
	if err != nil {
		return fmt.Errorf("remove repository info from praefect database: %w", err)
	}

	if !removed {
		fmt.Fprintln(cmd.w, "The database has no information about this repository.")
	} else {
		fmt.Fprintln(cmd.w, "Removed repository metadata from the database.")
	}

	fmt.Fprintln(cmd.w, "Removing replication events...")
	ticker := helper.NewTimerTicker(time.Second)
	defer ticker.Stop()
	if err := cmd.removeReplicationEvents(ctx, logger, db, ticker); err != nil {
		return fmt.Errorf("remove scheduled replication events: %w", err)
	}
	fmt.Fprintln(cmd.w, "Replication event removal completed.")

	// We should try to remove repository from each of gitaly nodes.
	fmt.Fprintln(cmd.w, "Removing repository directly from gitaly nodes...")
	cmd.removeRepositoryForEachGitaly(ctx, cfg, logger)
	fmt.Fprintln(cmd.w, "Finished removing repository from gitaly nodes.")

	return nil
}

// getStoragesFromNodes looks on disk to finds the storages this repository exists for
func (cmd *removeRepository) getStoragesFromNodes(ctx context.Context, cfg config.Config) ([]string, error) {
	var storages []string
	for _, vs := range cfg.VirtualStorages {
		if vs.Name != cmd.virtualStorage {
			continue
		}

		storages = make([]string, len(vs.Nodes))
		var wg sync.WaitGroup

		for i := 0; i < len(vs.Nodes); i++ {
			wg.Add(1)
			go func(node *config.Node, i int) {
				defer wg.Done()
				repo := &gitalypb.Repository{
					StorageName:  node.Storage,
					RelativePath: cmd.relativePath,
				}
				exists, err := repositoryExists(ctx, repo, node.Address, node.Token)
				if err != nil {
					cmd.logger.WithError(err).Errorf("checking if repository exists on %q", node.Storage)
				}
				if exists {
					storages[i] = node.Storage
				}
			}(vs.Nodes[i], i)
		}

		wg.Wait()
		break
	}

	var storagesFound []string
	for _, storage := range storages {
		if storage != "" {
			storagesFound = append(storagesFound, storage)
		}
	}

	return storagesFound, nil
}

func (cmd *removeRepository) removeRepositoryFromDatabase(ctx context.Context, db *sql.DB) (bool, error) {
	var removed bool
	if err := db.QueryRowContext(
		ctx,
		`WITH remove_storages_info AS (
				DELETE FROM storage_repositories
				WHERE virtual_storage = $1 AND relative_path = $2
			)
			DELETE FROM repositories
			WHERE virtual_storage = $1 AND relative_path = $2
			RETURNING TRUE`,
		cmd.virtualStorage,
		cmd.relativePath,
	).Scan(&removed); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		return false, fmt.Errorf("query row: %w", err)
	}
	return removed, nil
}

func (cmd *removeRepository) removeRepository(ctx context.Context, repo *gitalypb.Repository, addr, token string) (bool, error) {
	conn, err := subCmdDial(ctx, addr, token, cmd.dialTimeout)
	if err != nil {
		return false, fmt.Errorf("error dialing: %w", err)
	}
	defer func() { _ = conn.Close() }()

	ctx = metadata.AppendToOutgoingContext(ctx, "client_name", removeRepositoryCmdName)
	repositoryClient := gitalypb.NewRepositoryServiceClient(conn)
	if _, err := repositoryClient.RemoveRepository(ctx, &gitalypb.RemoveRepositoryRequest{Repository: repo}); err != nil {
		if _, ok := status.FromError(err); !ok {
			return false, fmt.Errorf("RemoveRepository: %w", err)
		}
		return false, nil
	}
	return true, nil
}

func (cmd *removeRepository) removeReplicationEvents(ctx context.Context, logger logrus.FieldLogger, db *sql.DB, ticker helper.Ticker) error {
	// Wait for the completion of the repository replication jobs.
	// As some of them could be a repository creation jobs we need to remove those newly created
	// repositories after replication finished.
	start := time.Now()
	var tick helper.Ticker
	for found := true; found; {
		if tick != nil {
			tick.Reset()
			<-tick.C()
		} else {
			tick = ticker
		}

		if int(time.Since(start).Seconds())%5 == 0 {
			logger.Debug("awaiting for the repository in_progress replication jobs to complete...")
		}
		row := db.QueryRowContext(
			ctx,
			`WITH remove_replication_jobs AS (
				DELETE FROM replication_queue
				WHERE job->>'virtual_storage' = $1
					AND job->>'relative_path' = $2
					-- Do not remove ongoing replication events as we need to wait
					-- for their completion.
					AND state != 'in_progress'
			)
			SELECT EXISTS(
				SELECT
				FROM replication_queue
				WHERE job->>'virtual_storage' = $1
					AND job->>'relative_path' = $2
					AND state = 'in_progress')`,
			cmd.virtualStorage,
			cmd.relativePath,
		)
		if err := row.Scan(&found); err != nil {
			return fmt.Errorf("scan in progress jobs: %w", err)
		}
	}
	return nil
}

func (cmd *removeRepository) removeNode(
	ctx context.Context,
	logger logrus.FieldLogger,
	node *config.Node,
) {
	logger.Debugf("remove repository with gitaly %q at %q", node.Storage, node.Address)
	repo := &gitalypb.Repository{
		StorageName:  node.Storage,
		RelativePath: cmd.relativePath,
	}
	removed, err := cmd.removeRepository(ctx, repo, node.Address, node.Token)
	if err != nil {
		logger.WithError(err).Warnf("repository removal failed for %q", node.Storage)
	} else {
		if removed {
			fmt.Fprintf(cmd.w, "Successfully removed %s from %s\n", cmd.relativePath, node.Storage)
		} else {
			fmt.Fprintf(cmd.w, "Did not remove %s from %s\n", cmd.relativePath, node.Storage)
		}
	}
	logger.Debugf("repository removal call to gitaly %q completed", node.Storage)
}

func (cmd *removeRepository) removeRepositoryForEachGitaly(ctx context.Context, cfg config.Config, logger logrus.FieldLogger) {
	for _, vs := range cfg.VirtualStorages {
		if vs.Name == cmd.virtualStorage {
			var wg sync.WaitGroup
			for i := 0; i < len(vs.Nodes); i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					cmd.removeNode(ctx, logger, vs.Nodes[i])
				}(i)
			}
			wg.Wait()
			break
		}
	}
}
