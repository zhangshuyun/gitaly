package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"io"
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
			"	Runs in dry-run mode by default checks whether the repository exists" +
			"	without actually removing it from the database and disk.\n" +
			"	When -apply is used, the repository will be removed from the database as well as\n" +
			"	the individual gitaly nodes on which they exist.\n")
		fs.PrintDefaults()
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
			return errors.New("repository is not being tracked in Praefect")
		}
	}

	if !cmd.apply {
		fmt.Fprintln(cmd.w, "Re-run the command with -apply to remove repositories from the database and disk.")
		return nil
	}

	fmt.Fprintf(cmd.w, "Attempting to remove %s from the database, and delete it from all gitaly nodes...\n", cmd.relativePath)

	addr, err := getNodeAddress(cfg)
	if err != nil {
		return fmt.Errorf("get node address: %w", err)
	}

	_, err = cmd.removeRepository(ctx, &gitalypb.Repository{
		StorageName:  cmd.virtualStorage,
		RelativePath: cmd.relativePath,
	}, addr, cfg.Auth.Token)
	if err != nil {
		return fmt.Errorf("repository removal failed: %w", err)
	}

	fmt.Fprintln(cmd.w, "Repository removal completed.")

	fmt.Fprintln(cmd.w, "Removing replication events...")
	ticker := helper.NewTimerTicker(time.Second)
	defer ticker.Stop()
	if err := cmd.removeReplicationEvents(ctx, logger, db, ticker); err != nil {
		return fmt.Errorf("remove scheduled replication events: %w", err)
	}
	fmt.Fprintln(cmd.w, "Replication event removal completed.")
	return nil
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
		return false, err
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
