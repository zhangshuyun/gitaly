package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"math/rand"
	"time"

	"github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/datastore"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/datastore/glsql"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"gitlab.com/gitlab-org/labkit/correlation"
	"google.golang.org/grpc/metadata"
)

const (
	trackRepositoryCmdName = "track-repository"
)

type trackRepository struct {
	logger               logrus.FieldLogger
	virtualStorage       string
	relativePath         string
	authoritativeStorage string
}

var errAuthoritativeRepositoryNotExist = errors.New("authoritative repository does not exist")

func newTrackRepository(logger logrus.FieldLogger) *trackRepository {
	return &trackRepository{logger: logger}
}

func (cmd *trackRepository) FlagSet() *flag.FlagSet {
	fs := flag.NewFlagSet(trackRepositoryCmdName, flag.ExitOnError)
	fs.StringVar(&cmd.virtualStorage, paramVirtualStorage, "", "name of the repository's virtual storage")
	fs.StringVar(&cmd.relativePath, paramRelativePath, "", "relative path to the repository")
	fs.StringVar(&cmd.authoritativeStorage, paramAuthoritativeStorage, "", "storage with the repository to consider as authoritative")
	fs.Usage = func() {
		_, _ = printfErr("Description:\n" +
			"	This command adds a given repository to be tracked by Praefect.\n" +
			"       It checks if the repository exists on disk on the authoritative storage, " +
			"       and whether database records are absent from tracking the repository.")
		fs.PrintDefaults()
	}
	return fs
}

func (cmd trackRepository) Exec(flags *flag.FlagSet, cfg config.Config) error {
	switch {
	case flags.NArg() > 0:
		return unexpectedPositionalArgsError{Command: flags.Name()}
	case cmd.virtualStorage == "":
		return requiredParameterError(paramVirtualStorage)
	case cmd.relativePath == "":
		return requiredParameterError(paramRelativePath)
	case cmd.authoritativeStorage == "":
		if cfg.Failover.ElectionStrategy == config.ElectionStrategyPerRepository {
			return requiredParameterError(paramAuthoritativeStorage)
		}
	}

	db, err := glsql.OpenDB(cfg.DB)
	if err != nil {
		return fmt.Errorf("connect to database: %w", err)
	}
	defer func() { _ = db.Close() }()

	ctx := correlation.ContextWithCorrelation(context.Background(), correlation.SafeRandomID())
	logger := cmd.logger.WithField("correlation_id", correlation.ExtractFromContext(ctx))

	return cmd.exec(ctx, logger, db, cfg)
}

const trackRepoErrorPrefix = "attempting to track repository in praefect database"

func (cmd *trackRepository) exec(ctx context.Context, logger logrus.FieldLogger, db *sql.DB, cfg config.Config) error {
	logger.WithFields(logrus.Fields{
		"virtual_storage":       cmd.virtualStorage,
		"relative_path":         cmd.relativePath,
		"authoritative_storage": cmd.authoritativeStorage,
	}).Debug("track repository")

	var primary string
	var secondaries []string
	var variableReplicationFactorEnabled, savePrimary bool
	if cfg.Failover.ElectionStrategy == config.ElectionStrategyPerRepository {
		savePrimary = true
		primary = cmd.authoritativeStorage

		for _, vs := range cfg.VirtualStorages {
			if vs.Name == cmd.virtualStorage {
				for _, node := range vs.Nodes {
					if node.Storage == cmd.authoritativeStorage {
						continue
					}
					secondaries = append(secondaries, node.Storage)
				}
			}
		}

		r := rand.New(rand.NewSource(time.Now().UnixNano()))
		replicationFactor := cfg.DefaultReplicationFactors()[cmd.virtualStorage]

		if replicationFactor > 0 {
			variableReplicationFactorEnabled = true
			// Select random secondaries according to the default replication factor.
			r.Shuffle(len(secondaries), func(i, j int) {
				secondaries[i], secondaries[j] = secondaries[j], secondaries[i]
			})

			secondaries = secondaries[:replicationFactor-1]
		}
	} else {
		savePrimary = false
		if err := db.QueryRowContext(ctx, `SELECT node_name FROM shard_primaries WHERE shard_name = $1 AND demoted = 'false'`, cmd.virtualStorage).Scan(&primary); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return fmt.Errorf("%s: no primaries found", trackRepoErrorPrefix)
			}
			return fmt.Errorf("%s: %w", trackRepoErrorPrefix, err)
		}
	}

	authoritativeRepoExists, err := cmd.authoritativeRepositoryExists(ctx, cfg, primary)
	if err != nil {
		return fmt.Errorf("%s: %w", trackRepoErrorPrefix, err)
	}

	if !authoritativeRepoExists {
		return fmt.Errorf("%s: %w", trackRepoErrorPrefix, errAuthoritativeRepositoryNotExist)
	}

	if err := cmd.trackRepository(
		ctx,
		datastore.NewPostgresRepositoryStore(db, cfg.StorageNames()),
		primary,
		secondaries,
		savePrimary,
		variableReplicationFactorEnabled,
	); err != nil {
		return fmt.Errorf("%s: %w", trackRepoErrorPrefix, err)
	}

	logger.Debug("finished adding new repository to be tracked in praefect database.")

	return nil
}

func (cmd *trackRepository) trackRepository(
	ctx context.Context,
	ds *datastore.PostgresRepositoryStore,
	primary string,
	secondaries []string,
	savePrimary bool,
	variableReplicationFactorEnabled bool,
) error {
	if err := ds.CreateRepository(
		ctx,
		cmd.virtualStorage,
		cmd.relativePath,
		primary,
		nil,
		secondaries,
		savePrimary,
		variableReplicationFactorEnabled,
	); err != nil {
		var repoExistsError datastore.RepositoryExistsError
		if errors.As(err, &repoExistsError) {
			cmd.logger.Print("repository is already tracked in praefect database")
			return nil
		}

		return fmt.Errorf("CreateRepository: %w", err)
	}

	return nil
}

func (cmd *trackRepository) repositoryExists(ctx context.Context, repo *gitalypb.Repository, addr, token string) (bool, error) {
	conn, err := subCmdDial(addr, token)
	if err != nil {
		return false, fmt.Errorf("error dialing: %w", err)
	}
	defer func() { _ = conn.Close() }()

	ctx = metadata.AppendToOutgoingContext(ctx, "client_name", trackRepositoryCmdName)
	repositoryClient := gitalypb.NewRepositoryServiceClient(conn)
	res, err := repositoryClient.RepositoryExists(ctx, &gitalypb.RepositoryExistsRequest{Repository: repo})
	if err != nil {
		return false, err
	}

	return res.GetExists(), nil
}

func (cmd *trackRepository) authoritativeRepositoryExists(ctx context.Context, cfg config.Config, nodeName string) (bool, error) {
	for _, vs := range cfg.VirtualStorages {
		if vs.Name != cmd.virtualStorage {
			continue
		}

		for _, node := range vs.Nodes {
			if node.Storage == nodeName {
				logger.Debugf("check if repository %q exists on gitaly %q at %q", cmd.relativePath, node.Storage, node.Address)
				repo := &gitalypb.Repository{
					StorageName:  node.Storage,
					RelativePath: cmd.relativePath,
				}
				exists, err := cmd.repositoryExists(ctx, repo, node.Address, node.Token)
				if err != nil {
					logger.WithError(err).Warnf("checking if repository exists %q, %q", node.Storage, cmd.relativePath)
					return false, nil
				}
				return exists, nil
			}
		}
		return false, fmt.Errorf("node %q not found", cmd.authoritativeStorage)
	}
	return false, fmt.Errorf("virtual storage %q not found", cmd.virtualStorage)
}
