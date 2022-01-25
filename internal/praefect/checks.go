package praefect

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	migrate "github.com/rubenv/sql-migrate"
	gitalyauth "gitlab.com/gitlab-org/gitaly/v14/auth"
	"gitlab.com/gitlab-org/gitaly/v14/client"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper/env"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/datastore"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/datastore/glsql"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/datastore/migrations"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/nodes"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"gitlab.com/gitlab-org/labkit/correlation"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
)

// Severity is a type that indicates the severity of a check
type Severity string

const (
	// Warning indicates a severity level of warning
	Warning Severity = "warning"
	// Fatal indicates a severity level of fatal
	// any checks that are Fatal will prevent Praefect from starting up
	Fatal = "fatal"
)

// Check is a struct representing a check on the health of a Gitaly cluster's setup. These are separate from the "healthcheck"
// concept which is more concerned with the health of the praefect service. These checks are meant to diagnose any issues with
// the praefect cluster setup itself and will be run on startup/restarts.
type Check struct {
	Run         func(ctx context.Context) error
	Name        string
	Description string
	Severity    Severity
}

// CheckFunc is a function type that takes a praefect config and returns a Check
type CheckFunc func(conf config.Config, w io.Writer, quiet bool) *Check

// NewPraefectMigrationCheck returns a Check that checks if all praefect migrations have run
func NewPraefectMigrationCheck(conf config.Config, w io.Writer, quiet bool) *Check {
	return &Check{
		Name:        "praefect migrations",
		Description: "confirms whether or not all praefect migrations have run",
		Run: func(ctx context.Context) error {
			openDBCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
			defer cancel()
			db, err := glsql.OpenDB(openDBCtx, conf.DB)
			if err != nil {
				return err
			}
			defer db.Close()

			migrationSet := migrate.MigrationSet{
				TableName: migrations.MigrationTableName,
			}

			migrationSource := &migrate.MemoryMigrationSource{
				Migrations: migrations.All(),
			}

			migrationsNeeded, _, err := migrationSet.PlanMigration(db, "postgres", migrationSource, migrate.Up, 0)
			if err != nil {
				return err
			}

			missingMigrations := len(migrationsNeeded)

			if missingMigrations > 0 {
				return fmt.Errorf("%d migrations have not been run", missingMigrations)
			}

			return nil
		},
		Severity: Fatal,
	}
}

// NewGitalyNodeConnectivityCheck returns a check that ensures Praefect can talk to all nodes of all virtual storages
func NewGitalyNodeConnectivityCheck(conf config.Config, w io.Writer, quiet bool) *Check {
	return &Check{
		Name: "gitaly node connectivity & disk access",
		Description: "confirms if praefect can reach all of its gitaly nodes, and " +
			"whether or not the gitaly nodes can read/write from and to its storages.",
		Run: func(ctx context.Context) error {
			return nodes.PingAll(ctx, conf, nodes.NewTextPrinter(w), quiet)
		},
		Severity: Fatal,
	}
}

// NewPostgresReadWriteCheck returns a check that ensures Praefect can read and write to the database
func NewPostgresReadWriteCheck(conf config.Config, w io.Writer, quiet bool) *Check {
	return &Check{
		Name:        "database read/write",
		Description: "checks if praefect can write/read to and from the database",
		Run: func(ctx context.Context) error {
			db, err := glsql.OpenDB(ctx, conf.DB)
			if err != nil {
				return fmt.Errorf("error opening database connection: %w", err)
			}
			defer db.Close()

			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				return fmt.Errorf("error starting transaction: %w", err)
			}
			//nolint: errcheck
			defer tx.Rollback()

			var id int
			if err = tx.QueryRowContext(ctx, "SELECT id FROM hello_world").Scan(&id); err != nil {
				if !errors.Is(err, sql.ErrNoRows) {
					return fmt.Errorf("error reading from table: %w", err)
				}
			}

			logMessage(quiet, w, "successfully read from database")

			res, err := tx.ExecContext(ctx, "INSERT INTO hello_world (id) VALUES(1)")
			if err != nil {
				return fmt.Errorf("error writing to table: %w", err)
			}

			rows, err := res.RowsAffected()
			if err != nil {
				return err
			}

			if rows != 1 {
				return errors.New("failed to insert row")
			}
			logMessage(quiet, w, "successfully wrote to database")

			return nil
		},
		Severity: Fatal,
	}
}

// NewUnavailableReposCheck returns a check that finds the number of repositories without a valid primary
func NewUnavailableReposCheck(conf config.Config, w io.Writer, quiet bool) *Check {
	return &Check{
		Name:        "unavailable repositories",
		Description: "lists repositories that are missing a valid primary, hence rendering them unavailable",
		Run: func(ctx context.Context) error {
			db, err := glsql.OpenDB(ctx, conf.DB)
			if err != nil {
				return fmt.Errorf("error opening database connection: %w", err)
			}
			defer db.Close()

			unavailableRepositories, err := datastore.CountUnavailableRepositories(
				ctx,
				db,
				conf.VirtualStorageNames(),
			)
			if err != nil {
				return err
			}

			if len(unavailableRepositories) == 0 {
				logMessage(quiet, w, "All repositories are available.")
				return nil
			}

			for virtualStorage, unavailableCount := range unavailableRepositories {
				format := "virtual-storage %q has %d repositories that are unavailable."
				if unavailableCount == 1 {
					format = "virtual-storage %q has %d repository that is unavailable."
				}
				logMessage(quiet, w, format, virtualStorage, unavailableCount)
			}

			return errors.New("repositories unavailable")
		},
		Severity: Warning,
	}
}

// NewClockSyncCheck returns a function that returns a check that verifies if system clock is in sync.
func NewClockSyncCheck(clockDriftCheck func(ntpHost string, driftThreshold time.Duration) (bool, error)) func(_ config.Config, _ io.Writer, _ bool) *Check {
	return func(conf config.Config, w io.Writer, quite bool) *Check {
		return &Check{
			Name: "clock synchronization",
			Description: "checks if system clock is in sync with NTP service. " +
				"You can use NTP_HOST env var to provide NTP service URL to query and " +
				"DRIFT_THRESHOLD to provide allowed drift as a duration (1ms, 20sec, etc.)",
			Severity: Fatal,
			Run: func(ctx context.Context) error {
				const driftThresholdMillisEnvName = "DRIFT_THRESHOLD"
				driftThreshold, err := env.GetDuration(driftThresholdMillisEnvName, time.Minute)
				if err != nil {
					return fmt.Errorf("env var %s expected to be an duration (5s, 100ms, etc.)", driftThresholdMillisEnvName)
				}
				// If user specify 10ns it would be converted into 0ms, we should prevent
				// this situation and exit with detailed error.
				if driftThreshold != 0 && driftThreshold.Milliseconds() == 0 {
					return fmt.Errorf("env var %s expected to be 0 or at least 1ms", driftThresholdMillisEnvName)
				}

				ntpHost := os.Getenv("NTP_HOST")
				correlationID := correlation.SafeRandomID()
				ctx = correlation.ContextWithCorrelation(ctx, correlationID)

				logMessage(quite, w, "checking with NTP service at %s and allowed clock drift %d ms [correlation_id: %s]", ntpHost, driftThreshold.Milliseconds(), correlationID)

				g, ctx := errgroup.WithContext(ctx)
				g.Go(func() error {
					synced, err := clockDriftCheck(ntpHost, driftThreshold)
					if err != nil {
						return fmt.Errorf("praefect: %w", err)
					}
					if !synced {
						return errors.New("praefect: clock is not synced")
					}
					return nil
				})

				for i := range conf.VirtualStorages {
					for j := range conf.VirtualStorages[i].Nodes {
						node := conf.VirtualStorages[i].Nodes[j]
						g.Go(func() error {
							opts := []grpc.DialOption{grpc.WithBlock()}
							if len(node.Token) > 0 {
								opts = append(opts, grpc.WithPerRPCCredentials(gitalyauth.RPCCredentialsV2(node.Token)))
							}

							cc, err := client.DialContext(ctx, node.Address, opts)
							if err != nil {
								return fmt.Errorf("%s machine: %w", node.Address, err)
							}
							defer func() { _ = cc.Close() }()

							serverServiceClient := gitalypb.NewServerServiceClient(cc)
							resp, err := serverServiceClient.ClockSynced(ctx, &gitalypb.ClockSyncedRequest{
								NtpHost: ntpHost, DriftThresholdMillis: driftThreshold.Milliseconds(),
							})
							if err != nil {
								return fmt.Errorf("gitaly node at %s: %w", node.Address, err)
							}
							if !resp.GetSynced() {
								return fmt.Errorf("gitaly node at %s: clock is not synced", node.Address)
							}
							return nil
						})
					}
				}
				return g.Wait()
			},
		}
	}
}

func logMessage(quiet bool, w io.Writer, format string, a ...interface{}) {
	if quiet {
		return
	}
	fmt.Fprintf(w, format+"\n", a...)
}
