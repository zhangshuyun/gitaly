// Command praefect provides a reverse-proxy server with high-availability
// specific features for Gitaly.
//
// Additionally, praefect has subcommands for common tasks:
//
// SQL Ping
//
// The subcommand "sql-ping" checks if the database configured in the config
// file is reachable:
//
//     praefect -config PATH_TO_CONFIG sql-ping
//
// SQL Migrate
//
// The subcommand "sql-migrate" will apply any outstanding SQL migrations.
//
//     praefect -config PATH_TO_CONFIG sql-migrate
//
// Dial Nodes
//
// The subcommand "dial-nodes" helps diagnose connection problems to Gitaly or
// Praefect. The subcommand works by sourcing the connection information from
// the config file, and then dialing and health checking the remote nodes.
//
//     praefect -config PATH_TO_CONFIG dial-nodes
//
// Reconcile
//
// The subcommand "reconcile" performs a consistency check of a backend storage
// against the primary or another storage in the same virtual storage group.
//
//     praefect -config PATH_TO_CONFIG reconcile -virtual <vstorage> -target <t-storage> [-reference <r-storage>]
//
// "-virtual" specifies which virtual storage the target and reference
// belong to.
//
// "-target" specifies the storage name of the backend Gitaly you wish to
// reconcile.
//
// "-reference" is an optional argument that specifies which storage location to
// check the target against. If an inconsistency is found, the target will
// attempt to repair itself using the reference as the source of truth. If the
// reference storage is omitted, Praefect will perform the check against the
// current primary. If the primary is the same as the target, an error will
// occur.
package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/internal/bootstrap"
	"gitlab.com/gitlab-org/gitaly/internal/bootstrap/starter"
	"gitlab.com/gitlab-org/gitaly/internal/config/sentry"
	"gitlab.com/gitlab-org/gitaly/internal/log"
	"gitlab.com/gitlab-org/gitaly/internal/praefect"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/datastore"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/datastore/glsql"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/metrics"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/nodes"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/protoregistry"
	"gitlab.com/gitlab-org/gitaly/internal/version"
	"gitlab.com/gitlab-org/labkit/monitoring"
	"gitlab.com/gitlab-org/labkit/tracing"
)

var (
	flagConfig  = flag.String("config", "", "Location for the config.toml")
	flagVersion = flag.Bool("version", false, "Print version and exit")
	logger      = log.Default()

	errNoConfigFile = errors.New("the config flag must be passed")
)

const progname = "praefect"

func main() {
	flag.Usage = func() {
		cmds := []string{}
		for k := range subcommands {
			cmds = append(cmds, k)
		}

		printfErr("Usage of %s:\n", progname)
		flag.PrintDefaults()
		printfErr("  subcommand (optional)\n")
		printfErr("\tOne of %s\n", strings.Join(cmds, ", "))
	}
	flag.Parse()

	// If invoked with -version
	if *flagVersion {
		fmt.Println(praefect.GetVersionString())
		os.Exit(0)
	}

	conf, err := initConfig()
	if err != nil {
		printfErr("%s: configuration error: %v\n", progname, err)
		os.Exit(1)
	}

	if args := flag.Args(); len(args) > 0 {
		os.Exit(subCommand(conf, args[0], args[1:]))
	}

	configure(conf)

	logger.WithField("version", praefect.GetVersionString()).Info("Starting " + progname)

	starterConfigs, err := getStarterConfigs(conf.SocketPath, conf.ListenAddr)
	if err != nil {
		logger.Fatalf("%s", err)
	}

	if err := run(starterConfigs, conf); err != nil {
		logger.Fatalf("%v", err)
	}
}

func initConfig() (config.Config, error) {
	var conf config.Config

	if *flagConfig == "" {
		return conf, errNoConfigFile
	}

	conf, err := config.FromFile(*flagConfig)
	if err != nil {
		return conf, fmt.Errorf("error reading config file: %v", err)
	}

	if err := conf.Validate(); err != nil {
		return config.Config{}, err
	}

	return conf, nil
}

func configure(conf config.Config) {
	conf.ConfigureLogger()

	tracing.Initialize(tracing.WithServiceName(progname))

	if conf.PrometheusListenAddr != "" {
		logger.WithField("address", conf.PrometheusListenAddr).Info("Starting prometheus listener")
		conf.Prometheus.Configure()

		go func() {
			if err := monitoring.Start(
				monitoring.WithListenerAddress(conf.PrometheusListenAddr),
				monitoring.WithBuildInformation(praefect.GetVersion(), praefect.GetBuildTime())); err != nil {
				logger.WithError(err).Errorf("Unable to start healthcheck listener: %v", conf.PrometheusListenAddr)
			}
		}()
	}

	sentry.ConfigureSentry(version.GetVersion(), conf.Sentry)
}

func run(cfgs []starter.Config, conf config.Config) error {
	nodeLatencyHistogram, err := metrics.RegisterNodeLatency(conf.Prometheus)
	if err != nil {
		return err
	}

	delayMetric, err := metrics.RegisterReplicationDelay(conf.Prometheus)
	if err != nil {
		return err
	}

	latencyMetric, err := metrics.RegisterReplicationLatency(conf.Prometheus)
	if err != nil {
		return err
	}

	queueMetric, err := metrics.RegisterReplicationJobsInFlight()
	if err != nil {
		return err
	}

	nodeManager, err := nodes.NewManager(logger, conf, nodeLatencyHistogram)
	if err != nil {
		return err
	}
	nodeManager.Start(1*time.Second, 3*time.Second)

	registry := protoregistry.New()
	if err = registry.RegisterFiles(protoregistry.GitalyProtoFileDescriptors...); err != nil {
		return err
	}

	ds := datastore.Datastore{
		ReplicasDatastore: datastore.NewInMemory(conf),
	}

	if conf.PostgresQueueEnabled {
		db, closedb, err := initDatabase(logger, conf)
		if err != nil {
			return err
		}
		defer closedb()
		ds.ReplicationEventQueue = datastore.NewPostgresReplicationEventQueue(db)
	} else {
		ds.ReplicationEventQueue = datastore.NewMemoryReplicationEventQueue()
	}

	var (
		// top level server dependencies
		coordinator = praefect.NewCoordinator(logger, ds, nodeManager, conf, registry)
		repl        = praefect.NewReplMgr(
			conf.VirtualStorages[0].Name,
			logger,
			ds,
			nodeManager,
			praefect.WithDelayMetric(delayMetric),
			praefect.WithLatencyMetric(latencyMetric),
			praefect.WithQueueMetric(queueMetric))
		srv = praefect.NewServer(coordinator.StreamDirector, logger, registry, conf)

		serverErrors = make(chan error, 1)
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, err := bootstrap.New()
	if err != nil {
		return fmt.Errorf("unable to create a bootstrap: %v", err)
	}

	srv.RegisterServices(nodeManager, conf, ds)

	b.StopAction = srv.GracefulStop
	for _, cfg := range cfgs {
		b.RegisterStarter(starter.New(cfg, srv))
	}

	if err := b.Start(); err != nil {
		return fmt.Errorf("unable to start the bootstrap: %v", err)
	}

	go func() { serverErrors <- b.Wait() }()
	go func() {
		serverErrors <- repl.ProcessBacklog(ctx, praefect.ExpBackoffFunc(1*time.Second, 5*time.Second))
	}()

	return <-serverErrors
}

func getStarterConfigs(socketPath, listenAddr string) ([]starter.Config, error) {
	var cfgs []starter.Config
	if socketPath != "" {
		if err := os.RemoveAll(socketPath); err != nil {
			return nil, err
		}

		cleanPath := strings.TrimPrefix(socketPath, "unix:")

		cfgs = append(cfgs, starter.Config{Name: starter.Unix, Addr: cleanPath})

		logger.WithField("address", socketPath).Info("listening on unix socket")
	}

	if listenAddr != "" {
		cleanAddr := strings.TrimPrefix(listenAddr, "tcp://")

		cfgs = append(cfgs, starter.Config{Name: starter.TCP, Addr: cleanAddr})

		logger.WithField("address", listenAddr).Info("listening at tcp address")
	}

	return cfgs, nil
}

func initDatabase(logger *logrus.Entry, conf config.Config) (*sql.DB, func(), error) {
	db, err := glsql.OpenDB(conf.DB)
	if err != nil {
		logger.WithError(err).Error("SQL connection open failed")
		return nil, nil, err
	}

	closedb := func() {
		if err := db.Close(); err != nil {
			logger.WithError(err).Error("SQL connection close failed")
		}
	}

	if err := datastore.CheckPostgresVersion(db); err != nil {
		closedb()
		return nil, nil, err
	}

	return db, closedb, nil
}
