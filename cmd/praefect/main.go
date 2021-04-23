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
//     praefect -config PATH_TO_CONFIG sql-migrate [-ignore-unknown=true|false]
//
// By default, the migration will ignore any unknown migrations that are
// not known by the Praefect binary.
//
// "-ignore-unknown=false" will disable this behavior.
//
// The subcommand "sql-migrate-status" will show which SQL migrations have
// been applied and which ones have not:
//
//     praefect -config PATH_TO_CONFIG sql-migrate-status
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
//     praefect -config PATH_TO_CONFIG reconcile -virtual <vstorage> -target
//     <t-storage> [-reference <r-storage>] [-f]
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
//
// By default, a dry-run is performed where no replications are scheduled. When
// the flag "-f" is provided, the replications will actually schedule.
//
// Dataloss
//
// The subcommand "dataloss" identifies Gitaly nodes which are missing data from the
// previous write-enabled primary node. It does so by looking through incomplete
// replication jobs. This is useful for identifying potential data loss from a failover
// event.
//
//     praefect -config PATH_TO_CONFIG dataloss [-virtual-storage <virtual-storage>]
//
// "-virtual-storage" specifies which virtual storage to check for data loss. If not specified,
// the check is performed for every configured virtual storage.
//
// Accept Dataloss
//
// The subcommand "accept-dataloss" allows for accepting data loss in a repository to enable it for
// writing again. The current version of the repository on the authoritative storage is set to be
// the latest version and replications to other nodes are scheduled in order to bring them consistent
// with the new authoritative version.
//
//     praefect -config PATH_TO_CONFIG accept-dataloss -virtual-storage <virtual-storage> -relative-path <relative-path> -authoritative-storage <authoritative-storage>
package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/internal/backchannel"
	"gitlab.com/gitlab-org/gitaly/internal/bootstrap"
	"gitlab.com/gitlab-org/gitaly/internal/bootstrap/starter"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config/sentry"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/log"
	"gitlab.com/gitlab-org/gitaly/internal/praefect"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/datastore"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/datastore/glsql"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/importer"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/metrics"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/nodes"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/nodes/tracker"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/protoregistry"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/reconciler"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/service/transaction"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/transactions"
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

	conf.ConfigureLogger()

	if args := flag.Args(); len(args) > 0 {
		os.Exit(subCommand(conf, args[0], args[1:]))
	}

	configure(conf)

	logger.WithField("version", praefect.GetVersionString()).Info("Starting " + progname)

	starterConfigs, err := getStarterConfigs(conf)
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

	if !conf.Failover.Enabled && conf.Failover.ElectionStrategy != "" {
		logger.WithField("election_strategy", conf.Failover.ElectionStrategy).Warn(
			"ignoring configured election strategy as failover is disabled")
	}

	return conf, nil
}

func configure(conf config.Config) {
	tracing.Initialize(tracing.WithServiceName(progname))

	if conf.PrometheusListenAddr != "" {
		conf.Prometheus.Configure()
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

	var db *sql.DB

	if conf.NeedsSQL() {
		dbConn, closedb, err := initDatabase(logger, conf)
		if err != nil {
			return err
		}
		defer closedb()
		db = dbConn
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var queue datastore.ReplicationEventQueue
	var rs datastore.RepositoryStore
	var csg datastore.ConsistentStoragesGetter
	var metricsCollectors []prometheus.Collector

	if conf.MemoryQueueEnabled {
		queue = datastore.NewMemoryReplicationEventQueue(conf)
		rs = datastore.MockRepositoryStore{}
		csg = rs
		logger.Info("reads distribution caching is disabled for in memory storage")
	} else {
		queue = datastore.NewPostgresReplicationEventQueue(db)
		rs = datastore.NewPostgresRepositoryStore(db, conf.StorageNames())

		if conf.DB.ToPQString(true) == "" {
			csg = rs
			logger.Info("reads distribution caching is disabled because direct connection to Postgres is not set")
		} else {
			listenerOpts := datastore.DefaultPostgresListenerOpts
			listenerOpts.Addr = conf.DB.ToPQString(true)
			listenerOpts.Channels = []string{"repositories_updates", "storage_repositories_updates"}

			storagesCached, err := datastore.NewCachingConsistentStoragesGetter(logger, rs, conf.VirtualStorageNames())
			if err != nil {
				return fmt.Errorf("caching storage provider: %w", err)
			}

			postgresListener, err := datastore.NewPostgresListener(logger, listenerOpts, storagesCached)
			if err != nil {
				return err
			}

			defer func() {
				if err := postgresListener.Close(); err != nil {
					logger.WithError(err).Error("error on closing Postgres notifications listener")
				}
			}()

			metricsCollectors = append(metricsCollectors, storagesCached, postgresListener)
			csg = storagesCached
			logger.Info("reads distribution caching is enabled by configuration")
		}
	}

	var errTracker tracker.ErrorTracker

	if conf.Failover.Enabled {
		thresholdsConfigured, err := conf.Failover.ErrorThresholdsConfigured()
		if err != nil {
			return err
		}

		if thresholdsConfigured {
			errTracker, err = tracker.NewErrors(ctx, conf.Failover.ErrorThresholdWindow.Duration(), conf.Failover.ReadErrorThresholdCount, conf.Failover.WriteErrorThresholdCount)
			if err != nil {
				return err
			}
		}
	}

	transactionManager := transactions.NewManager(conf)
	clientHandshaker := backchannel.NewClientHandshaker(logger, praefect.NewBackchannelServerFactory(logger, transaction.NewServer(transactionManager)))
	assignmentStore := praefect.NewDisabledAssignmentStore(conf.StorageNames())
	var (
		nodeManager   nodes.Manager
		healthChecker praefect.HealthChecker
		nodeSet       praefect.NodeSet
		router        praefect.Router
		primaryGetter praefect.PrimaryGetter
	)
	if conf.Failover.ElectionStrategy == config.ElectionStrategyPerRepository {
		nodeSet, err = praefect.DialNodes(ctx, conf.VirtualStorages, protoregistry.GitalyProtoPreregistered, errTracker, clientHandshaker)
		if err != nil {
			return fmt.Errorf("dial nodes: %w", err)
		}
		defer nodeSet.Close()

		hm := nodes.NewHealthManager(logger, db, nodes.GeneratePraefectName(conf, logger), nodeSet.HealthClients())
		go func() {
			if err := hm.Run(ctx, helper.NewTimerTicker(time.Second)); err != nil {
				logger.WithError(err).Error("health manager exited")
			}
		}()
		healthChecker = hm

		elector := nodes.NewPerRepositoryElector(logger, db, hm)
		go func() {
			if err := elector.Run(ctx, hm.Updated()); err != nil {
				logger.WithError(err).Error("primary elector exited")
			}
		}()

		primaryGetter = elector
		assignmentStore = datastore.NewAssignmentStore(db, conf.StorageNames())

		router = praefect.NewPerRepositoryRouter(
			nodeSet.Connections(),
			elector,
			hm,
			praefect.NewLockedRandom(rand.New(rand.NewSource(time.Now().UnixNano()))),
			csg,
			assignmentStore,
			conf.DefaultReplicationFactors(),
		)
	} else {
		nodeMgr, err := nodes.NewManager(logger, conf, db, csg, nodeLatencyHistogram, protoregistry.GitalyProtoPreregistered, errTracker, clientHandshaker)
		if err != nil {
			return err
		}

		healthChecker = praefect.HealthChecker(nodeMgr)
		nodeSet = praefect.NodeSetFromNodeManager(nodeMgr)
		router = praefect.NewNodeManagerRouter(nodeMgr, rs)
		primaryGetter = nodeMgr
		nodeManager = nodeMgr

		nodeMgr.Start(conf.Failover.BootstrapInterval.Duration(), conf.Failover.MonitorInterval.Duration())
	}

	logger.Infof("election strategy: %q", conf.Failover.ElectionStrategy)
	logger.Info("background started: gitaly nodes health monitoring")

	var (
		// top level server dependencies
		coordinator = praefect.NewCoordinator(
			queue,
			rs,
			router,
			transactionManager,
			conf,
			protoregistry.GitalyProtoPreregistered,
		)

		repl = praefect.NewReplMgr(
			logger,
			conf.VirtualStorageNames(),
			queue,
			rs,
			healthChecker,
			nodeSet,
			praefect.WithDelayMetric(delayMetric),
			praefect.WithLatencyMetric(latencyMetric),
			praefect.WithDequeueBatchSize(conf.Replication.BatchSize),
		)
		srvFactory = praefect.NewServerFactory(
			conf,
			logger,
			coordinator.StreamDirector,
			nodeManager,
			transactionManager,
			queue,
			rs,
			assignmentStore,
			protoregistry.GitalyProtoPreregistered,
			nodeSet.Connections(),
			primaryGetter,
		)
	)
	metricsCollectors = append(metricsCollectors, transactionManager, coordinator, repl)
	if db != nil {
		prometheus.MustRegister(
			datastore.NewRepositoryStoreCollector(
				logger,
				conf.VirtualStorageNames(),
				db,
				conf.Failover.ElectionStrategy == config.ElectionStrategyPerRepository,
			),
		)
	}
	prometheus.MustRegister(metricsCollectors...)

	b, err := bootstrap.New()
	if err != nil {
		return fmt.Errorf("unable to create a bootstrap: %v", err)
	}

	b.StopAction = srvFactory.GracefulStop
	for _, cfg := range cfgs {
		srv, err := srvFactory.Create(cfg.IsSecure())
		if err != nil {
			return fmt.Errorf("create gRPC server: %w", err)
		}
		b.RegisterStarter(starter.New(cfg, srv))
	}

	if conf.PrometheusListenAddr != "" {
		logger.WithField("address", conf.PrometheusListenAddr).Info("Starting prometheus listener")

		b.RegisterStarter(func(listen bootstrap.ListenFunc, _ chan<- error) error {
			l, err := listen(starter.TCP, conf.PrometheusListenAddr)
			if err != nil {
				return err
			}

			go func() {
				if err := monitoring.Start(
					monitoring.WithListener(l),
					monitoring.WithBuildInformation(praefect.GetVersion(), praefect.GetBuildTime())); err != nil {
					logger.WithError(err).Errorf("Unable to start prometheus listener: %v", conf.PrometheusListenAddr)
				}
			}()

			return nil
		})
	}

	if db != nil && nodeManager != nil {
		go func() {
			virtualStorages := conf.VirtualStorageNames()
			finished := make(map[string]bool, len(virtualStorages))
			for _, virtualStorage := range virtualStorages {
				finished[virtualStorage] = true
			}

			for result := range importer.New(nodeManager, virtualStorages, db).Run(ctx) {
				if result.Error != nil {
					logger.WithFields(logrus.Fields{
						"virtual_storage": result.VirtualStorage,
						logrus.ErrorKey:   result.Error,
					}).Error("importing repositories to database failed")
					finished[result.VirtualStorage] = false
					continue
				}

				logger.WithFields(logrus.Fields{
					"virtual_storage": result.VirtualStorage,
					"relative_paths":  result.RelativePaths,
				}).Info("imported repositories to database")
			}

			logger.WithField("virtual_storages", finished).Info("repository importer finished")
		}()
	}

	if err := b.Start(); err != nil {
		return fmt.Errorf("unable to start the bootstrap: %v", err)
	}

	go repl.ProcessBacklog(ctx, praefect.ExpBackoffFunc(1*time.Second, 5*time.Second))
	logger.Info("background started: processing of the replication events")
	repl.ProcessStale(ctx, 30*time.Second, time.Minute)
	logger.Info("background started: processing of the stale replication events")

	if interval := conf.Reconciliation.SchedulingInterval.Duration(); interval > 0 {
		if conf.MemoryQueueEnabled {
			logger.Warn("Disabled automatic reconciliation as it is only implemented using SQL queue and in-memory queue is configured.")
		} else {
			r := reconciler.NewReconciler(
				logger,
				db,
				healthChecker,
				conf.StorageNames(),
				conf.Reconciliation.HistogramBuckets,
			)
			prometheus.MustRegister(r)
			go r.Run(ctx, helper.NewTimerTicker(interval))
		}
	}

	return b.Wait(conf.GracefulStopTimeout.Duration())
}

func getStarterConfigs(conf config.Config) ([]starter.Config, error) {
	var cfgs []starter.Config
	unique := map[string]struct{}{}
	for schema, addr := range map[string]string{
		starter.TCP:  conf.ListenAddr,
		starter.TLS:  conf.TLSListenAddr,
		starter.Unix: conf.SocketPath,
	} {
		if addr == "" {
			continue
		}

		addrConf, err := starter.ParseEndpoint(addr)
		if err != nil {
			// address doesn't include schema
			if !errors.Is(err, starter.ErrEmptySchema) {
				return nil, err
			}
			addrConf = starter.Config{Name: schema, Addr: addr}
		}

		if _, found := unique[addrConf.Addr]; found {
			return nil, fmt.Errorf("same address can't be used for different schemas %q", addr)
		}
		unique[addrConf.Addr] = struct{}{}

		cfgs = append(cfgs, addrConf)

		logger.WithFields(logrus.Fields{"schema": schema, "address": addr}).Info("listening")
	}

	if len(cfgs) == 0 {
		return nil, errors.New("no listening addresses were provided, unable to start")
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
