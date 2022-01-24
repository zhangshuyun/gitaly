package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/go-enry/go-license-detector/v4/licensedb"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	log "github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/v14/client"
	"gitlab.com/gitlab-org/gitaly/v14/internal/backchannel"
	"gitlab.com/gitlab-org/gitaly/v14/internal/bootstrap"
	"gitlab.com/gitlab-org/gitaly/v14/internal/bootstrap/starter"
	"gitlab.com/gitlab-org/gitaly/v14/internal/cache"
	"gitlab.com/gitlab-org/gitaly/v14/internal/cgroups"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/updateref"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git2go"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config/sentry"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/hook"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/linguist"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/rubyserver"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/server"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/service/setup"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/storage"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/transaction"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitlab"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper/env"
	glog "gitlab.com/gitlab-org/gitaly/v14/internal/log"
	"gitlab.com/gitlab-org/gitaly/v14/internal/middleware/limithandler"
	"gitlab.com/gitlab-org/gitaly/v14/internal/streamcache"
	"gitlab.com/gitlab-org/gitaly/v14/internal/tempdir"
	"gitlab.com/gitlab-org/gitaly/v14/internal/version"
	"gitlab.com/gitlab-org/labkit/monitoring"
	"gitlab.com/gitlab-org/labkit/tracing"
	"google.golang.org/grpc"
)

var flagVersion = flag.Bool("version", false, "Print version and exit")

func loadConfig(configPath string) (config.Cfg, error) {
	cfgFile, err := os.Open(configPath)
	if err != nil {
		return config.Cfg{}, err
	}
	defer cfgFile.Close()

	cfg, err := config.Load(cfgFile)
	if err != nil {
		return config.Cfg{}, err
	}

	if err := cfg.Validate(); err != nil {
		return config.Cfg{}, fmt.Errorf("invalid config: %w", err)
	}

	return cfg, nil
}

func flagUsage() {
	fmt.Println(version.GetVersionString())
	fmt.Printf("Usage: %v [OPTIONS] configfile\n", os.Args[0])
	flag.PrintDefaults()
}

func main() {
	flag.Usage = flagUsage
	flag.Parse()

	// If invoked with -version
	if *flagVersion {
		fmt.Println(version.GetVersionString())
		os.Exit(0)
	}

	if flag.NArg() != 1 || flag.Arg(0) == "" {
		flag.Usage()
		os.Exit(2)
	}

	log.Info("Starting Gitaly", "version", version.GetVersionString())
	cfg, err := configure(flag.Arg(0))
	if err != nil {
		log.Fatal(err)
	}
	log.WithError(run(cfg)).Error("shutting down")
	log.Info("Gitaly stopped")
}

func configure(configPath string) (config.Cfg, error) {
	cfg, err := loadConfig(configPath)
	if err != nil {
		return config.Cfg{}, fmt.Errorf("load config: config_path %q: %w", configPath, err)
	}

	glog.Configure(glog.Loggers, cfg.Logging.Format, cfg.Logging.Level)

	if err := cgroups.NewManager(cfg.Cgroups).Setup(); err != nil {
		return config.Cfg{}, fmt.Errorf("failed setting up cgroups: %w", err)
	}

	sentry.ConfigureSentry(version.GetVersion(), sentry.Config(cfg.Logging.Sentry))
	cfg.Prometheus.Configure()
	tracing.Initialize(tracing.WithServiceName("gitaly"))
	preloadLicenseDatabase()

	return cfg, nil
}

func preloadLicenseDatabase() {
	// the first call to `licensedb.Detect` could be too long
	// https://github.com/go-enry/go-license-detector/issues/13
	// this is why we're calling it here to preload license database
	// on server startup to avoid long initialization on gRPC
	// method handling.
	licensedb.Preload()
	log.Info("License database preloaded")
}

func run(cfg config.Cfg) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, err := bootstrap.New(promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "gitaly_connections_total",
			Help: "Total number of connections to Gitaly",
		},
		[]string{"type"},
	))
	if err != nil {
		return fmt.Errorf("init bootstrap: %w", err)
	}

	skipHooks, _ := env.GetBool("GITALY_TESTING_NO_GIT_HOOKS", false)
	var commandFactoryOpts []git.ExecCommandFactoryOption
	if skipHooks {
		commandFactoryOpts = append(commandFactoryOpts, git.WithSkipHooks())
	}

	gitCmdFactory, cleanup, err := git.NewExecCommandFactory(cfg, commandFactoryOpts...)
	if err != nil {
		return fmt.Errorf("creating Git command factory: %w", err)
	}
	defer cleanup()

	gitVersion, err := gitCmdFactory.GitVersion(ctx)
	if err != nil {
		return fmt.Errorf("git version detection: %w", err)
	}

	if !gitVersion.IsSupported() {
		return fmt.Errorf("unsupported Git version: %q", gitVersion)
	}

	registry := backchannel.NewRegistry()
	transactionManager := transaction.NewManager(cfg, registry)
	prometheus.MustRegister(transactionManager)

	hookManager := hook.Manager(hook.DisabledManager{})

	locator := config.NewLocator(cfg)

	tempdir.StartCleaning(locator, cfg.Storages, time.Hour)

	prometheus.MustRegister(gitCmdFactory)

	if skipHooks {
		log.Warn("skipping GitLab API client creation since hooks are bypassed via GITALY_TESTING_NO_GIT_HOOKS")
	} else {
		gitlabClient, err := gitlab.NewHTTPClient(glog.Default(), cfg.Gitlab, cfg.TLS, cfg.Prometheus)
		if err != nil {
			return fmt.Errorf("could not create GitLab API client: %w", err)
		}
		prometheus.MustRegister(gitlabClient)

		hm := hook.NewManager(cfg, locator, gitCmdFactory, transactionManager, gitlabClient)

		hookManager = hm
	}

	conns := client.NewPoolWithOptions(
		client.WithDialer(client.HealthCheckDialer(client.DialContext)),
		client.WithDialOptions(client.FailOnNonTempDialError()...),
	)
	defer conns.Close()

	catfileCache := catfile.NewCache(cfg)
	defer catfileCache.Stop()
	prometheus.MustRegister(catfileCache)

	diskCache := cache.New(cfg, locator)
	prometheus.MustRegister(diskCache)
	if err := diskCache.StartWalkers(); err != nil {
		return fmt.Errorf("disk cache walkers: %w", err)
	}

	limitHandler := limithandler.New(cfg, limithandler.LimitConcurrencyByRepo)
	prometheus.MustRegister(limitHandler)

	gitalyServerFactory := server.NewGitalyServerFactory(cfg, glog.Default(), registry, diskCache, limitHandler)
	defer gitalyServerFactory.Stop()

	ling, err := linguist.New(cfg, gitCmdFactory)
	if err != nil {
		return fmt.Errorf("linguist instance creation: %w", err)
	}

	git2goExecutor := git2go.NewExecutor(cfg, gitCmdFactory, locator)

	updaterWithHooks := updateref.NewUpdaterWithHooks(cfg, locator, hookManager, gitCmdFactory, catfileCache)

	rubySrv := rubyserver.New(cfg, gitCmdFactory)
	if err := rubySrv.Start(); err != nil {
		return fmt.Errorf("initialize gitaly-ruby: %v", err)
	}
	defer rubySrv.Stop()

	streamCache := streamcache.New(cfg.PackObjectsCache, glog.Default())

	for _, c := range []starter.Config{
		{Name: starter.Unix, Addr: cfg.SocketPath, HandoverOnUpgrade: true},
		{Name: starter.Unix, Addr: cfg.GitalyInternalSocketPath(), HandoverOnUpgrade: false},
		{Name: starter.TCP, Addr: cfg.ListenAddr, HandoverOnUpgrade: true},
		{Name: starter.TLS, Addr: cfg.TLSListenAddr, HandoverOnUpgrade: true},
	} {
		if c.Addr == "" {
			continue
		}

		var srv *grpc.Server
		if c.HandoverOnUpgrade {
			srv, err = gitalyServerFactory.CreateExternal(c.IsSecure())
			if err != nil {
				return fmt.Errorf("create external gRPC server: %w", err)
			}
		} else {
			srv, err = gitalyServerFactory.CreateInternal()
			if err != nil {
				return fmt.Errorf("create internal gRPC server: %w", err)
			}
		}

		setup.RegisterAll(srv, &service.Dependencies{
			Cfg:                cfg,
			RubyServer:         rubySrv,
			GitalyHookManager:  hookManager,
			TransactionManager: transactionManager,
			StorageLocator:     locator,
			ClientPool:         conns,
			GitCmdFactory:      gitCmdFactory,
			Linguist:           ling,
			CatfileCache:       catfileCache,
			DiskCache:          diskCache,
			PackObjectsCache:   streamCache,
			Git2goExecutor:     git2goExecutor,
			UpdaterWithHooks:   updaterWithHooks,
		})
		b.RegisterStarter(starter.New(c, srv))
	}

	if addr := cfg.PrometheusListenAddr; addr != "" {
		b.RegisterStarter(func(listen bootstrap.ListenFunc, _ chan<- error, _ *prometheus.CounterVec) error {
			l, err := listen("tcp", addr)
			if err != nil {
				return err
			}

			log.WithField("address", addr).Info("starting prometheus listener")

			go func() {
				if err := monitoring.Start(
					monitoring.WithListener(l),
					monitoring.WithBuildInformation(
						version.GetVersion(),
						version.GetBuildTime()),
					monitoring.WithBuildExtraLabels(
						map[string]string{"git_version": gitVersion.String()},
					)); err != nil {
					log.WithError(err).Error("Unable to serve prometheus")
				}
			}()

			return nil
		})
	}

	for _, shard := range cfg.Storages {
		if err := storage.WriteMetadataFile(shard.Path); err != nil {
			// TODO should this be a return? https://gitlab.com/gitlab-org/gitaly/issues/1893
			log.WithError(err).Error("Unable to write gitaly metadata file")
		}
	}

	if err := b.Start(); err != nil {
		return fmt.Errorf("unable to start the bootstrap: %v", err)
	}

	shutdownWorkers, err := gitalyServerFactory.StartWorkers(ctx, glog.Default(), cfg)
	if err != nil {
		return fmt.Errorf("initialize auxiliary workers: %v", err)
	}
	defer shutdownWorkers()

	defer func() {
		if err := cgroups.NewManager(cfg.Cgroups).Cleanup(); err != nil {
			log.WithError(err).Warn("error cleaning up cgroups")
		}
	}()

	gracefulStopTicker := helper.NewTimerTicker(cfg.GracefulRestartTimeout.Duration())
	defer gracefulStopTicker.Stop()

	return b.Wait(gracefulStopTicker, gitalyServerFactory.GracefulStop)
}
