package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/client"
	"gitlab.com/gitlab-org/gitaly/internal/backchannel"
	"gitlab.com/gitlab-org/gitaly/internal/bootstrap"
	"gitlab.com/gitlab-org/gitaly/internal/bootstrap/starter"
	"gitlab.com/gitlab-org/gitaly/internal/cgroups"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config/sentry"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/hook"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/linguist"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/rubyserver"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/server"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/service/setup"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/transaction"
	glog "gitlab.com/gitlab-org/gitaly/internal/log"
	"gitlab.com/gitlab-org/gitaly/internal/storage"
	"gitlab.com/gitlab-org/gitaly/internal/tempdir"
	"gitlab.com/gitlab-org/gitaly/internal/version"
	"gitlab.com/gitlab-org/labkit/monitoring"
	"gitlab.com/gitlab-org/labkit/tracing"
)

var (
	flagVersion = flag.Bool("version", false, "Print version and exit")
)

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
		return config.Cfg{}, err
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

	if err := verifyGitVersion(cfg); err != nil {
		return config.Cfg{}, err
	}

	sentry.ConfigureSentry(version.GetVersion(), sentry.Config(cfg.Logging.Sentry))
	cfg.Prometheus.Configure()
	config.ConfigureConcurrencyLimits(cfg)
	tracing.Initialize(tracing.WithServiceName("gitaly"))

	return cfg, nil
}

func verifyGitVersion(cfg config.Cfg) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	gitVersion, err := git.CurrentVersion(ctx, git.NewExecCommandFactory(cfg))
	if err != nil {
		return fmt.Errorf("git version detection: %w", err)
	}

	if !gitVersion.IsSupported() {
		return fmt.Errorf("unsupported Git version: %q", gitVersion)
	}
	return nil
}

func run(cfg config.Cfg) error {
	tempdir.StartCleaning(cfg.Storages, time.Hour)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, err := bootstrap.New()
	if err != nil {
		return fmt.Errorf("init bootstrap: %w", err)
	}

	registry := backchannel.NewRegistry()
	transactionManager := transaction.NewManager(cfg, registry)
	prometheus.MustRegister(transactionManager)

	hookManager := hook.Manager(hook.DisabledManager{})

	locator := config.NewLocator(cfg)

	if config.SkipHooks() {
		log.Warn("skipping GitLab API client creation since hooks are bypassed via GITALY_TESTING_NO_GIT_HOOKS")
	} else {
		gitlabAPI, err := hook.NewGitlabAPI(cfg.Gitlab, cfg.TLS)
		if err != nil {
			return fmt.Errorf("could not create GitLab API client: %w", err)
		}

		hm := hook.NewManager(locator, transactionManager, gitlabAPI, cfg)

		hookManager = hm
	}

	conns := client.NewPoolWithOptions(
		client.WithDialer(client.HealthCheckDialer(client.DialContext)),
		client.WithDialOptions(client.FailOnNonTempDialError()...),
	)
	defer conns.Close()

	gitCmdFactory := git.NewExecCommandFactory(cfg)
	prometheus.MustRegister(gitCmdFactory)

	gitalyServerFactory := server.NewGitalyServerFactory(cfg, registry)
	defer gitalyServerFactory.Stop()

	ling, err := linguist.New(cfg)
	if err != nil {
		return fmt.Errorf("linguist instance creation: %w", err)
	}

	b.StopAction = gitalyServerFactory.GracefulStop

	rubySrv := rubyserver.New(cfg)
	if err := rubySrv.Start(); err != nil {
		return fmt.Errorf("initialize gitaly-ruby: %v", err)
	}
	defer rubySrv.Stop()

	for _, c := range []starter.Config{
		{starter.Unix, cfg.SocketPath},
		{starter.Unix, cfg.GitalyInternalSocketPath()},
		{starter.TCP, cfg.ListenAddr},
		{starter.TLS, cfg.TLSListenAddr},
	} {
		if c.Addr == "" {
			continue
		}

		srv, err := gitalyServerFactory.Create(c.IsSecure())
		if err != nil {
			return fmt.Errorf("create gRPC server: %w", err)
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
		})
		b.RegisterStarter(starter.New(c, srv))
	}

	if addr := cfg.PrometheusListenAddr; addr != "" {
		b.RegisterStarter(func(listen bootstrap.ListenFunc, _ chan<- error) error {
			l, err := listen("tcp", addr)
			if err != nil {
				return err
			}

			ctx, cancel := context.WithCancel(context.TODO())
			defer cancel()

			gitVersion, err := git.CurrentVersion(ctx, gitCmdFactory)
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

	return b.Wait(cfg.GracefulRestartTimeout.Duration())
}
