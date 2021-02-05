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
	"gitlab.com/gitlab-org/gitaly/internal/bootstrap"
	"gitlab.com/gitlab-org/gitaly/internal/bootstrap/starter"
	"gitlab.com/gitlab-org/gitaly/internal/cgroups"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config/sentry"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/hook"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/server"
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

func loadConfig(configPath string) error {
	cfgFile, err := os.Open(configPath)
	if err != nil {
		return err
	}
	defer cfgFile.Close()

	cfg, err := config.Load(cfgFile)
	if err != nil {
		return err
	}

	if err := cfg.Validate(); err != nil {
		return err
	}

	config.Config = cfg
	return nil
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
	if err := configure(flag.Arg(0)); err != nil {
		log.Fatal(err)
	}
	log.WithError(run(config.Config)).Error("shutting down")
	log.Info("Gitaly stopped")
}

func configure(configPath string) error {
	if err := loadConfig(configPath); err != nil {
		return fmt.Errorf("load config: config_path %q: %w", configPath, err)
	}

	glog.Configure(config.Config.Logging.Format, config.Config.Logging.Level)

	if err := cgroups.NewManager(config.Config.Cgroups).Setup(); err != nil {
		return fmt.Errorf("failed setting up cgroups: %w", err)
	}

	if err := verifyGitVersion(); err != nil {
		return err
	}

	sentry.ConfigureSentry(version.GetVersion(), sentry.Config(config.Config.Logging.Sentry))
	config.Config.Prometheus.Configure()
	config.ConfigureConcurrencyLimits(config.Config)
	tracing.Initialize(tracing.WithServiceName("gitaly"))

	return nil
}

func verifyGitVersion() error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	gitVersion, err := git.Version(ctx)
	if err != nil {
		return fmt.Errorf("git version detection: %w", err)
	}

	supported, err := git.SupportedVersion(gitVersion)
	if err != nil {
		return fmt.Errorf("git version comparison: %w", err)
	}
	if !supported {
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

	transactionManager := transaction.NewManager(cfg)
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

	servers := server.NewGitalyServerFactory(cfg, hookManager, transactionManager, conns, locator, gitCmdFactory)
	defer servers.Stop()

	b.StopAction = servers.GracefulStop

	for _, c := range []starter.Config{
		{starter.Unix, cfg.SocketPath},
		{starter.Unix, cfg.GitalyInternalSocketPath()},
		{starter.TCP, cfg.ListenAddr},
		{starter.TLS, cfg.TLSListenAddr},
	} {
		if c.Addr == "" {
			continue
		}

		b.RegisterStarter(starter.New(c, servers))
	}

	if addr := cfg.PrometheusListenAddr; addr != "" {
		b.RegisterStarter(func(listen bootstrap.ListenFunc, _ chan<- error) error {
			l, err := listen("tcp", addr)
			if err != nil {
				return err
			}

			ctx, cancel := context.WithCancel(context.TODO())
			defer cancel()

			gitVersion, err := git.Version(ctx)
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
						map[string]string{"git_version": gitVersion},
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

	if err := servers.StartRuby(); err != nil {
		return fmt.Errorf("initialize gitaly-ruby: %v", err)
	}

	shutdownWorkers, err := servers.StartWorkers(ctx, glog.Default(), cfg)
	if err != nil {
		return fmt.Errorf("initialize auxiliary workers: %v", err)
	}
	defer shutdownWorkers()

	defer func() {
		if err := cgroups.NewManager(config.Config.Cgroups).Cleanup(); err != nil {
			log.WithError(err).Warn("error cleaning up cgroups")
		}
	}()

	return b.Wait(cfg.GracefulRestartTimeout.Duration())
}
