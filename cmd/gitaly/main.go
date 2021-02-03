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

	log.WithField("version", version.GetVersionString()).Info("Starting Gitaly")

	configPath := flag.Arg(0)
	if err := loadConfig(configPath); err != nil {
		log.WithError(err).WithField("config_path", configPath).Fatal("load config")
	}

	glog.Configure(config.Config.Logging.Format, config.Config.Logging.Level)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cgroupsManager := cgroups.NewManager(config.Config.Cgroups)
	if err := cgroupsManager.Setup(); err != nil {
		log.WithError(err).Fatal("failed setting up cgroups")
	}
	defer func() {
		if err := cgroupsManager.Cleanup(); err != nil {
			log.WithError(err).Warn("error cleaning up cgroups")
		}
	}()

	gitVersion, err := git.Version(ctx)
	if err != nil {
		log.WithError(err).Fatal("Git version detection")
	}

	supported, err := git.SupportedVersion(gitVersion)
	if err != nil {
		log.WithError(err).Fatal("Git version comparison")
	}
	if !supported {
		log.Fatalf("unsupported Git version: %q", gitVersion)
	}

	b, err := bootstrap.New()
	if err != nil {
		log.WithError(err).Fatal("init bootstrap")
	}

	sentry.ConfigureSentry(version.GetVersion(), sentry.Config(config.Config.Logging.Sentry))
	config.Config.Prometheus.Configure()
	config.ConfigureConcurrencyLimits(config.Config)
	tracing.Initialize(tracing.WithServiceName("gitaly"))

	tempdir.StartCleaning(config.Config.Storages, time.Hour)

	log.WithError(run(config.Config, b)).Error("shutting down")
}

// Inside here we can use deferred functions. This is needed because
// log.Fatal bypasses deferred functions.
func run(cfg config.Cfg, b *bootstrap.Bootstrap) error {
	var gitlabAPI hook.GitlabAPI
	var err error

	transactionManager := transaction.NewManager(cfg)
	prometheus.MustRegister(transactionManager)

	hookManager := hook.Manager(hook.DisabledManager{})

	locator := config.NewLocator(cfg)

	if config.SkipHooks() {
		log.Warn("skipping GitLab API client creation since hooks are bypassed via GITALY_TESTING_NO_GIT_HOOKS")
	} else {
		gitlabAPI, err = hook.NewGitlabAPI(cfg.Gitlab, cfg.TLS)
		if err != nil {
			log.Fatalf("could not create GitLab API client: %v", err)
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

	ctx := context.Background()
	shutdownWorkers, err := servers.StartWorkers(ctx, glog.Default(), cfg)
	if err != nil {
		return fmt.Errorf("initialize auxiliary workers: %v", err)
	}
	defer shutdownWorkers()

	return b.Wait(cfg.GracefulRestartTimeout.Duration())
}
