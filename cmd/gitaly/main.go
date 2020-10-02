package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/v13/internal/bootstrap"
	"gitlab.com/gitlab-org/gitaly/v13/internal/bootstrap/starter"
	"gitlab.com/gitlab-org/gitaly/v13/internal/git"
	"gitlab.com/gitlab-org/gitaly/v13/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v13/internal/gitaly/config/sentry"
	"gitlab.com/gitlab-org/gitaly/v13/internal/gitaly/hook"
	"gitlab.com/gitlab-org/gitaly/v13/internal/gitaly/server"
	glog "gitlab.com/gitlab-org/gitaly/v13/internal/log"
	"gitlab.com/gitlab-org/gitaly/v13/internal/storage"
	"gitlab.com/gitlab-org/gitaly/v13/internal/tempdir"
	"gitlab.com/gitlab-org/gitaly/v13/internal/version"
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

	if err = config.Load(cfgFile); err != nil {
		return err
	}

	return config.Validate()
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

	config.ConfigureLogging()

	b, err := bootstrap.New()
	if err != nil {
		log.WithError(err).Fatal("init bootstrap")
	}

	sentry.ConfigureSentry(version.GetVersion(), sentry.Config(config.Config.Logging.Sentry))
	config.Config.Prometheus.Configure()
	config.ConfigureConcurrencyLimits()
	tracing.Initialize(tracing.WithServiceName("gitaly"))

	tempdir.StartCleaning(time.Hour)

	log.WithError(run(b)).Error("shutting down")
}

// Inside here we can use deferred functions. This is needed because
// log.Fatal bypasses deferred functions.
func run(b *bootstrap.Bootstrap) error {
	var gitlabAPI hook.GitlabAPI
	var err error

	if config.SkipHooks() {
		log.Warn("skipping GitLab API client creation since hooks are bypassed via GITALY_TESTING_NO_GIT_HOOKS")
	} else {
		gitlabAPI, err = hook.NewGitlabAPI(config.Config.Gitlab)
		if err != nil {
			log.Fatalf("could not create GitLab API client: %v", err)
		}
	}

	hookManager := hook.NewManager(gitlabAPI, config.Config)
	prometheus.MustRegister(hookManager)

	servers := server.NewGitalyServerFactory(hookManager)
	defer servers.Stop()

	b.StopAction = servers.GracefulStop

	for _, c := range []starter.Config{
		{starter.Unix, config.Config.SocketPath},
		{starter.Unix, config.GitalyInternalSocketPath()},
		{starter.TCP, config.Config.ListenAddr},
		{starter.TLS, config.Config.TLSListenAddr},
	} {
		if c.Addr == "" {
			continue
		}

		b.RegisterStarter(starter.New(c, servers))
	}

	if addr := config.Config.PrometheusListenAddr; addr != "" {
		b.RegisterStarter(func(listen bootstrap.ListenFunc, _ chan<- error) error {
			l, err := listen("tcp", addr)
			if err != nil {
				return err
			}

			gitVersion, err := git.Version()
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

	for _, shard := range config.Config.Storages {
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
	shutdownWorkers, err := servers.StartWorkers(ctx, glog.Default(), config.Config)
	if err != nil {
		return fmt.Errorf("initialize auxiliary workers: %v", err)
	}
	defer shutdownWorkers()

	return b.Wait(config.Config.GracefulRestartTimeout.Duration())
}
