package main

import (
	"flag"
	"fmt"
	"os"

	log "github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/internal/bootstrap"
	"gitlab.com/gitlab-org/gitaly/internal/bootstrap/starter"
	"gitlab.com/gitlab-org/gitaly/internal/config"
	"gitlab.com/gitlab-org/gitaly/internal/config/sentry"
	"gitlab.com/gitlab-org/gitaly/internal/git"
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

	configPath := flag.Arg(0)
	if err := loadConfig(configPath); err != nil {
		log.WithError(err).WithField("config_path", configPath).Fatal("load config")
	}
	config.ConfigureLogging()

	// gitaly-wrapper is supposed to set config.EnvUpgradesEnabled in order to enable graceful upgrades
	b, err := bootstrap.New(bootstrap.PidFile(), bootstrap.UpgradesEnabled())
	if err != nil {
		log.WithError(err).Fatal("init bootstrap")
	}

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

	sentry.ConfigureSentry(version.GetVersion(), sentry.Config(config.Config.Logging.Sentry))
	config.ConfigurePrometheus()
	config.ConfigureConcurrencyLimits()
	tracing.Initialize(tracing.WithServiceName("gitaly"))

	tempdir.StartCleaning()

	log.WithError(run(b)).Error("shutting down")
}

// Inside here we can use deferred functions. This is needed because
// log.Fatal bypasses deferred functions.
func run(b *bootstrap.Bootstrap) error {
	servers := bootstrap.NewGitalyServerFactory()
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
				if err := monitoring.Serve(
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
		if err := storage.WriteMetadataFile(shard); err != nil {
			// TODO should this be a return? https://gitlab.com/gitlab-org/gitaly/issues/1893
			log.WithError(err).Error("Unable to write gitaly metadata file")
		}
	}

	if err := b.Start(); err != nil {
		return fmt.Errorf("unable to start the bootstrap: %v", err)
	}

	log.Info("Bootstrap started")

	if err := servers.StartRuby(); err != nil {
		return fmt.Errorf("initialize gitaly-ruby: %v", err)
	}

	log.Info("Bootstrap waiting")
	return b.Wait()
}
