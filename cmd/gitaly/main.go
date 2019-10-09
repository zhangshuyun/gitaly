package main

import (
	"flag"
	"fmt"
	"net/http"
	"os"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	log "github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/internal/bootstrap"
	"gitlab.com/gitlab-org/gitaly/internal/config"
	"gitlab.com/gitlab-org/gitaly/internal/config/sentry"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/server"
	"gitlab.com/gitlab-org/gitaly/internal/storage"
	"gitlab.com/gitlab-org/gitaly/internal/tempdir"
	"gitlab.com/gitlab-org/gitaly/internal/version"
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

// registerServerVersionPromGauge registers a label with the current server version
// making it easy to see what versions of Gitaly are running across a cluster
func registerServerVersionPromGauge() {
	gitVersion, err := git.Version()
	if err != nil {
		fmt.Printf("git version: %v\n", err)
		os.Exit(1)
	}
	gitlabBuildInfoGauge := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "gitlab_build_info",
		Help: "Current build info for this GitLab Service",
		ConstLabels: prometheus.Labels{
			"version":     version.GetVersion(),
			"built":       version.GetBuildTime(),
			"git_version": gitVersion,
		},
	})

	prometheus.MustRegister(gitlabBuildInfoGauge)
	gitlabBuildInfoGauge.Set(1)
}

func flagUsage() {
	fmt.Println(version.GetVersionString())
	fmt.Printf("Usage: %v [OPTIONS] configfile\n", os.Args[0])
	flag.PrintDefaults()
}

func main() {
	flag.Usage = flagUsage
	flag.Parse()

	// gitaly-wrapper is supposed to set config.EnvUpgradesEnabled in order to enable graceful upgrades
	_, isWrapped := os.LookupEnv(config.EnvUpgradesEnabled)
	b, err := bootstrap.New(os.Getenv(config.EnvPidFile), isWrapped)
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
	registerServerVersionPromGauge()

	configPath := flag.Arg(0)
	if err := loadConfig(configPath); err != nil {
		log.WithError(err).WithField("config_path", configPath).Fatal("load config")
	}

	config.ConfigureLogging()
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
	servers := bootstrap.NewServerFactory()
	defer servers.Stop()

	b.StopAction = servers.GracefulStop

	for _, c := range []starterConfig{
		{unix, config.Config.SocketPath},
		{tcp, config.Config.ListenAddr},
		{tls, config.Config.TLSListenAddr},
	} {
		if c.addr == "" {
			continue
		}

		b.RegisterStarter(gitalyStarter(c, servers))
	}

	if addr := config.Config.PrometheusListenAddr; addr != "" {
		b.RegisterStarter(func(listen bootstrap.ListenFunc, _ chan<- error) error {
			l, err := listen("tcp", addr)
			if err != nil {
				return err
			}

			log.WithField("address", addr).Info("starting prometheus listener")

			promMux := http.NewServeMux()
			promMux.Handle("/metrics", promhttp.Handler())

			server.AddPprofHandlers(promMux)

			go func() {
				if err := http.Serve(l, promMux); err != nil {
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

	if err := servers.StartRuby(); err != nil {
		return fmt.Errorf("initialize gitaly-ruby: %v", err)
	}

	return b.Wait()
}
