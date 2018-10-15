package main

import (
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	log "github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/internal/config"
	"gitlab.com/gitlab-org/gitaly/internal/connectioncounter"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/linguist"
	"gitlab.com/gitlab-org/gitaly/internal/rubyserver"
	"gitlab.com/gitlab-org/gitaly/internal/server"
	"gitlab.com/gitlab-org/gitaly/internal/tempdir"
	"gitlab.com/gitlab-org/gitaly/internal/version"
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

	if err := config.Validate(); err != nil {
		return err
	}

	if err := linguist.LoadColors(); err != nil {
		return fmt.Errorf("load linguist colors: %v", err)
	}

	return nil
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
	config.ConfigureSentry(version.GetVersion())
	config.ConfigurePrometheus()
	config.ConfigureConcurrencyLimits()

	tempdir.StartCleaning()

	var listeners []net.Listener

	if socketPath := config.Config.SocketPath; socketPath != "" {
		l, err := createUnixListener(socketPath)
		if err != nil {
			log.WithError(err).Fatal("configure unix listener")
		}
		log.WithField("address", socketPath).Info("listening on unix socket")
		listeners = append(listeners, l)
	}

	if addr := config.Config.ListenAddr; addr != "" {
		l, err := net.Listen("tcp", addr)
		if err != nil {
			log.WithError(err).Fatal("configure tcp listener")
		}

		log.WithField("address", addr).Info("listening at tcp address")
		listeners = append(listeners, connectioncounter.New("tcp", l))
	}

	if config.Config.PrometheusListenAddr != "" {
		log.WithField("address", config.Config.PrometheusListenAddr).Info("Starting prometheus listener")
		promMux := http.NewServeMux()
		promMux.Handle("/metrics", promhttp.Handler())

		server.AddPprofHandlers(promMux)

		go func() {
			http.ListenAndServe(config.Config.PrometheusListenAddr, promMux)
		}()
	}

	log.WithError(run(listeners)).Fatal("shutting down")
}

func createUnixListener(socketPath string) (net.Listener, error) {
	if err := os.Remove(socketPath); err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	l, err := net.Listen("unix", socketPath)
	return connectioncounter.New("unix", l), err
}

// Inside here we can use deferred functions. This is needed because
// log.Fatal bypasses deferred functions.
func run(listeners []net.Listener) error {
	signals := []os.Signal{syscall.SIGTERM, syscall.SIGINT}
	termCh := make(chan os.Signal, len(signals))
	signal.Notify(termCh, signals...)

	ruby, err := rubyserver.Start()
	if err != nil {
		return err
	}
	defer ruby.Stop()

	server := server.New(ruby)
	defer server.Stop()

	serverErrors := make(chan error, len(listeners))
	for _, listener := range listeners {
		// Must pass the listener as a function argument because there is a race
		// between 'go' and 'for'.
		go func(l net.Listener) {
			serverErrors <- server.Serve(l)
		}(listener)
	}

	select {
	case s := <-termCh:
		err = fmt.Errorf("received signal %q", s)
	case err = <-serverErrors:
	}

	return err
}
