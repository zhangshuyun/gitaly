package main

import (
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/cloudflare/tableflip"
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
	"google.golang.org/grpc"
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
	upg, err := tableflip.New(tableflip.Options{})
	if err != nil {
		log.WithError(err).Fatal("Unable to tableflip")
	}
	defer upg.Stop()

	if socketPath := config.Config.SocketPath; socketPath != "" {
		l, err := createUnixListener(upg, socketPath)
		if err != nil {
			log.WithError(err).Fatal("configure unix listener")
		}
		log.WithField("address", socketPath).Info("listening on unix socket")
		listeners = append(listeners, l)
	}

	if addr := config.Config.ListenAddr; addr != "" {
		l, err := upg.Fds.Listen("tcp", addr)
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
			l, err := upg.Fds.Listen("tcp", config.Config.PrometheusListenAddr)
			if err != nil {
				log.WithError(err).Fatal("No tableflip")
			}

			err = http.Serve(l, promMux)
			if err != nil {
				log.WithError(err).Fatal("Unable to serve")
			}
		}()
	}

	run(upg, listeners)
}

func createUnixListener(upg *tableflip.Upgrader, socketPath string) (net.Listener, error) {
	if err := os.Remove(socketPath); err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	l, err := upg.Fds.Listen("unix", socketPath)
	return connectioncounter.New("unix", l), err
}

// This function will never return
func run(upg *tableflip.Upgrader, listeners []net.Listener) {
	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGHUP)
		for range sig {
			log.Println("SIGHUPSIGHUPSIGHUPSIGHUP")

			err := upg.Upgrade()
			if err != nil {
				log.Println("Upgrade failed:", err)
				continue
			}

			log.Println("Upgrade succeeded")
		}
	}()

	signals := []os.Signal{syscall.SIGTERM, syscall.SIGINT}
	termCh := make(chan os.Signal, len(signals))
	signal.Notify(termCh, signals...)

	ruby, err := rubyserver.Start()
	if err != nil {
		log.WithError(err).Fatal("unable to start ruby server")
	}
	defer ruby.Stop()

	server := server.New(ruby)

	err = gracefulListen(server, upg, listeners)
	if err != nil {
		log.WithError(err).Fatal("unable to start ruby server")
	}
}

func gracefulListen(server *grpc.Server, upg *tableflip.Upgrader, listeners []net.Listener) error {
	defer server.Stop()

	serverErrors := make(chan error, len(listeners))
	for _, listener := range listeners {
		// Must pass the listener as a function argument because there is a race
		// between 'go' and 'for'.
		go func(l net.Listener) {
			serverErrors <- server.Serve(l)
		}(listener)
	}

	if err := upg.Ready(); err != nil {
		panic(err)
	}

	var err error
	select {
	case <-upg.Exit():
	case err = <-serverErrors:
	}

	if err != nil {
		log.WithError(err).Fatal("unable to listen")
	}

	time.AfterFunc(30*time.Second, func() {
		os.Exit(1)
	})

	server.GracefulStop()
	return nil
}
