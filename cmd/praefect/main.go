package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"

	"gitlab.com/gitlab-org/gitaly/internal/bootstrap"
	"gitlab.com/gitlab-org/gitaly/internal/bootstrap/starter"
	"gitlab.com/gitlab-org/gitaly/internal/config/sentry"
	"gitlab.com/gitlab-org/gitaly/internal/log"
	"gitlab.com/gitlab-org/gitaly/internal/praefect"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/config"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/conn"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/datastore"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/protoregistry"
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

func main() {
	flag.Parse()

	// If invoked with -version
	if *flagVersion {
		fmt.Println(praefect.GetVersionString())
		os.Exit(0)
	}

	conf, err := configure()
	if err != nil {
		logger.Fatal(err)
	}

	starterConfigs, err := getStarterConfigs(conf.SocketPath, conf.ListenAddr)
	if err != nil {
		logger.Fatalf("%s", err)
	}

	if err := run(starterConfigs, conf); err != nil {
		logger.Fatalf("%v", err)
	}
}

func configure() (config.Config, error) {
	var conf config.Config

	if *flagConfig == "" {
		return conf, errNoConfigFile
	}

	conf, err := config.FromFile(*flagConfig)
	if err != nil {
		return conf, fmt.Errorf("error reading config file: %v", err)
	}

	if err := conf.Validate(); err != nil {
		return conf, err
	}

	logger = conf.ConfigureLogger()
	tracing.Initialize(tracing.WithServiceName("praefect"))

	if conf.PrometheusListenAddr != "" {
		logger.WithField("address", conf.PrometheusListenAddr).Info("Starting prometheus listener")
		conf.Prometheus.Configure()

		go func() {
			if err := monitoring.Serve(
				monitoring.WithListenerAddress(conf.PrometheusListenAddr),
				monitoring.WithBuildInformation(praefect.GetVersion(), praefect.GetBuildTime())); err != nil {
				logger.WithError(err).Errorf("Unable to start healthcheck listener: %v", conf.PrometheusListenAddr)
			}
		}()
	}

	logger.WithField("version", praefect.GetVersionString()).Info("Starting Praefect")

	sentry.ConfigureSentry(version.GetVersion(), conf.Sentry)

	return conf, nil
}

func run(cfgs []starter.Config, conf config.Config) error {
	clientConnections := conn.NewClientConnections()

	for _, virtualStorage := range conf.VirtualStorages {
		for _, node := range virtualStorage.Nodes {
			if _, err := clientConnections.GetConnection(node.Storage); err == nil {
				continue
			}
			if err := clientConnections.RegisterNode(node.Storage, node.Address, node.Token); err != nil {
				return fmt.Errorf("failed to register %s: %s", node.Address, err)
			}

			logger.WithField("node_address", node.Address).Info("registered gitaly node")
		}
	}

	var (
		// top level server dependencies
		ds           = datastore.NewInMemory(conf)
		coordinator  = praefect.NewCoordinator(logger, ds, clientConnections, conf, protoregistry.GitalyProtoFileDescriptors...)
		repl         = praefect.NewReplMgr("default", logger, ds, clientConnections)
		srv          = praefect.NewServer(ds, coordinator, repl, logger, clientConnections, conf)
		serverErrors = make(chan error, 1)
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	b, err := bootstrap.New()
	if err != nil {
		return fmt.Errorf("unable to create a bootstrap: %v", err)
	}

	srv.RegisterServices()

	b.StopAction = srv.GracefulStop
	for _, cfg := range cfgs {
		b.RegisterStarter(starter.New(cfg, srv))
	}

	if err := b.Start(); err != nil {
		return fmt.Errorf("unable to start the bootstrap: %v", err)
	}

	go func() { serverErrors <- b.Wait() }()
	go func() { serverErrors <- repl.ProcessBacklog(ctx) }()

	go coordinator.FailoverRotation()

	return <-serverErrors
}

func getStarterConfigs(socketPath, listenAddr string) ([]starter.Config, error) {
	var cfgs []starter.Config
	if socketPath != "" {
		if err := os.RemoveAll(socketPath); err != nil {
			return nil, err
		}

		cleanPath := strings.TrimPrefix(socketPath, "unix:")

		cfgs = append(cfgs, starter.Config{Name: starter.Unix, Addr: cleanPath})

		logger.WithField("address", socketPath).Info("listening on unix socket")
	}

	if listenAddr != "" {
		cleanAddr := strings.TrimPrefix(listenAddr, "tcp://")

		cfgs = append(cfgs, starter.Config{Name: starter.TCP, Addr: cleanAddr})

		logger.WithField("address", listenAddr).Info("listening at tcp address")
	}

	return cfgs, nil
}
