package main

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"os"

	"gitlab.com/gitlab-org/gitaly/internal/config"
	"gitlab.com/gitlab-org/gitaly/internal/connectioncounter"
	"gitlab.com/gitlab-org/gitaly/internal/service"
	"gitlab.com/gitlab-org/gitaly/internal/service/middleware/loghandler"
	"gitlab.com/gitlab-org/gitaly/internal/service/middleware/panichandler"

	"github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/mwitkow/go-grpc-middleware"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

var version string

func loadConfig() {
	switch {
	case len(os.Args) >= 2:
		cfgFile, err := os.Open(os.Args[1])
		if err != nil {
			log.Printf("warning: can not open file for reading: %q: %v", os.Args[1], err)
			break
		}
		defer cfgFile.Close()
		if err = config.Load(cfgFile); err != nil {
			log.Printf("warning: can not load configuration: %q: %v", os.Args[1], err)
		}

	default:
		log.Printf("warning: no configuration file given")
		if err := config.Load(nil); err != nil {
			log.Printf("warning: can not load configuration: %v", err)
		}
	}
}

func validateConfig() error {
	if config.Config.SocketPath == "" && config.Config.ListenAddr == "" {
		return fmt.Errorf("Must set $GITALY_SOCKET_PATH or $GITALY_LISTEN_ADDR")
	}

	return config.ValidateStorages()
}

func main() {
	log.Println("Starting Gitaly", version)

	loadConfig()

	if err := validateConfig(); err != nil {
		log.Fatal(err)
	}

	var listeners []net.Listener

	if socketPath := config.Config.SocketPath; socketPath != "" {
		l, err := createUnixListener(socketPath)
		if err != nil {
			log.Fatalf("configure unix listener: %v", err)
		}
		log.Printf("listening on unix socket %q", socketPath)
		listeners = append(listeners, l)
	}

	if addr := config.Config.ListenAddr; addr != "" {
		l, err := net.Listen("tcp", addr)
		if err != nil {
			log.Fatalf("configure tcp listener: %v", err)
		}
		log.Printf("listening at tcp address %q", addr)
		listeners = append(listeners, connectioncounter.New("tcp", l))
	}

	server := grpc.NewServer(
		grpc.StreamInterceptor(grpc_middleware.ChainStreamServer(
			panichandler.StreamPanicHandler,         // Panic Handler first: handle panics gracefully
			grpc_prometheus.StreamServerInterceptor, // Prometheus Metrics next: measure RPC times
			loghandler.StreamLogHandler,
		)),
		grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(
			panichandler.UnaryPanicHandler,         // Panic Handler first: handle panics gracefully
			grpc_prometheus.UnaryServerInterceptor, // Prometheus Metrics next: measure RPC times
			loghandler.UnaryLogHandler,
		)),
	)

	service.RegisterAll(server)
	reflection.Register(server)

	// After all your registrations, make sure all of the Prometheus metrics are initialized.
	grpc_prometheus.Register(server)

	serverError := make(chan error, len(listeners))
	for _, listener := range listeners {
		// Must pass the listener as a function argument because there is a race
		// between 'go' and 'for'.
		go func(l net.Listener) {
			serverError <- server.Serve(l)
		}(listener)
	}

	if config.Config.PrometheusListenAddr != "" {
		log.Print("Starting prometheus listener ", config.Config.PrometheusListenAddr)
		promMux := http.NewServeMux()
		promMux.Handle("/metrics", promhttp.Handler())
		go func() {
			http.ListenAndServe(config.Config.PrometheusListenAddr, promMux)
		}()
	}

	log.Fatal(<-serverError)
}

func createUnixListener(socketPath string) (net.Listener, error) {
	if err := os.Remove(socketPath); err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	l, err := net.Listen("unix", socketPath)
	return connectioncounter.New("unix", l), err
}
