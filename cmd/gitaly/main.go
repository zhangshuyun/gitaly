package main

import (
	"log"
	"net"
	"net/http"
	"os"

	_ "gitlab.com/gitlab-org/gitaly/internal/config"
	"gitlab.com/gitlab-org/gitaly/internal/service"
	"gitlab.com/gitlab-org/gitaly/internal/service/middleware/panichandler"

	"github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/kelseyhightower/envconfig"
	"github.com/mwitkow/go-grpc-middleware"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

// Config specifies the gitaly server configuration
type Config struct {
	SocketPath           string `split_words:"true"`
	PrometheusListenAddr string `split_words:"true"`
}

var version string

func main() {
	log.Println("Starting Gitaly", version)

	config := Config{}
	err := envconfig.Process("gitaly", &config)
	if err != nil {
		log.Fatal(err)
	}

	if config.SocketPath == "" {
		log.Fatal("GITALY_SOCKET_PATH environment variable is not set")
	}

	if err := os.Remove(config.SocketPath); err != nil && !os.IsNotExist(err) {
		log.Fatal(err)
	}

	listener, err := net.Listen("unix", config.SocketPath)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("Listening on socket", config.SocketPath)

	server := grpc.NewServer(
		grpc.StreamInterceptor(grpc_middleware.ChainStreamServer(
			panichandler.StreamPanicHandler,         // Panic Handler first: handle panics gracefully
			grpc_prometheus.StreamServerInterceptor, // Prometheus Metrics next: measure RPC times
		)),
		grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(
			panichandler.UnaryPanicHandler,         // Panic Handler first: handle panics gracefully
			grpc_prometheus.UnaryServerInterceptor, // Prometheus Metrics next: measure RPC times
		)),
	)

	service.RegisterAll(server)
	reflection.Register(server)

	// After all your registrations, make sure all of the Prometheus metrics are initialized.
	grpc_prometheus.Register(server)

	serverError := make(chan error, 2)
	go func() {
		serverError <- server.Serve(listener)
	}()

	if config.PrometheusListenAddr != "" {
		log.Print("Starting prometheus listener ", config.PrometheusListenAddr)
		promMux := http.NewServeMux()
		promMux.Handle("/metrics", promhttp.Handler())
		go func() {
			http.ListenAndServe(config.PrometheusListenAddr, promMux)
		}()
	}

	log.Fatal(<-serverError)
}
