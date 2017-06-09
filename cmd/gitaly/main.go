package main

import (
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	log "github.com/Sirupsen/logrus"

	"gitlab.com/gitlab-org/gitaly/internal/config"
	"gitlab.com/gitlab-org/gitaly/internal/connectioncounter"
	"gitlab.com/gitlab-org/gitaly/internal/service"
	"gitlab.com/gitlab-org/gitaly/internal/service/middleware/loghandler"
	"gitlab.com/gitlab-org/gitaly/internal/service/middleware/panichandler"

	"github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/mwitkow/go-grpc-middleware"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"google.golang.org/grpc"
	"google.golang.org/grpc/grpclog"
	"google.golang.org/grpc/reflection"
)

var version string

func loadConfig() {
	switch {
	case len(os.Args) >= 2:
		cfgFile, err := os.Open(os.Args[1])
		if err != nil {
			log.WithFields(log.Fields{
				"filename": os.Args[1],
				"error":    err,
			}).Warn("can not open file for reading")
			break
		}
		defer cfgFile.Close()
		if err = config.Load(cfgFile); err != nil {
			log.WithFields(log.Fields{
				"filename": os.Args[1],
				"error":    err,
			}).Warn("can not load configuration")
		}

	default:
		log.Warn("no configuration file given")
		if err := config.Load(nil); err != nil {
			log.WithError(err).Warn("can not load configuration")
		}
	}
}

func validateConfig() error {
	if config.Config.SocketPath == "" && config.Config.ListenAddr == "" {
		return fmt.Errorf("Must set $GITALY_SOCKET_PATH or $GITALY_LISTEN_ADDR")
	}

	return config.ValidateStorages()
}

// registerServerVersionPromGauge registers a label with the current server version
// making it easy to see what versions of Gitaly are running across a cluster
func registerServerVersionPromGauge() {
	gitlabBuildInfoGauge := prometheus.NewGauge(prometheus.GaugeOpts{
		Name:        "gitlab_build_info",
		Help:        "Current build info for this GitLab Service",
		ConstLabels: prometheus.Labels{"version": version},
	})

	prometheus.MustRegister(gitlabBuildInfoGauge)
	gitlabBuildInfoGauge.Set(1)
}

func main() {
	log.WithField("version", version).Info("Starting Gitaly")
	registerServerVersionPromGauge()

	loadConfig()

	if err := validateConfig(); err != nil {
		log.Fatal(err)
	}

	config.ConfigureLogging()
	grpclog.SetLogger(log.StandardLogger())
	config.ConfigureSentry(version)
	config.ConfigurePrometheus()

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

	server := grpc.NewServer(
		grpc.StreamInterceptor(grpc_middleware.ChainStreamServer(
			loghandler.StreamLogHandler,
			grpc_prometheus.StreamServerInterceptor,
			panichandler.StreamPanicHandler, // Panic handler should remain last
		)),
		grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(
			loghandler.UnaryLogHandler,
			grpc_prometheus.UnaryServerInterceptor,
			panichandler.UnaryPanicHandler, // Panic handler should remain last
		)),
	)

	service.RegisterAll(server)
	reflection.Register(server)

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
		log.WithField("address", config.Config.PrometheusListenAddr).Info("Starting prometheus listener")
		promMux := http.NewServeMux()
		promMux.Handle("/metrics", promhttp.Handler())
		go func() {
			http.ListenAndServe(config.Config.PrometheusListenAddr, promMux)
		}()
	}

	hupCh := make(chan os.Signal, 1)
	signal.Notify(hupCh, syscall.SIGHUP)
	select {
	case <-listenSupervisor():
		log.Print("supervisor went away")
	case err := <-serverError:
		log.Printf("server error: %v", err)
	case <-hupCh:
		log.Printf("received SIGHUP")
	}

	stopDone := make(chan struct{})
	go func() {
		log.Print("starting graceful stop")
		server.GracefulStop()
		close(stopDone)
	}()
	select {
	case <-time.After(10 * time.Minute):
		log.Print("graceful stop timed out")
	case <-stopDone:
	}
	log.Print("exiting")
}

func createUnixListener(socketPath string) (net.Listener, error) {
	if err := os.Remove(socketPath); err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	l, err := net.Listen("unix", socketPath)
	return connectioncounter.New("unix", l), err
}

func listenSupervisor() <-chan struct{} {
	supervisor := os.NewFile(3, "supervisor")
	if _, err := supervisor.Stat(); err != nil {
		log.Fatal(err)
	}

	done := make(chan struct{})
	go func() {
		time.Sleep(time.Second)
		heartbeat := make(chan struct{})
		go func() {
			data := make([]byte, 1)
			for {
				n, err := supervisor.Read(data)
				if n > 0 && err == nil {
					heartbeat <- struct{}{}
				}
			}
		}()

		t := time.NewTicker(100 * time.Millisecond)
		ticksQuiet := 0
		for {
			select {
			case <-t.C:
				ticksQuiet += 1
				if ticksQuiet == 10 {
					log.Print("too many missed heartbeats from supervisor")
					close(done)
					return
				}
			case <-heartbeat:
				ticksQuiet = 0
			}
		}
	}()

	return done
}
