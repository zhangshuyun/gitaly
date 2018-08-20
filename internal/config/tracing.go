package config

import (
	"io"

	opentracing "github.com/opentracing/opentracing-go"
	log "github.com/sirupsen/logrus"
	jaegercfg "github.com/uber/jaeger-client-go/config"
)

// ConfigureTracing configures global tracing
func ConfigureTracing() io.Closer {
	traceCfg, err := jaegercfg.FromEnv()
	if err != nil {
		log.WithError(err).Info("Skipping jaeger configuration step")
		return nil
	}

	traceCfg.ServiceName = "gitaly"
	tracer, closer, err := traceCfg.NewTracer()
	if err != nil {
		log.WithError(err).Warn("Could not initialize jaeger tracer")
		return nil
	}

	opentracing.SetGlobalTracer(tracer)
	return closer
}
