package config

import (
	opentracing "github.com/opentracing/opentracing-go"
	log "github.com/sirupsen/logrus"
	jaegercfg "github.com/uber/jaeger-client-go/config"
)

// ConfigureTracing configures global tracing
func ConfigureTracing() {
	traceCfg, err := jaegercfg.FromEnv()
	if err != nil {
		log.WithError(err).Info("Skipping jaeger configuration step")
	} else {
		traceCfg.ServiceName = "gitaly"
		tracer, closer, err := traceCfg.NewTracer()
		if err != nil {
			log.WithError(err).Warn("Could not initialize jaeger tracer")
		} else {
			defer closer.Close()
			opentracing.SetGlobalTracer(tracer)
		}
	}

}
