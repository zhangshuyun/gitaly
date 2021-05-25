package blackbox

import (
	"context"
	"net"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/stats"
	"gitlab.com/gitlab-org/gitaly/v14/internal/version"
	"gitlab.com/gitlab-org/labkit/monitoring"
)

func Run(cfg *Config) error {
	listener, err := net.Listen("tcp", cfg.PrometheusListenAddr)
	if err != nil {
		return err
	}

	go runProbes(cfg)

	return servePrometheus(listener)
}

func runProbes(cfg *Config) {
	for ; ; time.Sleep(cfg.SleepDuration) {
		for _, probe := range cfg.Probes {
			doProbe(probe)
		}
	}
}

func servePrometheus(l net.Listener) error {
	return monitoring.Start(
		monitoring.WithListener(l),
		monitoring.WithBuildInformation(version.GetVersion(), version.GetBuildTime()),
	)
}

func doProbe(probe Probe) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	entry := log.WithField("probe", probe.Name)
	entry.Info("starting probe")

	clone := &stats.Clone{
		URL:      probe.URL,
		User:     probe.User,
		Password: probe.Password,
	}

	if err := clone.Perform(ctx); err != nil {
		entry.WithError(err).Error("probe failed")
		return
	}

	entry.Info("finished probe")

	setGauge := func(gv *prometheus.GaugeVec, value float64) {
		gv.WithLabelValues(probe.Name).Set(value)
	}

	setGauge(getFirstPacket, clone.Get.FirstGitPacket().Seconds())
	setGauge(getTotalTime, clone.Get.ResponseBody().Seconds())
	setGauge(getAdvertisedRefs, float64(len(clone.Get.Refs)))
	setGauge(wantedRefs, float64(clone.RefsWanted()))
	setGauge(postTotalTime, clone.Post.ResponseBody().Seconds())
	setGauge(postFirstProgressPacket, clone.Post.BandFirstPacket("progress").Seconds())
	setGauge(postFirstPackPacket, clone.Post.BandFirstPacket("pack").Seconds())
	setGauge(postPackBytes, float64(clone.Post.BandPayloadSize("pack")))
}
