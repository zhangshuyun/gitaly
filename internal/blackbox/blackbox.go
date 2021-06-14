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

// Blackbox encapsulates all details required to run the blackbox prober.
type Blackbox struct {
	cfg Config

	getFirstPacket          *prometheus.GaugeVec
	getTotalTime            *prometheus.GaugeVec
	getAdvertisedRefs       *prometheus.GaugeVec
	wantedRefs              *prometheus.GaugeVec
	postTotalTime           *prometheus.GaugeVec
	postFirstProgressPacket *prometheus.GaugeVec
	postFirstPackPacket     *prometheus.GaugeVec
	postPackBytes           *prometheus.GaugeVec
}

// New creates a new Blackbox structure.
func New(cfg Config) Blackbox {
	return Blackbox{
		cfg:                     cfg,
		getFirstPacket:          newGauge("get_first_packet_seconds", "Time to first Git packet in GET /info/refs response"),
		getTotalTime:            newGauge("get_total_time_seconds", "Time to receive entire GET /info/refs response"),
		getAdvertisedRefs:       newGauge("get_advertised_refs", "Number of Git refs advertised in GET /info/refs"),
		wantedRefs:              newGauge("wanted_refs", "Number of Git refs selected for (fake) Git clone (branches + tags)"),
		postTotalTime:           newGauge("post_total_time_seconds", "Time to receive entire POST /upload-pack response"),
		postFirstProgressPacket: newGauge("post_first_progress_packet_seconds", "Time to first progress band Git packet in POST /upload-pack response"),
		postFirstPackPacket:     newGauge("post_first_pack_packet_seconds", "Time to first pack band Git packet in POST /upload-pack response"),
		postPackBytes:           newGauge("post_pack_bytes", "Number of pack band bytes in POST /upload-pack response"),
	}
}

func newGauge(name string, help string) *prometheus.GaugeVec {
	return prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "gitaly_blackbox",
			Subsystem: "git_http",
			Name:      name,
			Help:      help,
		},
		[]string{"probe"},
	)
}

// Describe is used to describe Prometheus metrics.
func (b Blackbox) Describe(descs chan<- *prometheus.Desc) {
	prometheus.DescribeByCollect(b, descs)
}

// Collect is used to collect Prometheus metrics.
func (b Blackbox) Collect(metrics chan<- prometheus.Metric) {
	b.getFirstPacket.Collect(metrics)
	b.getTotalTime.Collect(metrics)
	b.getAdvertisedRefs.Collect(metrics)
	b.wantedRefs.Collect(metrics)
	b.postTotalTime.Collect(metrics)
	b.postFirstProgressPacket.Collect(metrics)
	b.postFirstPackPacket.Collect(metrics)
	b.postPackBytes.Collect(metrics)
}

// Run starts the blackbox. It sets up and serves the Prometheus listener and starts a Goroutine
// which runs the probes.
func (b Blackbox) Run() error {
	listener, err := net.Listen("tcp", b.cfg.PrometheusListenAddr)
	if err != nil {
		return err
	}

	go b.runProbes()

	return servePrometheus(listener)
}

func (b Blackbox) runProbes() {
	for ; ; time.Sleep(b.cfg.sleepDuration) {
		for _, probe := range b.cfg.Probes {
			b.doProbe(probe)
		}
	}
}

func servePrometheus(l net.Listener) error {
	return monitoring.Start(
		monitoring.WithListener(l),
		monitoring.WithBuildInformation(version.GetVersion(), version.GetBuildTime()),
	)
}

func (b Blackbox) doProbe(probe Probe) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	entry := log.WithField("probe", probe.Name)
	entry.Info("starting probe")

	clone, err := stats.PerformClone(ctx, probe.URL, probe.User, probe.Password, false)
	if err != nil {
		entry.WithError(err).Error("probe failed")
		return
	}

	entry.Info("finished probe")

	setGauge := func(gv *prometheus.GaugeVec, value float64) {
		gv.WithLabelValues(probe.Name).Set(value)
	}

	setGauge(b.getFirstPacket, clone.ReferenceDiscovery.FirstGitPacket().Seconds())
	setGauge(b.getTotalTime, clone.ReferenceDiscovery.ResponseBody().Seconds())
	setGauge(b.getAdvertisedRefs, float64(len(clone.ReferenceDiscovery.Refs())))
	setGauge(b.wantedRefs, float64(clone.FetchPack.RefsWanted()))
	setGauge(b.postTotalTime, clone.FetchPack.ResponseBody().Seconds())
	setGauge(b.postFirstProgressPacket, clone.FetchPack.BandFirstPacket("progress").Seconds())
	setGauge(b.postFirstPackPacket, clone.FetchPack.BandFirstPacket("pack").Seconds())
	setGauge(b.postPackBytes, float64(clone.FetchPack.BandPayloadSize("pack")))
}
