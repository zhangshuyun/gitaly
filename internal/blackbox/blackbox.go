package blackbox

import (
	"context"
	"fmt"
	"net"
	"os"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/stats"
	"gitlab.com/gitlab-org/gitaly/v14/internal/version"
	"gitlab.com/gitlab-org/labkit/monitoring"
)

type httpReferenceDiscoveryMetrics struct {
	firstPacket    *prometheus.GaugeVec
	totalTime      *prometheus.GaugeVec
	advertisedRefs *prometheus.GaugeVec
}

func (m httpReferenceDiscoveryMetrics) measure(probeName string, rd stats.HTTPReferenceDiscovery) {
	m.firstPacket.WithLabelValues(probeName).Set(rd.FirstGitPacket().Seconds())
	m.totalTime.WithLabelValues(probeName).Set(rd.ResponseBody().Seconds())
	m.advertisedRefs.WithLabelValues(probeName).Set(float64(len(rd.Refs())))
}

// Describe is used to describe Prometheus metrics.
func (m httpReferenceDiscoveryMetrics) Describe(descs chan<- *prometheus.Desc) {
	prometheus.DescribeByCollect(m, descs)
}

// Collect is used to collect Prometheus metrics.
func (m httpReferenceDiscoveryMetrics) Collect(metrics chan<- prometheus.Metric) {
	m.firstPacket.Collect(metrics)
	m.totalTime.Collect(metrics)
	m.advertisedRefs.Collect(metrics)
}

type httpPostStats interface {
	ResponseBody() time.Duration
	BandFirstPacket(b string) time.Duration
	BandPayloadSize(b string) int64
}

type httpPostMetrics struct {
	totalTime           *prometheus.GaugeVec
	firstProgressPacket *prometheus.GaugeVec
	firstPackPacket     *prometheus.GaugeVec
	packBytes           *prometheus.GaugeVec
}

func (m httpPostMetrics) measure(probeName string, stats httpPostStats) {
	m.totalTime.WithLabelValues(probeName).Set(stats.ResponseBody().Seconds())
	m.firstProgressPacket.WithLabelValues(probeName).Set(stats.BandFirstPacket("progress").Seconds())
	m.firstPackPacket.WithLabelValues(probeName).Set(stats.BandFirstPacket("pack").Seconds())
	m.packBytes.WithLabelValues(probeName).Set(float64(stats.BandPayloadSize("pack")))
}

// Describe is used to describe Prometheus metrics.
func (m httpPostMetrics) Describe(descs chan<- *prometheus.Desc) {
	prometheus.DescribeByCollect(m, descs)
}

// Collect is used to collect Prometheus metrics.
func (m httpPostMetrics) Collect(metrics chan<- prometheus.Metric) {
	m.totalTime.Collect(metrics)
	m.firstProgressPacket.Collect(metrics)
	m.firstPackPacket.Collect(metrics)
	m.packBytes.Collect(metrics)
}

// Blackbox encapsulates all details required to run the blackbox prober.
type Blackbox struct {
	cfg Config

	fetchReferenceDiscoveryMetrics httpReferenceDiscoveryMetrics
	httpPostMetrics                httpPostMetrics
	wantedRefs                     *prometheus.GaugeVec
}

// New creates a new Blackbox structure.
func New(cfg Config) Blackbox {
	return Blackbox{
		cfg: cfg,
		fetchReferenceDiscoveryMetrics: httpReferenceDiscoveryMetrics{
			firstPacket:    newGauge("get_first_packet_seconds", "Time to first Git packet in GET /info/refs response"),
			totalTime:      newGauge("get_total_time_seconds", "Time to receive entire GET /info/refs response"),
			advertisedRefs: newGauge("get_advertised_refs", "Number of Git refs advertised in GET /info/refs"),
		},
		httpPostMetrics: httpPostMetrics{
			totalTime:           newGauge("post_total_time_seconds", "Time to receive entire POST /upload-pack response"),
			firstProgressPacket: newGauge("post_first_progress_packet_seconds", "Time to first progress band Git packet in POST /upload-pack response"),
			firstPackPacket:     newGauge("post_first_pack_packet_seconds", "Time to first pack band Git packet in POST /upload-pack response"),
			packBytes:           newGauge("post_pack_bytes", "Number of pack band bytes in POST /upload-pack response"),
		},
		wantedRefs: newGauge("wanted_refs", "Number of Git refs selected for (fake) Git clone (branches + tags)"),
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
	b.fetchReferenceDiscoveryMetrics.Collect(metrics)
	b.httpPostMetrics.Collect(metrics)
	b.wantedRefs.Collect(metrics)
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
			entry := log.WithFields(map[string]interface{}{
				"probe": probe.Name,
				"type":  probe.Type,
			})

			entry.Info("starting probe")

			var err error
			switch probe.Type {
			case Fetch:
				err = b.fetch(probe)
			case Push:
				err = b.push(probe)
			default:
				err = fmt.Errorf("unsupported probe type: %q", probe.Type)
			}
			if err != nil {
				entry.WithError(err).Error("probe failed")
			}

			entry.Info("finished probe")
		}
	}
}

func servePrometheus(l net.Listener) error {
	return monitoring.Start(
		monitoring.WithListener(l),
		monitoring.WithBuildInformation(version.GetVersion(), version.GetBuildTime()),
	)
}

func (b Blackbox) fetch(probe Probe) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	clone, err := stats.PerformHTTPClone(ctx, probe.URL, probe.User, probe.Password, false)
	if err != nil {
		return err
	}

	setGauge := func(gv *prometheus.GaugeVec, value float64) {
		gv.WithLabelValues(probe.Name).Set(value)
	}

	b.fetchReferenceDiscoveryMetrics.measure(probe.Name, clone.ReferenceDiscovery)
	b.httpPostMetrics.measure(probe.Name, &clone.FetchPack)
	setGauge(b.wantedRefs, float64(clone.FetchPack.RefsWanted()))

	return nil
}

func (b Blackbox) push(probe Probe) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	commands := make([]stats.PushCommand, len(probe.Push.Commands))
	for i, command := range probe.Push.Commands {
		oldOID, err := git.NewObjectIDFromHex(command.OldOID)
		if err != nil {
			return fmt.Errorf("invalid old object ID for probe %q: %w", probe.Name, err)
		}

		newOID, err := git.NewObjectIDFromHex(command.NewOID)
		if err != nil {
			return fmt.Errorf("invalid new object ID for probe %q: %w", probe.Name, err)
		}

		commands[i] = stats.PushCommand{
			Reference: git.ReferenceName(command.Reference),
			OldOID:    oldOID,
			NewOID:    newOID,
		}
	}

	packfile, err := os.Open(probe.Push.Packfile)
	if err != nil {
		return fmt.Errorf("opening packfile for probe %q: %w", probe.Name, err)
	}
	defer packfile.Close()

	clone, err := stats.PerformHTTPPush(
		ctx, probe.URL, probe.User, probe.Password, commands, packfile, false)
	if err != nil {
		return err
	}

	b.httpPostMetrics.measure(probe.Name, &clone.SendPack)

	return nil
}
