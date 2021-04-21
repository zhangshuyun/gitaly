package supervisor

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	log "github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/internal/ps"
)

var (
	rssGauge = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "gitaly_supervisor_rss_bytes",
			Help: "Resident set size of supervised processes, in bytes.",
		},
		[]string{"name"},
	)
	healthCounter = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "gitaly_supervisor_health_checks_total",
			Help: "Count of Gitaly supervisor health checks",
		},
		[]string{"name", "status"},
	)
)

type monitorProcess struct {
	pid  int
	wait <-chan struct{}
}

func monitorRss(procs <-chan monitorProcess, done chan<- struct{}, events chan<- Event, name string, threshold int) {
	log.WithField("supervisor.name", name).WithField("supervisor.rss_threshold", threshold).Info("starting RSS monitor")

	t := time.NewTicker(15 * time.Second)
	defer t.Stop()

	defer close(done)

	for mp := range procs {
	monitorLoop:
		for {
			rss, err := ps.RSS(mp.pid)
			if err != nil {
				log.WithError(err).Warn("getting RSS")
			}

			// converts from kB to B
			rss *= 1024
			rssGauge.WithLabelValues(name).Set(float64(rss))

			if rss > 0 {
				event := Event{Type: MemoryLow, Pid: mp.pid}
				if rss > threshold {
					event.Type = MemoryHigh
				}

				select {
				case events <- event:
				case <-time.After(1 * time.Second):
					// Prevent sending stale events
				}
			}

			select {
			case <-mp.wait:
				break monitorLoop
			case <-t.C:
			}
		}
	}
}

func monitorHealth(f func() error, events chan<- Event, name string, shutdown <-chan struct{}) {
	for {
		e := Event{Error: f()}

		if e.Error != nil {
			e.Type = HealthBad
			healthCounter.WithLabelValues(name, "bad").Inc()
		} else {
			e.Type = HealthOK
			healthCounter.WithLabelValues(name, "ok").Inc()
		}

		select {
		case events <- e:
		case <-time.After(1 * time.Second):
			// Prevent sending stale events
		case <-shutdown:
			return
		}

		time.Sleep(15 * time.Second)
	}
}
