package supervisor

import (
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
)

var (
	rssGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "gitaly_supervisor_rss_bytes",
			Help: "Resident set size of supervised processes, in bytes.",
		},
		[]string{"name"},
	)
	healthCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "gitaly_supervisor_health_checks",
			Help: "Count of Gitaly supervisor health checks",
		},
		[]string{"name", "status"},
	)
)

func init() {
	prometheus.MustRegister(rssGauge)
	prometheus.MustRegister(healthCounter)
}

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
			rss := 1024 * getRss(mp.pid)
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

// getRss returns RSS in kilobytes.
func getRss(pid int) int {
	// I tried adding a library to do this but it seemed like overkill
	// and YAGNI compared to doing this one 'ps' call.
	psRss, err := exec.Command("ps", "-o", "rss=", "-p", strconv.Itoa(pid)).Output()
	if err != nil {
		return 0
	}

	rss, err := strconv.Atoi(strings.TrimSpace(string(psRss)))
	if err != nil {
		return 0
	}

	return rss
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
