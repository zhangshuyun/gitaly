package command

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var inFlightCommandGauge = promauto.NewGauge(
	prometheus.GaugeOpts{
		Name: "gitaly_commands_running",
		Help: "Total number of processes currently being executed",
	},
)
