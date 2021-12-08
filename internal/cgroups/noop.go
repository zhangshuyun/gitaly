package cgroups

import (
	"github.com/prometheus/client_golang/prometheus"
	"gitlab.com/gitlab-org/gitaly/v14/internal/command"
)

// NoopManager is a cgroups manager that does nothing
type NoopManager struct{}

//nolint: revive,stylecheck // This is unintentionally missing documentation.
func (cg *NoopManager) Setup() error {
	return nil
}

//nolint: revive,stylecheck // This is unintentionally missing documentation.
func (cg *NoopManager) AddCommand(cmd *command.Command) error {
	return nil
}

//nolint: revive,stylecheck // This is unintentionally missing documentation.
func (cg *NoopManager) Cleanup() error {
	return nil
}

// Describe does nothing
func (cg *NoopManager) Describe(ch chan<- *prometheus.Desc) {}

// Collect does nothing
func (cg *NoopManager) Collect(ch chan<- prometheus.Metric) {}
