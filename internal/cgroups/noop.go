package cgroups

import (
	"gitlab.com/gitlab-org/gitaly/v14/internal/command"
)

// NoopManager is a cgroups manager that does nothing
type NoopManager struct{}

func (cg *NoopManager) Setup() error {
	return nil
}

func (cg *NoopManager) AddCommand(cmd *command.Command) error {
	return nil
}

func (cg *NoopManager) Cleanup() error {
	return nil
}
