package cgroups

import (
	"fmt"
	"hash/crc32"
	"os"
	"strings"

	"github.com/containerd/cgroups"
	specs "github.com/opencontainers/runtime-spec/specs-go"
	"gitlab.com/gitlab-org/gitaly/v14/internal/command"
	cgroupscfg "gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config/cgroups"
)

// CGroupV1Manager is the manager for cgroups v1
type CGroupV1Manager struct {
	cfg       cgroupscfg.Config
	hierarchy func() ([]cgroups.Subsystem, error)
}

func newV1Manager(cfg cgroupscfg.Config) *CGroupV1Manager {
	return &CGroupV1Manager{
		cfg: cfg,
		hierarchy: func() ([]cgroups.Subsystem, error) {
			return defaultSubsystems(cfg.Mountpoint)
		},
	}
}

func (cg *CGroupV1Manager) Setup() error {
	resources := &specs.LinuxResources{}

	if cg.cfg.CPU.Enabled {
		resources.CPU = &specs.LinuxCPU{
			Shares: &cg.cfg.CPU.Shares,
		}
	}

	if cg.cfg.Memory.Enabled {
		resources.Memory = &specs.LinuxMemory{
			Limit: &cg.cfg.Memory.Limit,
		}
	}

	for i := 0; i < int(cg.cfg.Count); i++ {
		_, err := cgroups.New(cg.hierarchy, cgroups.StaticPath(cg.cgroupPath(i)), resources)
		if err != nil {
			return fmt.Errorf("failed creating cgroup: %w", err)
		}
	}

	return nil
}

func (cg *CGroupV1Manager) AddCommand(cmd *command.Command) error {
	checksum := crc32.ChecksumIEEE([]byte(strings.Join(cmd.Args(), "")))
	groupID := uint(checksum) % cg.cfg.Count
	cgroupPath := cg.cgroupPath(int(groupID))

	control, err := cgroups.Load(cg.hierarchy, cgroups.StaticPath(cgroupPath))
	if err != nil {
		return fmt.Errorf("failed loading %s cgroup: %w", cgroupPath, err)
	}

	if err := control.Add(cgroups.Process{Pid: cmd.Pid()}); err != nil {
		// Command could finish so quickly before we can add it to a cgroup, so
		// we don't consider it an error.
		if strings.Contains(err.Error(), "no such process") {
			return nil
		}
		return fmt.Errorf("failed adding process to cgroup: %w", err)
	}

	return nil
}

func (cg *CGroupV1Manager) Cleanup() error {
	processCgroupPath := cg.currentProcessCgroup()

	control, err := cgroups.Load(cg.hierarchy, cgroups.StaticPath(processCgroupPath))
	if err != nil {
		return fmt.Errorf("failed loading cgroup %s: %w", processCgroupPath, err)
	}

	if err := control.Delete(); err != nil {
		return fmt.Errorf("failed cleaning up cgroup %s: %w", processCgroupPath, err)
	}

	return nil
}

func (cg *CGroupV1Manager) cgroupPath(groupID int) string {
	return fmt.Sprintf("/%s/shard-%d", cg.currentProcessCgroup(), groupID)
}

func (cg *CGroupV1Manager) currentProcessCgroup() string {
	return fmt.Sprintf("/%s/gitaly-%d", cg.cfg.HierarchyRoot, os.Getpid())
}

func defaultSubsystems(root string) ([]cgroups.Subsystem, error) {
	subsystems := []cgroups.Subsystem{
		cgroups.NewMemory(root),
		cgroups.NewCpu(root),
	}

	return subsystems, nil
}
