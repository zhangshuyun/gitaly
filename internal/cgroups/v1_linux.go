package cgroups

import (
	"fmt"
	"hash/crc32"
	"os"
	"strings"

	"github.com/containerd/cgroups"
	specs "github.com/opencontainers/runtime-spec/specs-go"
	"github.com/prometheus/client_golang/prometheus"
	"gitlab.com/gitlab-org/gitaly/v14/internal/command"
	cgroupscfg "gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config/cgroups"
	"gitlab.com/gitlab-org/gitaly/v14/internal/log"
)

// CGroupV1Manager is the manager for cgroups v1
type CGroupV1Manager struct {
	cfg                         cgroupscfg.Config
	hierarchy                   func() ([]cgroups.Subsystem, error)
	paths                       map[string]interface{}
	memoryFailedTotal, cpuUsage *prometheus.GaugeVec
	procs                       *prometheus.GaugeVec
}

func newV1Manager(cfg cgroupscfg.Config) *CGroupV1Manager {
	return &CGroupV1Manager{
		cfg: cfg,
		hierarchy: func() ([]cgroups.Subsystem, error) {
			return defaultSubsystems(cfg.Mountpoint)
		},
		paths: make(map[string]interface{}),
		memoryFailedTotal: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "gitaly_cgroup_memory_failed_total",
				Help: "Number of memory usage hits limits",
			},
			[]string{"path"},
		),
		cpuUsage: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "gitaly_cgroup_cpu_usage",
				Help: "CPU Usage of Cgroup",
			},
			[]string{"path", "type"},
		),
		procs: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "gitaly_cgroup_procs_total",
				Help: "Total number of procs",
			},
			[]string{"path", "subsystem"},
		),
	}
}

//nolint: revive,stylecheck // This is unintentionally missing documentation.
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

//nolint: revive,stylecheck // This is unintentionally missing documentation.
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

	cg.paths[cgroupPath] = struct{}{}

	return nil
}

// Collect collects metrics from the cgroups controller
func (cg *CGroupV1Manager) Collect(ch chan<- prometheus.Metric) {
	path := cg.currentProcessCgroup()
	logger := log.Default().WithField("cgroup_path", path)
	control, err := cgroups.Load(cg.hierarchy, cgroups.StaticPath(path))
	if err != nil {
		logger.WithError(err).Warn("unable to load cgroup controller")
		return
	}

	if metrics, err := control.Stat(); err != nil {
		logger.WithError(err).Warn("unable to get cgroup stats")
	} else {
		memoryMetric := cg.memoryFailedTotal.WithLabelValues(path)
		memoryMetric.Set(float64(metrics.Memory.Usage.Failcnt))
		ch <- memoryMetric

		cpuUserMetric := cg.cpuUsage.WithLabelValues(path, "user")
		cpuUserMetric.Set(float64(metrics.CPU.Usage.User))
		ch <- cpuUserMetric

		cpuKernelMetric := cg.cpuUsage.WithLabelValues(path, "kernel")
		cpuKernelMetric.Set(float64(metrics.CPU.Usage.Kernel))
		ch <- cpuKernelMetric
	}

	if subsystems, err := cg.hierarchy(); err != nil {
		logger.WithError(err).Warn("unable to get cgroup hierarchy")
	} else {
		for _, subsystem := range subsystems {
			processes, err := control.Processes(subsystem.Name(), true)
			if err != nil {
				logger.WithField("subsystem", subsystem.Name()).
					WithError(err).
					Warn("unable to get process list")
				continue
			}

			procsMetric := cg.procs.WithLabelValues(path, string(subsystem.Name()))
			procsMetric.Set(float64(len(processes)))
			ch <- procsMetric
		}
	}
}

// Describe describes the cgroup metrics that Collect provides
func (cg *CGroupV1Manager) Describe(ch chan<- *prometheus.Desc) {
	prometheus.DescribeByCollect(cg, ch)
}

//nolint: revive,stylecheck // This is unintentionally missing documentation.
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
