package supervisor

import (
	"context"
	"fmt"
	"io"
	"os/exec"
	"sync"
	"time"

	"github.com/kelseyhightower/envconfig"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	log "github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper"
	"gitlab.com/gitlab-org/labkit/tracing"
)

// Config holds configuration for the circuit breaker of the respawn loop.
type Config struct {
	// GITALY_SUPERVISOR_CRASH_THRESHOLD
	CrashThreshold int `split_words:"true" default:"5"`
	// GITALY_SUPERVISOR_CRASH_WAIT_TIME
	CrashWaitTime time.Duration `split_words:"true" default:"1m"`
	// GITALY_SUPERVISOR_CRASH_RESET_TIME
	CrashResetTime time.Duration `split_words:"true" default:"1m"`
}

var (
	startCounter = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "gitaly_supervisor_starts_total",
			Help: "Number of starts of supervised processes.",
		},
		[]string{"name"},
	)

	envInjector = tracing.NewEnvInjector()
)

// NewConfigFromEnv returns Config initialised from environment variables or an error.
func NewConfigFromEnv() (Config, error) {
	var config Config
	if err := envconfig.Process("gitaly_supervisor", &config); err != nil {
		return Config{}, err
	}
	return config, nil
}

// Process represents a running process.
type Process struct {
	Name string

	config          Config
	memoryThreshold int
	events          chan<- Event
	healthCheck     func() error

	// Information to start the process
	env  []string
	args []string
	dir  string

	// Shutdown
	shutdown chan struct{}
	done     chan struct{}
	stopOnce sync.Once
}

// New creates a new process instance.
func New(config Config, name string, env []string, args []string, dir string, memoryThreshold int, events chan<- Event, healthCheck func() error) (*Process, error) {
	if len(args) < 1 {
		return nil, fmt.Errorf("need at least one argument")
	}

	p := &Process{
		Name:            name,
		config:          config,
		memoryThreshold: memoryThreshold,
		events:          events,
		healthCheck:     healthCheck,
		env:             envInjector(context.Background(), env),
		args:            args,
		dir:             dir,
		shutdown:        make(chan struct{}),
		done:            make(chan struct{}),
	}

	go watch(p)
	return p, nil
}

func (p *Process) start(logger *log.Entry) (*exec.Cmd, error) {
	startCounter.WithLabelValues(p.Name).Inc()

	cmd := exec.Command(p.args[0], p.args[1:]...)
	cmd.Env = p.env
	cmd.Dir = p.dir
	cmd.Stdout = logger.WriterLevel(log.InfoLevel)
	cmd.Stderr = logger.WriterLevel(log.InfoLevel)
	return cmd, cmd.Start()
}

func (p *Process) notifyEvent(eventType EventType, pid int) {
	select {
	case p.events <- Event{Type: eventType, Pid: pid}:
	case <-time.After(1 * time.Second):
		// Timeout
	}
}

func watch(p *Process) {
	// Count crashes to prevent a tight respawn loop. This is a 'circuit breaker'.
	crashes := 0

	logger := log.WithField("supervisor.args", p.args).WithField("supervisor.name", p.Name)

	// Use a buffered channel because we don't want to block the respawn loop
	// on the monitor goroutine.
	monitorChan := make(chan monitorProcess, p.config.CrashThreshold)
	monitorDone := make(chan struct{})
	go monitorRss(monitorChan, monitorDone, p.events, p.Name, p.memoryThreshold)

	healthShutdown := make(chan struct{})
	if p.healthCheck != nil {
		go monitorHealth(p.healthCheck, p.events, p.Name, healthShutdown)
	}

	notificationTicker := helper.NewTimerTicker(1 * time.Minute)
	defer notificationTicker.Stop()

	crashResetTicker := helper.NewTimerTicker(p.config.CrashResetTime)
	defer crashResetTicker.Stop()

spawnLoop:
	for {
		if crashes >= p.config.CrashThreshold {
			logger.Warn("opening circuit breaker")
			select {
			case <-p.shutdown:
				break spawnLoop
			case <-time.After(p.config.CrashWaitTime):
				logger.Warn("closing circuit breaker")
				crashes = 0
			}
		}

		select {
		case <-p.shutdown:
			break spawnLoop
		default:
		}

		cmd, err := p.start(logger)
		if err != nil {
			go p.notifyEvent(Crash, -1)
			crashes++
			logger.WithError(err).Error("start failed")
			continue
		}
		pid := cmd.Process.Pid
		go p.notifyEvent(Up, pid)
		logger.WithField("supervisor.pid", pid).Warn("spawned")

		waitCh := make(chan struct{})
		go func(cmd *exec.Cmd, waitCh chan struct{}) {
			defer close(waitCh)

			err := cmd.Wait()
			cmd.Stdout.(io.WriteCloser).Close()
			cmd.Stderr.(io.WriteCloser).Close()
			logger.WithError(err).Warn("exited")
		}(cmd, waitCh)

		monitorChan <- monitorProcess{pid: pid, wait: waitCh}

		// We create the tickers before the spawn loop so we don't recreate the channels
		// every time we loop. Furthermore, stopping those tickers via deferred function
		// calls would only clean them up after we stop watching. So instead, we just reset
		// both timers here.
		notificationTicker.Reset()
		crashResetTicker.Reset()

	waitLoop:
		for {
			select {
			case <-notificationTicker.C():
				go p.notifyEvent(Up, pid)

				// We repeat this idempotent notification because its delivery is not
				// guaranteed.
				notificationTicker.Reset()
			case <-crashResetTicker.C():
				// We do not reset the crash reset ticker because we only need to
				// reset crashes once.
				crashes = 0
			case <-waitCh:
				go p.notifyEvent(Crash, pid)
				crashes++
				break waitLoop
			case <-p.shutdown:
				if cmd.Process != nil {
					cmd.Process.Kill()
				}
				<-waitCh
				break spawnLoop
			}
		}
	}

	close(healthShutdown)
	close(monitorChan)
	<-monitorDone
	close(p.done)
}

// Stop terminates the process.
func (p *Process) Stop() {
	if p == nil {
		return
	}

	p.stopOnce.Do(func() {
		close(p.shutdown)
	})

	select {
	case <-p.done:
	case <-time.After(1 * time.Second):
		// Don't wait for shutdown forever
	}
}
