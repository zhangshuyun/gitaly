package command

import (
	"context"
	"fmt"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"github.com/kelseyhightower/envconfig"
	"github.com/prometheus/client_golang/prometheus"
)

const logDurationThreshold = 5 * time.Millisecond

var (
	spawnTokens chan struct{}
	spawnConfig SpawnConfig

	spawnTimeoutCount = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "gitaly_spawn_timeouts_total",
			Help: "Number of process spawn timeouts",
		},
	)
)

// SpawnConfig holds configuration for command spawning timeouts and parallelism.
type SpawnConfig struct {
	// This default value (10 seconds) is very high. Spawning should take
	// milliseconds or less. If we hit 10 seconds, something is wrong, and
	// failing the request will create breathing room. Can be modified at
	// runtime with the GITALY_COMMAND_SPAWN_TIMEOUT environment variable.
	Timeout time.Duration `split_words:"true" default:"10s"`

	// MaxSpawnParallel limits the number of goroutines that can spawn a
	// process at the same time. These parallel spawns will contend for a
	// single lock (syscall.ForkLock) in exec.Cmd.Start(). Can be modified at
	// runtime with the GITALY_COMMAND_SPAWN_MAX_PARALLEL variable.
	//
	// Note that this does not limit the total number of child processes that
	// can be attached to Gitaly at the same time. It only limits the rate at
	// which we can create new child processes.
	MaxParallel int `split_words:"true" default:"10"`
}

func init() {
	envconfig.MustProcess("gitaly_command_spawn", &spawnConfig)
	spawnTokens = make(chan struct{}, spawnConfig.MaxParallel)
	prometheus.MustRegister(spawnTimeoutCount)
}

func getSpawnToken(ctx context.Context) (putToken func(), err error) {
	// Go has a global lock (syscall.ForkLock) for spawning new processes.
	// This select statement is a safety valve to prevent lots of Gitaly
	// requests from piling up behind the ForkLock if forking for some reason
	// slows down. This has happened in real life, see
	// https://gitlab.com/gitlab-org/gitaly/issues/823.
	start := time.Now()

	select {
	case spawnTokens <- struct{}{}:
		logTime(ctx, start, "spawn token acquired")

		return func() {
			<-spawnTokens
		}, nil
	case <-time.After(spawnConfig.Timeout):
		logTime(ctx, start, "spawn token timeout")
		spawnTimeoutCount.Inc()

		return nil, spawnTimeoutError{fmt.Errorf("process spawn timed out after %v", spawnConfig.Timeout)}
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func logTime(ctx context.Context, start time.Time, msg string) {
	delta := time.Since(start)
	if delta < logDurationThreshold {
		return
	}

	ctxlogrus.Extract(ctx).WithField("spawn_queue_ms", delta.Seconds()*1000).Info(msg)
}
