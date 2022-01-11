package main

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	"github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/v14/internal/bootstrap"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper/env"
	"gitlab.com/gitlab-org/gitaly/v14/internal/log"
	"gitlab.com/gitlab-org/gitaly/v14/internal/ps"
	"golang.org/x/sys/unix"
)

const (
	envJSONLogging = "WRAPPER_JSON_LOGGING"
)

func main() {
	var logFormat string
	if jsonLogging() {
		logFormat = "json"
	}

	log.Configure(log.Loggers, logFormat, "")

	if len(os.Args) < 2 {
		logrus.Fatalf("usage: %s forking_binary [args]", os.Args[0])
	}

	gitalyBin, gitalyArgs := os.Args[1], os.Args[2:]

	logger := log.Default().WithField("wrapper", os.Getpid())
	logger.Info("Wrapper started")

	pidFilePath := os.Getenv(bootstrap.EnvPidFile)
	if pidFilePath == "" {
		logger.Fatalf("missing pid file ENV variable %q", bootstrap.EnvPidFile)
	}
	logger.WithField("pid_file", pidFilePath).Info("finding process")

	process, err := findProcess(pidFilePath)
	if err != nil && !isRecoverable(err) {
		logger.WithError(err).Fatal("find process")
	} else if err != nil {
		logger.WithError(err).Error("find process")
	}

	if process != nil && isGitaly(process, gitalyBin) {
		logger.Info("adopting a process")
	} else {
		logger.Info("spawning a process")

		proc, err := spawnGitaly(gitalyBin, gitalyArgs)
		if err != nil {
			logger.WithError(err).Fatal("spawn gitaly")
		}

		process = proc
	}

	logger = logger.WithField("process", process.Pid)
	logger.Info("monitoring process")

	forwardSignals(process, logger)

	// wait
	for isAlive(process) {
		time.Sleep(1 * time.Second)
	}

	logger.Error("wrapper for process shutting down")
}

func isRecoverable(err error) bool {
	var numError *strconv.NumError
	return os.IsNotExist(err) || errors.As(err, &numError)
}

func findProcess(pidFilePath string) (*os.Process, error) {
	pid, err := readPIDFile(pidFilePath)
	if err != nil {
		return nil, err
	}

	// os.FindProcess on unix do not return an error if the process does not exist
	process, err := os.FindProcess(pid)
	if err != nil {
		return nil, err
	}

	if isAlive(process) {
		return process, nil
	}

	return nil, nil
}

func spawnGitaly(bin string, args []string) (*os.Process, error) {
	cmd := exec.Command(bin, args...)
	cmd.Env = append(os.Environ(), fmt.Sprintf("%s=true", bootstrap.EnvUpgradesEnabled))

	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Start(); err != nil {
		return nil, err
	}

	// This cmd.Wait() is crucial. Without it we cannot detect if the command we just spawned has crashed.
	go cmd.Wait()

	return cmd.Process, nil
}

func isRuntimeSig(s os.Signal) bool {
	return s == unix.SIGURG
}

func forwardSignals(gitaly *os.Process, log *logrus.Entry) {
	sigs := make(chan os.Signal, 1)
	go func() {
		for sig := range sigs {
			// In go1.14+, the go runtime issues SIGURG as an interrupt
			// to support pre-emptible system calls on Linux. We ignore
			// this signal since it's not relevant to the Gitaly process.
			if isRuntimeSig(sig) {
				continue
			}

			log.WithField("signal", sig).Warning("forwarding signal")

			if err := gitaly.Signal(sig); err != nil {
				log.WithField("signal", sig).WithError(err).Error("can't forward the signal")
			}
		}
	}()

	signal.Notify(sigs)
}

func readPIDFile(pidFilePath string) (int, error) {
	data, err := os.ReadFile(pidFilePath)
	if err != nil {
		return 0, err
	}

	return strconv.Atoi(string(data))
}

func isAlive(p *os.Process) bool {
	// After p exits, and after it gets reaped, this p.Signal will fail. It is crucial that p gets reaped.
	// If p was spawned by the current process, it will get reaped from a goroutine that does cmd.Wait().
	// If p was spawned by someone else we rely on them to reap it, or on p to become an orphan.
	// In the orphan case p should get reaped by the OS (PID 1).
	return p.Signal(syscall.Signal(0)) == nil
}

func isGitaly(p *os.Process, gitalyBin string) bool {
	command, err := ps.Comm(p.Pid)
	if err != nil {
		return false
	}

	if filepath.Base(command) == filepath.Base(gitalyBin) {
		return true
	}

	return false
}

func jsonLogging() bool {
	enabled, _ := env.GetBool(envJSONLogging, false)
	return enabled
}
