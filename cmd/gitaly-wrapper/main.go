package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	"github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/internal/bootstrap"
	"gitlab.com/gitlab-org/gitaly/internal/log"
	"gitlab.com/gitlab-org/gitaly/internal/ps"
)

const (
	envJSONLogging = "WRAPPER_JSON_LOGGING"
)

func main() {
	if jsonLogging() {
		logrus.SetFormatter(&logrus.JSONFormatter{TimestampFormat: log.LogTimestampFormat})
	}

	if len(os.Args) < 2 {
		logrus.Fatalf("usage: %s forking_binary [args]", os.Args[0])
	}

	gitalyBin, gitalyArgs := os.Args[1], os.Args[2:]

	log := logrus.WithField("wrapper", os.Getpid())
	log.Info("Wrapper started")

	if pidFile() == "" {
		log.Fatalf("missing pid file ENV variable %q", bootstrap.EnvPidFile)
	}

	log.WithField("pid_file", pidFile()).Info("finding gitaly")
	gitaly, err := findGitaly()
	if err != nil && !isRecoverable(err) {
		log.WithError(err).Fatal("find gitaly")
	} else if err != nil {
		log.WithError(err).Error("find gitaly")
	}

	if gitaly != nil && isGitaly(gitaly, gitalyBin) {
		log.Info("adopting a process")
	} else {
		log.Info("spawning a process")

		proc, err := spawnGitaly(gitalyBin, gitalyArgs)
		if err != nil {
			log.WithError(err).Fatal("spawn gitaly")
		}

		gitaly = proc
	}

	log = log.WithField("gitaly", gitaly.Pid)
	log.Info("monitoring gitaly")

	forwardSignals(gitaly, log)

	// wait
	for isAlive(gitaly) {
		time.Sleep(1 * time.Second)
	}

	log.Error("wrapper for gitaly shutting down")
}

func isRecoverable(err error) bool {
	_, isNumError := err.(*strconv.NumError)
	return os.IsNotExist(err) || isNumError
}

func findGitaly() (*os.Process, error) {
	pid, err := getPid()
	if err != nil {
		return nil, err
	}

	// os.FindProcess on unix do not return an error if the process does not exist
	gitaly, err := os.FindProcess(pid)
	if err != nil {
		return nil, err
	}

	if isAlive(gitaly) {
		return gitaly, nil
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

func forwardSignals(gitaly *os.Process, log *logrus.Entry) {
	sigs := make(chan os.Signal, 1)
	go func() {
		for sig := range sigs {
			log.WithField("signal", sig).Warning("forwarding signal")

			if err := gitaly.Signal(sig); err != nil {
				log.WithField("signal", sig).WithError(err).Error("can't forward the signal")
			}
		}
	}()

	signal.Notify(sigs)
}

func getPid() (int, error) {
	data, err := ioutil.ReadFile(pidFile())
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

func pidFile() string {
	return os.Getenv(bootstrap.EnvPidFile)
}

func jsonLogging() bool {
	return os.Getenv(envJSONLogging) == "true"
}
