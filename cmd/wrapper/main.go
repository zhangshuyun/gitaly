package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"os/signal"
	"path"
	"strconv"
	"syscall"
	"time"

	"github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/internal/bootstrap"
	"gitlab.com/gitlab-org/gitaly/internal/ps"
)

const (
	envJSONLogging = "WRAPPER_JSON_LOGGING"
)

func main() {
	if jsonLogging() {
		logrus.SetFormatter(&logrus.JSONFormatter{})
	}

	if len(os.Args) < 2 {
		logrus.Fatalf("usage: %s forking_binary [args]", os.Args[0])
	}

	bin, args := os.Args[1], os.Args[2:]

	log := logrus.WithField("wrapper", os.Getpid()).WithField("binary", bin)
	log.Info("Wrapper started")

	if bootstrap.PidFile() == "" {
		log.Fatalf("missing pid file ENV variable %q", bootstrap.PidFileEnvVar)
	}

	log.WithField("pid_file", bootstrap.PidFile()).Info("finding process")
	proc, err := findProcess()
	if err != nil {
		log.WithError(err).Fatal("find process")
	}

	if proc != nil && matches(proc, bin) {
		log.Info("adopting a process")
	} else {
		log.Info("spawning a process")

		newProc, err := spawn(bin, args)
		if err != nil {
			log.WithError(err).Fatal("spawn process")
		}

		proc = newProc
	}

	log = log.WithField("process", proc.Pid)
	log.Info("monitoring process")

	forwardSignals(proc, log)

	// wait
	for isAlive(proc) {
		time.Sleep(1 * time.Second)
	}

	log.Error("wrapper for process shutting down")
}

func findProcess() (*os.Process, error) {
	pid, err := getPid()
	if err != nil && !os.IsNotExist(err) {
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

func spawn(bin string, args []string) (*os.Process, error) {
	cmd := exec.Command(bin, args...)
	cmd.Env = append(os.Environ(), fmt.Sprintf("%s=true", bootstrap.UpgradesEnabledEnvVar))

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

func forwardSignals(proc *os.Process, log *logrus.Entry) {
	sigs := make(chan os.Signal, 1)
	go func() {
		for sig := range sigs {
			log.WithField("signal", sig).Warning("forwarding signal")

			if err := proc.Signal(sig); err != nil {
				log.WithField("signal", sig).WithError(err).Error("can't forward the signal")
			}
		}
	}()

	signal.Notify(sigs)
}

func getPid() (int, error) {
	data, err := ioutil.ReadFile(bootstrap.PidFile())
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

func matches(p *os.Process, bin string) bool {
	command, err := ps.Comm(p.Pid)
	if err != nil {
		return false
	}

	if path.Base(command) == path.Base(bin) {
		return true
	}

	return false
}

func jsonLogging() bool {
	return os.Getenv(envJSONLogging) == "true"
}
