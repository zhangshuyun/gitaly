package testhelper

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"time"

	"gitlab.com/gitlab-org/gitaly/v14/internal/command/commandcounter"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper/text"
	"go.uber.org/goleak"
)

// mustHaveNoGoroutines panics if it finds any Goroutines running.
func mustHaveNoGoroutines() {
	if err := goleak.Find(
		// opencensus has a "defaultWorker" which is started by the package's
		// `init()` function. There is no way to stop this worker, so it will leak
		// whenever we import the package.
		goleak.IgnoreTopFunction("go.opencensus.io/stats/view.(*worker).start"),
		// The Ruby server's load balancer is registered in the `init()` function
		// of our "rubyserver/balancer" package. Ideally we'd clean this up
		// eventually, but the pragmatic approach is to just wait until we remove
		// the Ruby sidecar altogether.
		goleak.IgnoreTopFunction("google.golang.org/grpc.(*ccBalancerWrapper).watcher"),
		goleak.IgnoreTopFunction("gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/rubyserver/balancer.(*builder).monitor"),
		// labkit's logger spawns a Goroutine which cannot be closed when calling
		// `Initialize()`.
		goleak.IgnoreTopFunction("gitlab.com/gitlab-org/labkit/log.listenForSignalHangup"),
		// The backchannel code is somehow stock on closing its connections. I have no clue
		// why that is, but we should investigate.
		goleak.IgnoreTopFunction("gitlab.com/gitlab-org/gitaly/v14/internal/backchannel.clientHandshake.serve.func4"),
	); err != nil {
		panic(fmt.Errorf("goroutines running: %w", err))
	}
}

// mustHaveNoChildProcess panics if it finds a running or finished child
// process. It waits for 2 seconds for processes to be cleaned up by other
// goroutines.
func mustHaveNoChildProcess() {
	waitDone := make(chan struct{})
	go func() {
		commandcounter.WaitAllDone()
		close(waitDone)
	}()

	select {
	case <-waitDone:
	case <-time.After(2 * time.Second):
	}

	if err := mustFindNoFinishedChildProcess(); err != nil {
		panic(err)
	}

	if err := mustFindNoRunningChildProcess(); err != nil {
		panic(err)
	}
}

func mustFindNoFinishedChildProcess() error {
	// Wait4(pid int, wstatus *WaitStatus, options int, rusage *Rusage) (wpid int, err error)
	//
	// We use pid -1 to wait for any child. We don't care about wstatus or
	// rusage. Use WNOHANG to return immediately if there is no child waiting
	// to be reaped.
	wpid, err := syscall.Wait4(-1, nil, syscall.WNOHANG, nil)
	if err == nil && wpid > 0 {
		return fmt.Errorf("wait4 found child process %d", wpid)
	}

	return nil
}

func mustFindNoRunningChildProcess() error {
	pgrep := exec.Command("pgrep", "-P", fmt.Sprintf("%d", os.Getpid()))
	desc := fmt.Sprintf("%q", strings.Join(pgrep.Args, " "))

	out, err := pgrep.Output()
	if err == nil {
		pidsComma := strings.Replace(text.ChompBytes(out), "\n", ",", -1)
		psOut, _ := exec.Command("ps", "-o", "pid,args", "-p", pidsComma).Output()
		return fmt.Errorf("found running child processes %s:\n%s", pidsComma, psOut)
	}

	exitError, ok := err.(*exec.ExitError)
	if !ok {
		return fmt.Errorf("expected ExitError, got %T", err)
	}

	if exitError.ExitCode() == 1 {
		return nil
	}

	return fmt.Errorf("%s: %w", desc, err)
}
