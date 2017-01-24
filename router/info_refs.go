package router

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"syscall"

	"gitlab.com/gitlab-org/gitaly/helper"

	"github.com/gorilla/mux"
)

const (
	gitalyRepoPathHeader = "Gitaly-Repo-Path"
	gitlabIdHeader       = "Gitaly-GL-Id"
)

func GetInfoRefs(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	rpc := vars["service"]

	repoPath := r.Header.Get(gitalyRepoPathHeader)
	if repoPath == "" {
		helper.Fail500(w, r, fmt.Errorf("GetInfoRefs: %s header was not found", gitalyRepoPathHeader))
		return
	}
	glId := r.Header.Get(gitlabIdHeader) // Allowed to be empty

	// Prepare our Git subprocess
	cmd := gitCommand(glId, "git", rpc, "--stateless-rpc", "--advertise-refs", repoPath)

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		helper.Fail500(w, r, fmt.Errorf("GetInfoRefs: stdout: %v", err))
		return
	}
	defer stdout.Close()

	if err := cmd.Start(); err != nil {
		helper.Fail500(w, r, fmt.Errorf("GetInfoRefs: start %v: %v", cmd.Args, err))
		return
	}
	defer helper.CleanUpProcessGroup(cmd) // Ensure brute force subprocess clean-up

	// Start writing the response
	w.Header().Set("Content-Type", fmt.Sprintf("application/x-git-%s-advertisement", rpc))
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(200) // Don't bother with HTTP 500 from this point on, just return

	if err := pktLine(w, fmt.Sprintf("# service=git-%s\n", rpc)); err != nil {
		helper.LogError(r, fmt.Errorf("GetInfoRefs: pktLine: %v", err))
		return
	}

	if err := pktFlush(w); err != nil {
		helper.LogError(r, fmt.Errorf("GetInfoRefs: pktFlush: %v", err))
		return
	}

	if _, err := io.Copy(w, stdout); err != nil {
		helper.LogError(r, fmt.Errorf("GetInfoRefs: copy output of %v: %v", cmd.Args, err))
		return
	}

	if err := cmd.Wait(); err != nil {
		helper.LogError(r, fmt.Errorf("GetInfoRefs: wait for %v: %v", cmd.Args, err))
		return
	}
}

func gitCommand(glId string, name string, args ...string) *exec.Cmd {
	cmd := exec.Command(name, args...)
	// Start the command in its own process group (nice for signalling)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	// Explicitly set the environment for the Git command
	cmd.Env = []string{
		fmt.Sprintf("HOME=%s", os.Getenv("HOME")),
		fmt.Sprintf("PATH=%s", os.Getenv("PATH")),
		fmt.Sprintf("LD_LIBRARY_PATH=%s", os.Getenv("LD_LIBRARY_PATH")),
		fmt.Sprintf("GL_ID=%s", glId),
		fmt.Sprintf("GL_PROTOCOL=http"),
	}
	// If we don't do something with cmd.Stderr, Git errors will be lost
	cmd.Stderr = os.Stderr
	return cmd
}

func pktLine(w io.Writer, s string) error {
	_, err := fmt.Fprintf(w, "%04x%s", len(s)+4, s)
	return err
}

func pktFlush(w io.Writer) error {
	_, err := fmt.Fprint(w, "0000")
	return err
}
