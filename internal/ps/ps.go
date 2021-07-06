package ps

import (
	"os/exec"
	"strconv"
	"strings"
)

// Exec invokes ps -o keywords -p pid and returns its output
func Exec(pid int, keywords string) (string, error) {
	out, err := exec.Command("ps", "-o", keywords, "-p", strconv.Itoa(pid)).Output()
	if err != nil {
		return "", err
	}

	return strings.TrimSpace(string(out)), nil
}

// Comm invokes ps -o comm= -p pid and returns its output
func Comm(pid int) (string, error) {
	return Exec(pid, "comm=")
}
