package git

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
)

// BuildSSHInvocation builds a command line to invoke SSH with the provided key and known hosts.
// Both are optional.
func BuildSSHInvocation(ctx context.Context, sshKey, knownHosts string) (string, func(), error) {
	const sshCommand = "ssh"
	if sshKey == "" && knownHosts == "" {
		return sshCommand, func() {}, nil
	}

	tmpDir, err := ioutil.TempDir("", "gitaly-ssh-invocation")
	if err != nil {
		return "", func() {}, fmt.Errorf("create temporary directory: %w", err)
	}

	cleanup := func() {
		if err := os.RemoveAll(tmpDir); err != nil {
			ctxlogrus.Extract(ctx).WithError(err).Error("failed to remove tmp directory with ssh key/config")
		}
	}

	args := []string{sshCommand}
	if sshKey != "" {
		sshKeyFile := filepath.Join(tmpDir, "ssh-key")
		if err := ioutil.WriteFile(sshKeyFile, []byte(sshKey), 0400); err != nil {
			cleanup()
			return "", nil, fmt.Errorf("create ssh key file: %w", err)
		}

		args = append(args, "-oIdentitiesOnly=yes", "-oIdentityFile="+sshKeyFile)
	}

	if knownHosts != "" {
		knownHostsFile := filepath.Join(tmpDir, "known-hosts")
		if err := ioutil.WriteFile(knownHostsFile, []byte(knownHosts), 0400); err != nil {
			cleanup()
			return "", nil, fmt.Errorf("create known hosts file: %w", err)
		}

		args = append(args, "-oStrictHostKeyChecking=yes", "-oCheckHostIP=no", "-oUserKnownHostsFile="+knownHostsFile)
	}

	return strings.Join(args, " "), cleanup, nil
}
