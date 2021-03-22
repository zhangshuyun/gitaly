package git

import (
	"io/ioutil"
	"path/filepath"
	"regexp"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
)

func TestBuildSSHInvocation(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	const tmpDirPattern = "=(\\S+)/(ssh-key|known-hosts)"
	reTmpDir := regexp.MustCompile(tmpDirPattern)

	for _, tc := range []struct {
		desc       string
		sshKey     string
		knownHosts string
	}{
		{
			desc: "no arguments",
		},
		{
			desc:   "ssh key given",
			sshKey: "ssh-key-content",
		},
		{
			desc:       "known hosts given",
			knownHosts: "known-hosts-content",
		},
		{
			desc:       "both given",
			sshKey:     "ssh-key-content",
			knownHosts: "known-hosts-content",
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			sshCommand, clean, err := BuildSSHInvocation(ctx, tc.sshKey, tc.knownHosts)
			require.NoError(t, err)
			defer clean()

			var tmpDir string
			if tc.sshKey != "" || tc.knownHosts != "" {
				matches := reTmpDir.FindStringSubmatch(sshCommand)
				require.Greater(t, len(matches), 1, "expected to find at least one file configured")
				tmpDir = matches[1]
				require.DirExists(t, tmpDir)
			} else {
				require.False(t, reTmpDir.MatchString(sshCommand))
			}

			sshKeyPath := filepath.Join(tmpDir, "ssh-key")
			knownHostsPath := filepath.Join(tmpDir, "known-hosts")

			expectedCommand := "ssh"
			if tc.sshKey != "" {
				content, err := ioutil.ReadFile(sshKeyPath)
				require.NoError(t, err)
				require.Equal(t, tc.sshKey, string(content))
				expectedCommand += " -oIdentitiesOnly=yes -oIdentityFile=" + sshKeyPath
			} else {
				require.NoFileExists(t, sshKeyPath)
			}

			if tc.knownHosts != "" {
				content, err := ioutil.ReadFile(knownHostsPath)
				require.NoError(t, err)
				require.Equal(t, tc.knownHosts, string(content))
				expectedCommand += " -oStrictHostKeyChecking=yes -oUserKnownHostsFile=" + knownHostsPath
			} else {
				require.NoFileExists(t, knownHostsPath)
			}

			require.Equal(t, expectedCommand, sshCommand)

			clean()
			require.NoDirExists(t, tmpDir)
		})
	}
}
