package main

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/golang/protobuf/jsonpb"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/updateref"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

const keepAroundNamespace = "refs/keep-around"

func TestVisibilityOfHiddenRefs(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	cfg, repo, repoPath := testcfg.BuildWithRepo(t)
	testhelper.ConfigureGitalySSHBin(t, cfg)
	testhelper.ConfigureGitalyHooksBin(t, cfg)

	socketPath := testhelper.GetTemporaryGitalySocketFileName(t)

	_, clean := runServer(t, false, cfg, "unix", socketPath)
	defer clean()

	_, clean = runServer(t, false, cfg, "unix", cfg.GitalyInternalSocketPath())
	defer clean()

	// Create a keep-around ref
	existingSha := "1e292f8fedd741b75372e19097c76d327140c312"
	keepAroundRef := fmt.Sprintf("%s/%s", keepAroundNamespace, existingSha)

	gitCmdFactory := git.NewExecCommandFactory(cfg)
	localRepo := localrepo.NewTestRepo(t, cfg, repo)
	updater, err := updateref.New(ctx, cfg, localRepo)

	require.NoError(t, err)
	require.NoError(t, updater.Create(git.ReferenceName(keepAroundRef), existingSha))
	require.NoError(t, updater.Wait())

	gittest.Exec(t, cfg, "-C", repoPath, "config", "transfer.hideRefs", keepAroundNamespace)

	output := gittest.Exec(t, cfg, "ls-remote", repoPath, keepAroundNamespace)
	require.Empty(t, output, "there should be no keep-around refs in normal ls-remote output")

	wd, err := os.Getwd()
	require.NoError(t, err)

	tests := []struct {
		name             string
		GitConfigOptions []string
		HiddenRefFound   bool
	}{
		{
			name:             "With no custom GitConfigOptions passed",
			GitConfigOptions: []string{},
			HiddenRefFound:   true,
		},
		{
			name:             "With custom GitConfigOptions passed",
			GitConfigOptions: []string{fmt.Sprintf("transfer.hideRefs=%s", keepAroundRef)},
			HiddenRefFound:   false,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			pbMarshaler := &jsonpb.Marshaler{}
			payload, err := pbMarshaler.MarshalToString(&gitalypb.SSHUploadPackRequest{
				Repository:       repo,
				GitConfigOptions: test.GitConfigOptions,
			})

			require.NoError(t, err)

			env := []string{
				fmt.Sprintf("GITALY_PAYLOAD=%s", payload),
				fmt.Sprintf("GITALY_ADDRESS=unix:%s", socketPath),
				fmt.Sprintf("GITALY_WD=%s", wd),
				fmt.Sprintf("PATH=.:%s", os.Getenv("PATH")),
				fmt.Sprintf("GIT_SSH_COMMAND=%s upload-pack", filepath.Join(cfg.BinDir, "gitaly-ssh")),
			}

			stdout := &bytes.Buffer{}
			cmd, err := gitCmdFactory.NewWithoutRepo(ctx, git.SubCmd{
				Name: "ls-remote",
				Args: []string{
					fmt.Sprintf("%s:%s", "git@localhost", repoPath),
					keepAroundRef,
				},
			}, git.WithEnv(env...), git.WithStdout(stdout))
			require.NoError(t, err)

			err = cmd.Wait()
			require.NoError(t, err)

			if test.HiddenRefFound {
				require.Equal(t, fmt.Sprintf("%s\t%s\n", existingSha, keepAroundRef), stdout.String())
			} else {
				require.NotEqual(t, fmt.Sprintf("%s\t%s\n", existingSha, keepAroundRef), stdout.String())
			}
		})
	}
}
