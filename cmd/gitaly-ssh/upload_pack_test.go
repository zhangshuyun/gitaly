package main

import (
	"bytes"
	"fmt"
	"os"
	"testing"

	"github.com/golang/protobuf/jsonpb"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/config"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/updateref"
	"gitlab.com/gitlab-org/gitaly/internal/server"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

const keepAroundNamespace = "refs/keep-around"

func TestVisibilityOfHiddenRefs(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	testRepo, testRepoPath, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()

	// Create a keep-around ref
	existingSha := "1e292f8fedd741b75372e19097c76d327140c312"
	keepAroundRef := fmt.Sprintf("%s/%s", keepAroundNamespace, existingSha)

	updater, err := updateref.New(ctx, testRepo)

	require.NoError(t, err)
	require.NoError(t, updater.Create(keepAroundRef, existingSha))
	require.NoError(t, updater.Wait())

	testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "config", "transfer.hideRefs", keepAroundNamespace)

	output := testhelper.MustRunCommand(t, nil, "git", "ls-remote", testRepoPath, keepAroundNamespace)
	require.Empty(t, output, "there should be no keep-around refs in normal ls-remote output")

	socketPath := testhelper.GetTemporaryGitalySocketFileName()

	unixServer, _ := runServer(t, server.NewInsecure, config.Config, "unix", socketPath)
	defer unixServer.Stop()

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
				Repository:       testRepo,
				GitConfigOptions: test.GitConfigOptions,
			})

			require.NoError(t, err)

			env := []string{
				fmt.Sprintf("GITALY_PAYLOAD=%s", payload),
				fmt.Sprintf("GITALY_ADDRESS=unix:%s", socketPath),
				fmt.Sprintf("GITALY_WD=%s", wd),
				fmt.Sprintf("PATH=.:%s", os.Getenv("PATH")),
				fmt.Sprintf("GIT_SSH_COMMAND=%s upload-pack", gitalySSHPath),
			}

			stdout := &bytes.Buffer{}
			cmd, err := git.SafeBareCmd(ctx, nil, stdout, nil, env, nil, git.SubCmd{
				Name: "ls-remote",
				Args: []string{
					fmt.Sprintf("%s:%s", "git@localhost", testRepoPath),
					keepAroundRef,
				},
			})
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
