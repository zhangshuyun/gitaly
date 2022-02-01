package command

import (
	"bytes"
	"os/exec"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
)

func TestCommand_EnableTrace2(t *testing.T) {
	ctx := testhelper.Context(t)

	buff := &bytes.Buffer{}
	cmd, err := New(ctx, exec.Command("/usr/bin/env"), nil, buff, nil)
	require.NoError(t, err)

	require.Len(t, cmd.cmd.ExtraFiles, 1)
	require.Contains(t, cmd.cmd.Env, "GIT_TRACE2_EVENT=3")
}
