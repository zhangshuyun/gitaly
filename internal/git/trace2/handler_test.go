package trace2

import (
	"fmt"
	"os/exec"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
)

func TestCopyHandler(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	cmd := exec.Command("env")

	env, err := LogHandler(ctx, cmd)
	require.NoError(t, err)

	require.Len(t, cmd.ExtraFiles, 1)
	require.Equal(t, env[0], fmt.Sprintf("%s=3", trace2EventFDKey))
	require.True(t, strings.HasPrefix(env[1], sessionIDKey))
}
