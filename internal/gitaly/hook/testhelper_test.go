package hook

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/command"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

func TestMain(m *testing.M) {
	testhelper.Run(m)
}

func getExpectedEnv(t testing.TB, cfg config.Cfg, repo *gitalypb.Repository) []string {
	repoPath, err := config.NewLocator(cfg).GetPath(repo)
	require.NoError(t, err)

	expectedEnv := map[string]string{
		"GIT_DIR":             repoPath,
		"GIT_TERMINAL_PROMPT": "0",
		"GL_ID":               "1234",
		"GL_PROJECT_PATH":     repo.GetGlProjectPath(),
		"GL_PROTOCOL":         "web",
		"GL_REPOSITORY":       repo.GetGlRepository(),
		"GL_USERNAME":         "user",
		"PWD":                 repoPath,
	}

	// This is really quite roundabout given that we'll convert it back to an array next, but
	// we need to deduplicate environment variables here.
	for _, allowedEnvVar := range command.AllowedEnvironment(os.Environ()) {
		kv := strings.SplitN(allowedEnvVar, "=", 2)
		require.Len(t, kv, 2)
		expectedEnv[kv[0]] = kv[1]
	}

	expectedEnv["PATH"] = fmt.Sprintf("%s:%s", filepath.Dir(cfg.Git.BinPath), os.Getenv("PATH"))

	result := make([]string, 0, len(expectedEnv))
	for key, value := range expectedEnv {
		result = append(result, fmt.Sprintf("%s=%s", key, value))
	}
	sort.Strings(result)

	return result
}
