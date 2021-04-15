package gittest

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"gitlab.com/gitlab-org/gitaly/internal/git/hooks"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
)

// WriteEnvToCustomHook dumps the env vars that the custom hooks receives to a file
func WriteEnvToCustomHook(t testing.TB, repoPath, hookName string) string {
	tempDir, cleanup := testhelper.TempDir(t)
	t.Cleanup(cleanup)

	outputPath := filepath.Join(tempDir, "hook.env")
	hookContent := fmt.Sprintf("#!/bin/sh\n/usr/bin/env >%s\n", outputPath)

	WriteCustomHook(t, repoPath, hookName, []byte(hookContent))

	return outputPath
}

// WriteCheckNewObjectExistsHook writes a pre-receive hook which only succeeds
// if it can find the object in the quarantine directory. if
// GIT_OBJECT_DIRECTORY and GIT_ALTERNATE_OBJECT_DIRECTORIES were not passed
// through correctly to the hooks, it will fail
func WriteCheckNewObjectExistsHook(t testing.TB, gitBin, repoPath string) {
	hook := fmt.Sprintf(`#!/usr/bin/env ruby
STDIN.each_line do |line|
  new_object = line.split(' ')[1]
  exit 1 unless new_object
  exit 1 unless	system(*%%W[%s cat-file -e #{new_object}])
end
`, gitBin)

	WriteCustomHook(t, repoPath, "pre-receive", []byte(hook))
}

// WriteCustomHook writes a hook in the repo/path.git/custom_hooks directory
func WriteCustomHook(t testing.TB, repoPath, name string, content []byte) {
	fullPath := filepath.Join(repoPath, "custom_hooks", name)
	testhelper.WriteExecutable(t, fullPath, content)
}

// CaptureHookEnv creates a bogus 'update' Git hook to sniff out what
// environment variables get set for hooks.
func CaptureHookEnv(t testing.TB) (string, func()) {
	tempDir, cleanup := testhelper.TempDir(t)

	oldOverride := hooks.Override
	hooks.Override = filepath.Join(tempDir, "hooks")
	hookOutputFile := filepath.Join(tempDir, "hook.env")

	if !assert.NoError(t, os.MkdirAll(hooks.Override, 0755)) {
		cleanup()
		t.FailNow()
	}

	script := []byte(`
#!/bin/sh
env | grep -e ^GIT -e ^GL_ > ` + hookOutputFile + "\n")

	if !assert.NoError(t, ioutil.WriteFile(filepath.Join(hooks.Override, "update"), script, 0755)) {
		cleanup()
		t.FailNow()
	}

	return hookOutputFile, func() {
		cleanup()
		hooks.Override = oldOverride
	}
}
