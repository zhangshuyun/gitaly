package testserver

import (
	"fmt"
	"os"
	"path"
	"runtime"
	"testing"

	"github.com/otiai10/copy"
	"github.com/stretchr/testify/require"
)

var (
	TestRoot, _ = os.MkdirTemp("", "test-gitlab-shell")
)

func PrepareTestRootDir(t *testing.T) {
	t.Helper()

	require.NoError(t, os.MkdirAll(TestRoot, 0700))

	t.Cleanup(func() { os.RemoveAll(TestRoot) })

	require.NoError(t, copyTestData())

	oldWd, err := os.Getwd()
	require.NoError(t, err)

	t.Cleanup(func() { os.Chdir(oldWd) })

	require.NoError(t, os.Chdir(TestRoot))
}

func copyTestData() error {
	testDataDir, err := getTestDataDir()
	if err != nil {
		return err
	}

	testdata := path.Join(testDataDir, "testroot")

	return copy.Copy(testdata, TestRoot)
}

func getTestDataDir() (string, error) {
	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		return "", fmt.Errorf("Could not get caller info")
	}

	return path.Join(path.Dir(currentFile), "testdata"), nil
}
