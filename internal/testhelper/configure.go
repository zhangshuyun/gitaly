package testhelper

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	log "github.com/sirupsen/logrus"
	gitalylog "gitlab.com/gitlab-org/gitaly/v14/internal/log"
)

var testDirectory string

// RunOption is an option that can be passed to Run.
type RunOption func(*runConfig)

type runConfig struct {
	setup                  func() error
	disableGoroutineChecks bool
}

// WithSetup allows the caller of Run to pass a setup function that will be called after global
// test state has been configured.
func WithSetup(setup func() error) RunOption {
	return func(cfg *runConfig) {
		cfg.setup = setup
	}
}

// WithDisabledGoroutineChecker disables checking for leaked Goroutines after tests have run. This
// should ideally only be used as a temporary measure until all Goroutine leaks have been fixed.
//
// Deprecated: This should not be used, but instead you should try to fix all Goroutine leakages.
func WithDisabledGoroutineChecker() RunOption {
	return func(cfg *runConfig) {
		cfg.disableGoroutineChecks = true
	}
}

// Run sets up required testing state and executes the given test suite. It can optionally receive a
// variable number of RunOptions.
func Run(m *testing.M, opts ...RunOption) {
	// Run tests in a separate function such that we can use deferred statements and still
	// (indirectly) call `os.Exit()` in case the test setup failed.
	if err := func() error {
		var cfg runConfig
		for _, opt := range opts {
			opt(&cfg)
		}

		defer mustHaveNoChildProcess()
		if !cfg.disableGoroutineChecks {
			defer mustHaveNoGoroutines()
		}

		cleanup, err := configure()
		if err != nil {
			return fmt.Errorf("test configuration: %w", err)
		}
		defer cleanup()

		if cfg.setup != nil {
			if err := cfg.setup(); err != nil {
				return fmt.Errorf("error calling setup function: %w", err)
			}
		}

		m.Run()

		return nil
	}(); err != nil {
		fmt.Printf("%s", err)
		os.Exit(1)
	}
}

// configure sets up the global test configuration. On failure,
// terminates the program.
func configure() (_ func(), returnedErr error) {
	gitalylog.Configure(gitalylog.Loggers, "json", "panic")

	if testDirectory != "" {
		return nil, errors.New("test directory has already been configured")
	}

	testDirectory = getTestTmpDir()
	defer func() {
		if returnedErr != nil {
			if err := os.RemoveAll(testDirectory); err != nil {
				log.Error(err)
			}
		}
	}()

	if err := configureGit(); err != nil {
		return nil, fmt.Errorf("configuring git: %w", err)
	}

	return func() {
		if err := os.RemoveAll(testDirectory); err != nil {
			log.Errorf("error removing test directory: %v", err)
		}
	}, nil
}

// configureGit configures git for test purpose
func configureGit() error {
	// We cannot use gittest here given that we ain't got no config yet. We thus need to
	// manually resolve the git executable, which is either stored in below envvar if
	// executed via our Makefile, or else just git as resolved via PATH.
	gitPath := "git"
	if path, ok := os.LookupEnv("GITALY_TESTING_GIT_BINARY"); ok {
		gitPath = path
	}

	// Unset environment variables which have an effect on Git itself.
	cmd := exec.Command(gitPath, "rev-parse", "--local-env-vars")
	envvars, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("error computing local envvars: %w", err)
	}
	for _, envvar := range strings.Split(string(envvars), "\n") {
		if err := os.Unsetenv(envvar); err != nil {
			return fmt.Errorf("error unsetting envvar: %w", err)
		}
	}

	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		return fmt.Errorf("could not get caller info")
	}

	// Set both GOCACHE and GOPATH to the currently active settings to not
	// have them be overridden by changing our home directory. default it
	for _, envvar := range []string{"GOCACHE", "GOPATH"} {
		cmd := exec.Command("go", "env", envvar)

		output, err := cmd.Output()
		if err != nil {
			return err
		}

		err = os.Setenv(envvar, strings.TrimSpace(string(output)))
		if err != nil {
			return err
		}
	}

	testHome := filepath.Join(filepath.Dir(currentFile), "testdata/home")
	// overwrite HOME env variable so user global .gitconfig doesn't influence tests
	return os.Setenv("HOME", testHome)
}

func getTestTmpDir() string {
	testTmpDir := os.Getenv("TEST_TMP_DIR")
	if testTmpDir != "" {
		return testTmpDir
	}

	testTmpDir, err := os.MkdirTemp("/tmp/", "gitaly-")
	if err != nil {
		log.Fatal(err)
	}

	// macOS symlinks /tmp/ to /private/tmp/ which can cause some check to fail
	tmpDir, err := filepath.EvalSymlinks(testTmpDir)
	if err != nil {
		log.Fatal(err)
	}

	return tmpDir
}
