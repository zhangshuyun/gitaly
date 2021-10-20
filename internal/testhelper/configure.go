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

	cleanup, err := configureTestDirectory()
	if err != nil {
		return nil, fmt.Errorf("configuring test directory: %w", err)
	}
	defer func() {
		if returnedErr != nil {
			cleanup()
		}
	}()

	if err := configureGit(); err != nil {
		return nil, fmt.Errorf("configuring git: %w", err)
	}

	return cleanup, nil
}

func configureTestDirectory() (_ func(), returnedErr error) {
	if testDirectory != "" {
		return nil, errors.New("test directory has already been configured")
	}

	// Ideally, we'd just pass "" to `os.MkdirTemp()`, which would then use either the value of
	// `$TMPDIR` or alternatively "/tmp". But given that macOS sets `$TMPDIR` to a user specific
	// temporary directory, resulting paths would be too long and thus cause issues galore. We
	// thus support our own specific variable instead which allows users to override it, with
	// our default being "/tmp".
	tempDirLocation := os.Getenv("TEST_TMP_DIR")
	if tempDirLocation == "" {
		tempDirLocation = "/tmp"
	}

	var err error
	testDirectory, err = os.MkdirTemp(tempDirLocation, "gitaly-")
	if err != nil {
		return nil, fmt.Errorf("creating test directory: %w", err)
	}

	cleanup := func() {
		if err := os.RemoveAll(testDirectory); err != nil {
			log.Errorf("cleaning up test directory: %v", err)
		}
	}
	defer func() {
		if returnedErr != nil {
			cleanup()
		}
	}()

	// macOS symlinks /tmp/ to /private/tmp/ which can cause some check to fail. We thus resolve
	// the symlinks to their actual location.
	testDirectory, err = filepath.EvalSymlinks(testDirectory)
	if err != nil {
		return nil, err
	}

	// In many locations throughout Gitaly, we create temporary files and directories. By
	// default, these would clutter the "real" temporary directory with useless cruft that stays
	// around after our tests. To avoid this, we thus set the TMPDIR environment variable to
	// point into a directory inside of out test directory.
	globalTempDir := filepath.Join(testDirectory, "tmp")
	if err := os.Mkdir(globalTempDir, 0o755); err != nil {
		return nil, fmt.Errorf("creating global temporary directory: %w", err)
	}
	if err := os.Setenv("TMPDIR", globalTempDir); err != nil {
		return nil, fmt.Errorf("setting global temporary directory: %w", err)
	}

	return cleanup, nil
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
