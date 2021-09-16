package testhelper

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"

	log "github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
	gitalylog "gitlab.com/gitlab-org/gitaly/v14/internal/log"
)

var (
	configureOnce sync.Once
	testDirectory string
)

// Configure sets up the global test configuration. On failure,
// terminates the program.
func Configure() func() {
	configureOnce.Do(func() {
		gitalylog.Configure(gitalylog.Loggers, "json", "panic")

		testDirectory = getTestTmpDir()

		for _, f := range []func() error{
			ConfigureGit,
		} {
			if err := f(); err != nil {
				if err := os.RemoveAll(testDirectory); err != nil {
					log.Error(err)
				}
				log.Fatalf("error configuring tests: %v", err)
			}
		}
	})

	return func() {
		if err := os.RemoveAll(testDirectory); err != nil {
			log.Fatalf("error removing test directory: %v", err)
		}
	}
}

// ConfigureGit configures git for test purpose
func ConfigureGit() error {
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

// ConfigureRuby configures Ruby settings for test purposes at run time.
func ConfigureRuby(cfg *config.Cfg) error {
	if dir := os.Getenv("GITALY_TEST_RUBY_DIR"); len(dir) > 0 {
		// Sometimes runtime.Caller is unreliable. This environment variable provides a bypass.
		cfg.Ruby.Dir = dir
	} else {
		_, currentFile, _, ok := runtime.Caller(0)
		if !ok {
			return fmt.Errorf("could not get caller info")
		}
		cfg.Ruby.Dir = filepath.Join(filepath.Dir(currentFile), "../../ruby")
	}

	if err := cfg.ConfigureRuby(); err != nil {
		log.Fatalf("validate ruby config: %v", err)
	}

	return nil
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
	return testTmpDir
}
