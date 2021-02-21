package testhelper

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"

	log "github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	gitalylog "gitlab.com/gitlab-org/gitaly/internal/log"
)

var (
	configureOnce sync.Once
	testDirectory string
)

// Configure sets up the global test configuration. On failure,
// terminates the program.
func Configure() func() {
	configureOnce.Do(func() {
		gitalylog.Configure("json", "panic")

		var err error
		testDirectory, err = ioutil.TempDir("", "gitaly-")
		if err != nil {
			log.Fatal(err)
		}

		config.Config.Logging.Dir = filepath.Join(testDirectory, "log")
		if err := os.Mkdir(config.Config.Logging.Dir, 0755); err != nil {
			os.RemoveAll(testDirectory)
			log.Fatal(err)
		}

		config.Config.Storages = []config.Storage{
			{Name: "default", Path: GitlabTestStoragePath()},
		}
		if err := os.Mkdir(config.Config.Storages[0].Path, 0755); err != nil {
			os.RemoveAll(testDirectory)
			log.Fatal(err)
		}

		config.Config.SocketPath = "/bogus"
		config.Config.GitlabShell.Dir = "/"

		config.Config.InternalSocketDir = filepath.Join(testDirectory, "internal-socket")
		if err := os.Mkdir(config.Config.InternalSocketDir, 0755); err != nil {
			os.RemoveAll(testDirectory)
			log.Fatal(err)
		}

		config.Config.BinDir = filepath.Join(testDirectory, "bin")
		if err := os.Mkdir(config.Config.BinDir, 0755); err != nil {
			os.RemoveAll(testDirectory)
			log.Fatal(err)
		}

		for _, f := range []func() error{
			func() error { return ConfigureRuby(&config.Config) },
			ConfigureGit,
			func() error { return config.Config.Validate() },
		} {
			if err := f(); err != nil {
				os.RemoveAll(testDirectory)
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

// ConfigureGitalyGit2Go configures the gitaly-git2go command for tests
func ConfigureGitalyGit2Go(outputDir string) {
	buildCommand(outputDir, "gitaly-git2go")
}

// ConfigureGitalyLfsSmudge configures the gitaly-lfs-smudge command for tests
func ConfigureGitalyLfsSmudge(outputDir string) {
	buildCommand(outputDir, "gitaly-lfs-smudge")
}

// ConfigureGitalySSH configures the gitaly-ssh command for tests
func ConfigureGitalySSH(outputDir string) {
	buildCommand(outputDir, "gitaly-ssh")
}

// ConfigureGitalyHooksBinary builds gitaly-hooks command for tests
func ConfigureGitalyHooksBinary(outputDir string) {
	buildCommand(outputDir, "gitaly-hooks")
}

func buildCommand(outputDir, cmd string) {
	if outputDir == "" {
		log.Fatal("BinDir must be set")
	}

	goBuildArgs := []string{
		"build",
		"-tags", "static,system_libgit2",
		"-o", filepath.Join(outputDir, cmd),
		fmt.Sprintf("gitlab.com/gitlab-org/gitaly/cmd/%s", cmd),
	}
	MustRunCommand(nil, nil, "go", goBuildArgs...)
}
