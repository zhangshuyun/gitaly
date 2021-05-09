package testhelper

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
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
		gitalylog.Configure(gitalylog.Loggers, "json", "panic")

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

// ConfigureGitalyGit2GoBin configures the gitaly-git2go command for tests
func ConfigureGitalyGit2GoBin(t testing.TB, cfg config.Cfg) {
	buildBinary(t, cfg.BinDir, "gitaly-git2go")
}

// ConfigureGitalyLfsSmudge configures the gitaly-lfs-smudge command for tests
func ConfigureGitalyLfsSmudge(outputDir string) {
	buildCommand(nil, outputDir, "gitaly-lfs-smudge")
}

// ConfigureGitalyHooksBin builds gitaly-hooks command for tests for the cfg.
func ConfigureGitalyHooksBin(t testing.TB, cfg config.Cfg) {
	buildBinary(t, cfg.BinDir, "gitaly-hooks")
}

// ConfigureGitalySSHBin builds gitaly-ssh command for tests for the cfg.
func ConfigureGitalySSHBin(t testing.TB, cfg config.Cfg) {
	buildBinary(t, cfg.BinDir, "gitaly-ssh")
}

func buildBinary(t testing.TB, dstDir, name string) {
	// binsPath is a shared between all tests location where all compiled binaries should be placed
	binsPath := filepath.Join(testDirectory, "bins")
	// binPath is a path to a specific binary file
	binPath := filepath.Join(binsPath, name)
	// lockPath is a path to the special lock file used to prevent parallel build runs
	lockPath := binPath + ".lock"

	defer func() {
		if !t.Failed() {
			// copy compiled binary to the destination folder
			require.NoError(t, os.MkdirAll(dstDir, os.ModePerm))
			MustRunCommand(t, nil, "cp", binPath, dstDir)
		}
	}()

	require.NoError(t, os.MkdirAll(binsPath, os.ModePerm))

	lockFile, err := os.OpenFile(lockPath, os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		if !errors.Is(err, os.ErrExist) {
			require.FailNow(t, err.Error())
		}
		// another process is creating the binary at the moment, wait for it to complete (5s)
		for i := 0; i < 50; i++ {
			if _, err := os.Stat(binPath); err != nil {
				if !errors.Is(err, os.ErrExist) {
					require.NoError(t, err)
				}
				time.Sleep(100 * time.Millisecond)
				continue
			}
			// binary was created
			return
		}
		require.FailNow(t, "another process is creating binary for too long")
	}
	defer func() { require.NoError(t, os.Remove(lockPath)) }()
	require.NoError(t, lockFile.Close())

	if _, err := os.Stat(binPath); err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			// something went wrong and for some reason the binary already exists
			require.FailNow(t, err.Error())
		}
		buildCommand(t, binsPath, name)
	}
}

func buildCommand(t testing.TB, outputDir, cmd string) {
	if outputDir == "" {
		log.Fatal("BinDir must be set")
	}

	goBuildArgs := []string{
		"build",
		"-tags", "static,system_libgit2",
		"-o", filepath.Join(outputDir, cmd),
		fmt.Sprintf("gitlab.com/gitlab-org/gitaly/cmd/%s", cmd),
	}
	MustRunCommand(t, nil, "go", goBuildArgs...)
}
