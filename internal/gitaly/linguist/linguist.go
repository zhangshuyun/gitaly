package linguist

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"

	"gitlab.com/gitlab-org/gitaly/internal/command"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
)

var (
	exportedEnvVars = []string{"HOME", "PATH", "GEM_HOME", "BUNDLE_PATH", "BUNDLE_APP_CONFIG"}
)

// Language is used to parse Linguist's language.json file.
type Language struct {
	Color string `json:"color"`
}

// ByteCountPerLanguage represents a counter value (bytes) per language.
type ByteCountPerLanguage map[string]uint64

// Instance is a holder of the defined in the system language settings.
type Instance struct {
	colorMap map[string]Language
}

// New loads the name->color map from the Linguist gem and returns initialised instance
// to use back to the caller or an error.
func New(cfg config.Cfg) (*Instance, error) {
	jsonReader, err := openLanguagesJSON(cfg)
	if err != nil {
		return nil, err
	}
	defer jsonReader.Close()

	var inst Instance

	if err := json.NewDecoder(jsonReader).Decode(&inst.colorMap); err != nil {
		return nil, err
	}

	return &inst, nil
}

// Stats returns the repository's language stats as reported by 'git-linguist'.
func (inst *Instance) Stats(ctx context.Context, cfg config.Cfg, repoPath string, commitID string) (ByteCountPerLanguage, error) {
	cmd, err := startGitLinguist(ctx, cfg, repoPath, commitID, "stats")
	if err != nil {
		return nil, err
	}

	data, err := ioutil.ReadAll(cmd)
	if err != nil {
		return nil, err
	}

	if err := cmd.Wait(); err != nil {
		return nil, err
	}

	stats := make(ByteCountPerLanguage)
	return stats, json.Unmarshal(data, &stats)
}

// Color returns the color Linguist has assigned to language.
func (inst *Instance) Color(language string) string {
	if color := inst.colorMap[language].Color; color != "" {
		return color
	}

	colorSha := sha256.Sum256([]byte(language))
	return fmt.Sprintf("#%x", colorSha[0:3])
}

func startGitLinguist(ctx context.Context, cfg config.Cfg, repoPath string, commitID string, linguistCommand string) (*command.Command, error) {
	bundle, err := exec.LookPath("bundle")
	if err != nil {
		return nil, err
	}

	args := []string{
		bundle,
		"exec",
		"bin/ruby-cd",
		repoPath,
		"git-linguist",
		"--commit=" + commitID,
		linguistCommand,
	}

	// This is a horrible hack. git-linguist will execute `git rev-parse
	// --git-dir` to check whether it is in a Git directory or not. We don't
	// want to use the one provided by PATH, but instead the one specified
	// via the configuration. git-linguist doesn't specify any way to choose
	// a different Git implementation, so we need to prepend the configured
	// Git's directory to PATH. But as our internal command interface will
	// overwrite PATH even if we pass it in here, we need to work around it
	// and instead execute the command with `env PATH=$GITDIR:$PATH`.
	gitDir := filepath.Dir(cfg.Git.BinPath)
	if path, ok := os.LookupEnv("PATH"); ok && gitDir != "." {
		args = append([]string{
			"env", fmt.Sprintf("PATH=%s:%s", gitDir, path),
		}, args...)
	}

	cmd := exec.Command(args[0], args[1:]...)
	cmd.Dir = cfg.Ruby.Dir

	internalCmd, err := command.New(ctx, cmd, nil, nil, nil, exportEnvironment()...)
	if err != nil {
		return nil, err
	}

	return internalCmd, nil
}

func openLanguagesJSON(cfg config.Cfg) (io.ReadCloser, error) {
	if jsonPath := cfg.Ruby.LinguistLanguagesPath; jsonPath != "" {
		// This is a fallback for environments where dynamic discovery of the
		// linguist path via Bundler is not working for some reason, for example
		// https://gitlab.com/gitlab-org/gitaly/issues/1119.
		return os.Open(jsonPath)
	}

	linguistPathSymlink, err := ioutil.TempFile("", "gitaly-linguist-path")
	if err != nil {
		return nil, err
	}
	defer os.Remove(linguistPathSymlink.Name())

	if err := linguistPathSymlink.Close(); err != nil {
		return nil, err
	}

	// We use a symlink because we cannot trust Bundler to not print garbage
	// on its stdout.
	rubyScript := `FileUtils.ln_sf(Bundler.rubygems.find_name('github-linguist').first.full_gem_path, ARGV.first)`
	cmd := exec.Command("bundle", "exec", "ruby", "-rfileutils", "-e", rubyScript, linguistPathSymlink.Name())
	cmd.Dir = cfg.Ruby.Dir

	// We have learned that in practice the command we are about to run is a
	// canary for Ruby/Bundler configuration problems. Including stderr and
	// stdout in the gitaly log is useful for debugging such problems.
	cmd.Stderr = os.Stderr
	cmd.Stdout = os.Stdout

	if err := cmd.Run(); err != nil {
		if exitError, ok := err.(*exec.ExitError); ok {
			err = fmt.Errorf("%v; stderr: %q", exitError, exitError.Stderr)
		}
		return nil, err
	}

	return os.Open(filepath.Join(linguistPathSymlink.Name(), "lib", "linguist", "languages.json"))
}

func exportEnvironment() []string {
	var env []string
	for _, envVarName := range exportedEnvVars {
		if val, ok := os.LookupEnv(envVarName); ok {
			env = append(env, fmt.Sprintf("%s=%s", envVarName, val))
		}
	}

	return env
}
