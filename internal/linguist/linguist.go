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
	"path"

	"gitlab.com/gitlab-org/gitaly/internal/command"
	"gitlab.com/gitlab-org/gitaly/internal/config"
)

func init() {
	config.RegisterHook(LoadColors)
}

var (
	colorMap = make(map[string]Language)
)

// Language is used to parse Linguist's language.json file.
type Language struct {
	Color string `json:"color"`
}

// CountPerLanguage represents a counter value (int) per language.
type CountPerLanguage map[string]int

// ByteCountPerLanguage represents a counter value (bytes) per language.
type ByteCountPerLanguage map[string]uint64

// FileListPerLanguage is used to parse Linguist's breakdown output to represent the list of files per language.
type FileListPerLanguage map[string][]string

// Stats returns the repository's language stats as reported by 'git-linguist'.
func Stats(ctx context.Context, repoPath string, commitID string) (ByteCountPerLanguage, error) {
	reader, err := startGitLinguist(ctx, repoPath, commitID, "stats")

	if err != nil {
		return nil, err
	}

	data, err := ioutil.ReadAll(reader)
	if err != nil {
		return nil, err
	}

	stats := make(ByteCountPerLanguage)
	return stats, json.Unmarshal(data, &stats)
}

// FileCountStats returns the file counts per language
func FileCountStats(ctx context.Context, repoPath string, commitID string) (CountPerLanguage, error) {
	reader, err := startGitLinguist(ctx, repoPath, commitID, "breakdown")
	if err != nil {
		return nil, err
	}

	data, err := ioutil.ReadAll(reader)
	if err != nil {
		return nil, err
	}

	languageFiles := make(FileListPerLanguage)
	if err := json.Unmarshal(data, &languageFiles); err != nil {
		return nil, err
	}

	stats := make(CountPerLanguage)
	for lang, files := range languageFiles {
		stats[lang] = len(files)
	}

	return stats, nil
}

// Color returns the color Linguist has assigned to language.
func Color(language string) string {
	if color := colorMap[language].Color; color != "" {
		return color
	}

	colorSha := sha256.Sum256([]byte(language))
	return fmt.Sprintf("#%x", colorSha[0:3])
}

// LoadColors loads the name->color map from the Linguist gem.
func LoadColors(cfg config.Cfg) error {
	jsonReader, err := openLanguagesJSON(cfg)
	if err != nil {
		return err
	}
	defer jsonReader.Close()

	return json.NewDecoder(jsonReader).Decode(&colorMap)
}

func startGitLinguist(ctx context.Context, repoPath string, commitID string, linguistCommand string) (io.Reader, error) {
	cmd := exec.Command("bundle", "exec", "bin/ruby-cd", repoPath, "git-linguist", "--commit="+commitID, linguistCommand)
	cmd.Dir = config.Config.Ruby.Dir

	reader, err := command.New(ctx, cmd, nil, nil, nil, os.Environ()...)
	if err != nil {
		return nil, err
	}

	return reader, nil
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
	cmd.Dir = config.Config.Ruby.Dir

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

	return os.Open(path.Join(linguistPathSymlink.Name(), "lib/linguist/languages.json"))
}
