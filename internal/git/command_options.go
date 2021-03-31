package git

import (
	"fmt"
	"io"
	"regexp"
	"strings"

	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

var (
	configKeyOptionRegex = regexp.MustCompile(`^[[:alnum:]]+[-[:alnum:]]*\.(.+\.)*[[:alnum:]]+[-[:alnum:]]*$`)
	// configKeyGlobalRegex is intended to verify config keys when used as
	// global arguments. We're playing it safe here by disallowing lots of
	// keys which git would parse just fine, but we only have a limited
	// number of config entries anyway. Most importantly, we cannot allow
	// `=` as part of the key as that would break parsing of `git -c`.
	configKeyGlobalRegex = regexp.MustCompile(`^[[:alnum:]]+(\.[-/_a-zA-Z0-9]+)+$`)

	flagRegex = regexp.MustCompile(`^(-|--)[[:alnum:]]`)
)

// GlobalOption is an interface for all options which can be globally applied
// to git commands. This is the command-inspecific part before the actual
// command that's being run, e.g. the `-c` part in `git -c foo.bar=value
// command`.
type GlobalOption interface {
	GlobalArgs() ([]string, error)
}

// Option is a git command line flag with validation logic
type Option interface {
	OptionArgs() ([]string, error)
}

// ConfigPair is a sub-command option for use with commands like "git config"
type ConfigPair struct {
	Key   string
	Value string
	// Origin shows the origin type: file, standard input, blob, command line.
	// https://git-scm.com/docs/git-config#Documentation/git-config.txt---show-origin
	Origin string
	// Scope shows the scope of this config value: local, global, system, command.
	// https://git-scm.com/docs/git-config#Documentation/git-config.txt---show-scope
	Scope string
}

// OptionArgs validates the config pair args
func (cp ConfigPair) OptionArgs() ([]string, error) {
	if !configKeyOptionRegex.MatchString(cp.Key) {
		return nil, fmt.Errorf("config key %q failed regexp validation: %w", cp.Key, ErrInvalidArg)
	}
	return []string{cp.Key, cp.Value}, nil
}

// GlobalArgs generates a git `-c <key>=<value>` flag. The key must pass
// validation by containing only alphanumeric sections separated by dots.
// No other characters are allowed for now as `git -c` may not correctly parse
// them, most importantly when they contain equals signs.
func (cp ConfigPair) GlobalArgs() ([]string, error) {
	if !configKeyGlobalRegex.MatchString(cp.Key) {
		return nil, fmt.Errorf("config key %q failed regexp validation: %w", cp.Key, ErrInvalidArg)
	}
	return []string{"-c", fmt.Sprintf("%s=%s", cp.Key, cp.Value)}, nil
}

// Flag is a single token optional command line argument that enables or
// disables functionality (e.g. "-L")
type Flag struct {
	Name string
}

// GlobalArgs returns the arguments for the given flag, which should typically
// only be the flag itself. It returns an error if the flag is not sanitary.
func (f Flag) GlobalArgs() ([]string, error) {
	return f.OptionArgs()
}

// OptionArgs returns an error if the flag is not sanitary
func (f Flag) OptionArgs() ([]string, error) {
	if !flagRegex.MatchString(f.Name) {
		return nil, fmt.Errorf("flag %q failed regex validation: %w", f.Name, ErrInvalidArg)
	}
	return []string{f.Name}, nil
}

// ValueFlag is an optional command line argument that is comprised of pair of
// tokens (e.g. "-n 50")
type ValueFlag struct {
	Name  string
	Value string
}

// GlobalArgs returns the arguments for the given value flag, which should
// typically be two arguments: the flag and its value. It returns an error if the value flag is not sanitary.
func (vf ValueFlag) GlobalArgs() ([]string, error) {
	return vf.OptionArgs()
}

// OptionArgs returns an error if the flag is not sanitary
func (vf ValueFlag) OptionArgs() ([]string, error) {
	if !flagRegex.MatchString(vf.Name) {
		return nil, fmt.Errorf("value flag %q failed regex validation: %w", vf.Name, ErrInvalidArg)
	}
	return []string{vf.Name, vf.Value}, nil
}

// ConvertGlobalOptions converts a protobuf message to a CmdOpt.
func ConvertGlobalOptions(options *gitalypb.GlobalOptions) []CmdOpt {
	if options != nil && options.GetLiteralPathspecs() {
		return []CmdOpt{
			WithEnv("GIT_LITERAL_PATHSPECS=1"),
		}
	}

	return nil
}

// ConvertConfigOptions converts `<key>=<value>` config entries into `ConfigPairs`.
func ConvertConfigOptions(options []string) ([]ConfigPair, error) {
	configPairs := make([]ConfigPair, len(options))

	for i, option := range options {
		configPair := strings.SplitN(option, "=", 2)
		if len(configPair) != 2 {
			return nil, fmt.Errorf("cannot convert invalid config key: %q", option)
		}

		configPairs[i] = ConfigPair{Key: configPair[0], Value: configPair[1]}
	}

	return configPairs, nil
}

type cmdCfg struct {
	env             []string
	globals         []GlobalOption
	stdin           io.Reader
	stdout          io.Writer
	stderr          io.Writer
	hooksConfigured bool
}

// CmdOpt is an option for running a command
type CmdOpt func(*cmdCfg) error

// WithStdin sets the command's stdin. Pass `command.SetupStdin` to make the
// command suitable for `Write()`ing to.
func WithStdin(r io.Reader) CmdOpt {
	return func(c *cmdCfg) error {
		c.stdin = r
		return nil
	}
}

// WithStdout sets the command's stdout.
func WithStdout(w io.Writer) CmdOpt {
	return func(c *cmdCfg) error {
		c.stdout = w
		return nil
	}
}

// WithStderr sets the command's stderr.
func WithStderr(w io.Writer) CmdOpt {
	return func(c *cmdCfg) error {
		c.stderr = w
		return nil
	}
}

// WithEnv adds environment variables to the command.
func WithEnv(envs ...string) CmdOpt {
	return func(c *cmdCfg) error {
		c.env = append(c.env, envs...)
		return nil
	}
}

// WithConfig adds git configuration entries to the command.
func WithConfig(configPairs ...ConfigPair) CmdOpt {
	return func(c *cmdCfg) error {
		for _, configPair := range configPairs {
			c.globals = append(c.globals, configPair)
		}
		return nil
	}
}

// WithConfigEnv adds git configuration entries to the command's environment. This should be used
// in place of `WithConfig()` in case config entries may contain secrets which shouldn't leak e.g.
// via the process's command line.
func WithConfigEnv(configPairs ...ConfigPair) CmdOpt {
	return func(c *cmdCfg) error {
		env := make([]string, 0, len(configPairs)*2+1)

		for i, configPair := range configPairs {
			env = append(env,
				fmt.Sprintf("GIT_CONFIG_KEY_%d=%s", i, configPair.Key),
				fmt.Sprintf("GIT_CONFIG_VALUE_%d=%s", i, configPair.Value),
			)
		}
		env = append(env, fmt.Sprintf("GIT_CONFIG_COUNT=%d", len(configPairs)))

		c.env = append(c.env, env...)
		return nil
	}
}

// WithGlobalOption adds the global options to the command. These are universal options which work
// across all git commands.
func WithGlobalOption(opts ...GlobalOption) CmdOpt {
	return func(c *cmdCfg) error {
		c.globals = append(c.globals, opts...)
		return nil
	}
}
