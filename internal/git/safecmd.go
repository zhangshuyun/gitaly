package git

import (
	"context"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"gitlab.com/gitlab-org/gitaly/internal/command"
	"gitlab.com/gitlab-org/gitaly/internal/git/repository"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

var (
	invalidationTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "gitaly_invalid_commands_total",
			Help: "Total number of invalid arguments tried to execute",
		},
		[]string{"command"},
	)

	// ErrInvalidArg represent family of errors to report about bad argument used to make a call.
	ErrInvalidArg = errors.New("invalid argument")
)

func init() {
	prometheus.MustRegister(invalidationTotal)
}

func incrInvalidArg(subcmdName string) {
	invalidationTotal.WithLabelValues(subcmdName).Inc()
}

// Cmd is an interface for safe git commands
type Cmd interface {
	ValidateArgs() ([]string, error)
	IsCmd()
	Subcommand() string
}

// SubCmd represents a specific git command
type SubCmd struct {
	Name        string   // e.g. "log", or "cat-file", or "worktree"
	Flags       []Option // optional flags before the positional args
	Args        []string // positional args after all flags
	PostSepArgs []string // post separator (i.e. "--") positional args
}

// CmdStream represents standard input/output streams for a command
type CmdStream struct {
	In       io.Reader // standard input
	Out, Err io.Writer // standard output and error
}

var subCmdNameRegex = regexp.MustCompile(`^[[:alnum:]]+(-[[:alnum:]]+)*$`)

// IsCmd allows SubCmd to satisfy the Cmd interface
func (sc SubCmd) IsCmd() {}

// Subcommand returns the subcommand name
func (sc SubCmd) Subcommand() string { return sc.Name }

func (sc SubCmd) supportsEndOfOptions() bool {
	switch sc.Name {
	case "checkout", "linguist", "for-each-ref", "archive", "upload-archive", "grep", "clone", "config", "rev-parse", "remote", "blame", "ls-tree":
		return false
	}
	return true
}

// ValidateArgs checks all arguments in the sub command and validates them
func (sc SubCmd) ValidateArgs() ([]string, error) {
	var safeArgs []string

	if !subCmdNameRegex.MatchString(sc.Name) {
		return nil, fmt.Errorf("invalid sub command name %q: %w", sc.Name, ErrInvalidArg)
	}
	safeArgs = append(safeArgs, sc.Name)

	for _, o := range sc.Flags {
		args, err := o.ValidateArgs()
		if err != nil {
			return nil, err
		}
		safeArgs = append(safeArgs, args...)
	}

	for _, a := range sc.Args {
		if err := validatePositionalArg(a); err != nil {
			return nil, err
		}
		safeArgs = append(safeArgs, a)
	}

	if sc.supportsEndOfOptions() {
		safeArgs = append(safeArgs, "--end-of-options")
	}

	if len(sc.PostSepArgs) > 0 {
		safeArgs = append(safeArgs, "--")
	}

	// post separator args do not need any validation
	safeArgs = append(safeArgs, sc.PostSepArgs...)

	return safeArgs, nil
}

// Option is a git command line flag with validation logic
type Option interface {
	IsOption()
	ValidateArgs() ([]string, error)
}

// SubSubCmd is a positional argument that appears in the list of options for
// a subcommand.
type SubSubCmd struct {
	Name string
}

// IsOption is a method present on all Flag interface implementations
func (SubSubCmd) IsOption() {}

// ValidateArgs returns an error if the command name or options are not
// sanitary
func (sc SubSubCmd) ValidateArgs() ([]string, error) {
	if !subCmdNameRegex.MatchString(sc.Name) {
		return nil, fmt.Errorf("invalid sub-sub command name %q: %w", sc.Name, ErrInvalidArg)
	}
	return []string{sc.Name}, nil
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

// IsOption is a method present on all Flag interface implementations
func (ConfigPair) IsOption() {}

var configKeyRegex = regexp.MustCompile(`^[[:alnum:]]+[-[:alnum:]]*\.(.+\.)*[[:alnum:]]+[-[:alnum:]]*$`)

// ValidateArgs validates the config pair args
func (cp ConfigPair) ValidateArgs() ([]string, error) {
	if !configKeyRegex.MatchString(cp.Key) {
		return nil, fmt.Errorf("config key %q failed regexp validation: %w", cp.Key, ErrInvalidArg)
	}
	return []string{cp.Key, cp.Value}, nil
}

// Flag is a single token optional command line argument that enables or
// disables functionality (e.g. "-L")
type Flag struct {
	Name string
}

// IsOption is a method present on all Flag interface implementations
func (Flag) IsOption() {}

// ValidateArgs returns an error if the flag is not sanitary
func (f Flag) ValidateArgs() ([]string, error) {
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

// IsOption is a method present on all Flag interface implementations
func (ValueFlag) IsOption() {}

// ValidateArgs returns an error if the flag is not sanitary
func (vf ValueFlag) ValidateArgs() ([]string, error) {
	if !flagRegex.MatchString(vf.Name) {
		return nil, fmt.Errorf("value flag %q failed regex validation: %w", vf.Name, ErrInvalidArg)
	}
	return []string{vf.Name, vf.Value}, nil
}

var flagRegex = regexp.MustCompile(`^(-|--)[[:alnum:]]`)

// IsInvalidArgErr relays if the error is due to an argument validation failure
func IsInvalidArgErr(err error) bool {
	return errors.Is(err, ErrInvalidArg)
}

func validatePositionalArg(arg string) error {
	if strings.HasPrefix(arg, "-") {
		return fmt.Errorf("positional arg %q cannot start with dash '-': %w", arg, ErrInvalidArg)
	}
	return nil
}

// ConvertGlobalOptions converts a protobuf message to command-line flags
func ConvertGlobalOptions(options *gitalypb.GlobalOptions) []Option {
	var globals []Option

	if options == nil {
		return globals
	}

	if options.GetLiteralPathspecs() {
		globals = append(globals, Flag{"--literal-pathspecs"})
	}

	return globals
}

type cmdCfg struct {
	env               []string
	stdin             io.Reader
	stdout            io.Writer
	stderr            io.Writer
	refHookConfigured bool
}

// CmdOpt is an option for running a command
type CmdOpt func(*cmdCfg) error

// WithStdin sets the command's stdin.
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

var (
	// ErrRefHookRequired indicates a ref hook configuration is needed but
	// absent from the command
	ErrRefHookRequired = errors.New("ref hook is required but not configured")
	// ErrRefHookNotRequired indicates an extraneous ref hook option was
	// provided
	ErrRefHookNotRequired = errors.New("ref hook is configured but not required")
)

func handleOpts(ctx context.Context, sc Cmd, cc *cmdCfg, opts []CmdOpt) error {
	for _, opt := range opts {
		if err := opt(cc); err != nil {
			return err
		}
	}

	if !cc.refHookConfigured && mayUpdateRef(sc.Subcommand()) {
		return fmt.Errorf("subcommand %q: %w", sc.Subcommand(), ErrRefHookRequired)
	}
	if cc.refHookConfigured && !mayUpdateRef(sc.Subcommand()) {
		return fmt.Errorf("subcommand %q: %w", sc.Subcommand(), ErrRefHookNotRequired)
	}

	return nil
}

// SafeCmd creates a command.Command with the given args and Repository. It
// validates the arguments in the command before executing.
func SafeCmd(ctx context.Context, repo repository.GitRepo, globals []Option, sc Cmd, opts ...CmdOpt) (*command.Command, error) {
	return SafeCmdWithEnv(ctx, nil, repo, globals, sc, opts...)
}

// SafeCmdWithEnv creates a command.Command with the given args, environment, and Repository. It
// validates the arguments in the command before executing.
func SafeCmdWithEnv(ctx context.Context, env []string, repo repository.GitRepo, globals []Option, sc Cmd, opts ...CmdOpt) (*command.Command, error) {
	cc := &cmdCfg{}

	if err := handleOpts(ctx, sc, cc, opts); err != nil {
		return nil, err
	}

	args, err := combineArgs(globals, sc)
	if err != nil {
		return nil, err
	}

	return unsafeCmdWithEnv(ctx, append(env, cc.env...), CmdStream{
		In:  cc.stdin,
		Out: cc.stdout,
		Err: cc.stderr,
	}, repo, args...)
}

// SafeBareCmd creates a git.Command with the given args, stream, and env. It
// validates the arguments in the command before executing.
func SafeBareCmd(ctx context.Context, stream CmdStream, env []string, globals []Option, sc Cmd, opts ...CmdOpt) (*command.Command, error) {
	cc := &cmdCfg{}

	if err := handleOpts(ctx, sc, cc, opts); err != nil {
		return nil, err
	}

	args, err := combineArgs(globals, sc)
	if err != nil {
		return nil, err
	}

	return unsafeBareCmd(ctx, stream, append(env, cc.env...), args...)
}

// SafeBareCmdInDir runs SafeBareCmd in the dir.
func SafeBareCmdInDir(ctx context.Context, dir string, stream CmdStream, env []string, globals []Option, sc Cmd) (*command.Command, error) {
	if dir == "" {
		return nil, errors.New("no 'dir' provided")
	}

	args, err := combineArgs(globals, sc)
	if err != nil {
		return nil, err
	}

	return unsafeBareCmdInDir(ctx, dir, stream, env, args...)
}

// SafeStdinCmd creates a git.Command with the given args and Repository that is
// suitable for Write()ing to. It validates the arguments in the command before
// executing.
func SafeStdinCmd(ctx context.Context, repo repository.GitRepo, globals []Option, sc SubCmd, opts ...CmdOpt) (*command.Command, error) {
	cc := &cmdCfg{}

	if err := handleOpts(ctx, sc, cc, opts); err != nil {
		return nil, err
	}

	args, err := combineArgs(globals, sc)
	if err != nil {
		return nil, err
	}

	return unsafeStdinCmd(ctx, cc.env, repo, args...)
}

// SafeCmdWithoutRepo works like Command but without a git repository. It
// validates the arguments in the command before executing.
func SafeCmdWithoutRepo(ctx context.Context, stream CmdStream, globals []Option, sc SubCmd) (*command.Command, error) {
	args, err := combineArgs(globals, sc)
	if err != nil {
		return nil, err
	}

	return unsafeCmdWithoutRepo(ctx, stream, args...)
}

func combineArgs(globals []Option, sc Cmd) (_ []string, err error) {
	var args []string

	defer func() {
		if err != nil && IsInvalidArgErr(err) && len(args) > 0 {
			incrInvalidArg(args[0])
		}
	}()

	for _, g := range globals {
		gargs, err := g.ValidateArgs()
		if err != nil {
			return nil, err
		}
		args = append(args, gargs...)
	}

	scArgs, err := sc.ValidateArgs()
	if err != nil {
		return nil, err
	}

	return append(args, scArgs...), nil
}
