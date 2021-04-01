package git

import (
	"errors"
	"fmt"
	"regexp"
)

var (
	// ErrInvalidArg represent family of errors to report about bad argument used to make a call.
	ErrInvalidArg = errors.New("invalid argument")
	// ErrHookPayloadRequired indicates a HookPayload is needed but
	// absent from the command.
	ErrHookPayloadRequired = errors.New("hook payload is required but not configured")
)

// Cmd is an interface for safe git commands
type Cmd interface {
	CommandArgs() ([]string, error)
	Subcommand() string
}

// SubCmd represents a specific git command
type SubCmd struct {
	Name        string   // e.g. "log", or "cat-file", or "worktree"
	Flags       []Option // optional flags before the positional args
	Args        []string // positional args after all flags
	PostSepArgs []string // post separator (i.e. "--") positional args
}

// Subcommand returns the subcommand name
func (sc SubCmd) Subcommand() string { return sc.Name }

// CommandArgs checks all arguments in the sub command and validates them
func (sc SubCmd) CommandArgs() ([]string, error) {
	var safeArgs []string

	commandDescription, ok := commandDescriptions[sc.Name]
	if !ok {
		return nil, fmt.Errorf("invalid sub command name %q: %w", sc.Name, ErrInvalidArg)
	}
	safeArgs = append(safeArgs, sc.Name)

	commandArgs, err := commandDescription.args(sc.Flags, sc.Args, sc.PostSepArgs)
	if err != nil {
		return nil, err
	}
	safeArgs = append(safeArgs, commandArgs...)

	return safeArgs, nil
}

// SubSubCmd is a positional argument that appears in the list of options for
// a subcommand.
type SubSubCmd struct {
	// Name is the name of the subcommand, e.g. "remote" in `git remote set-url`
	Name string
	// Action is the action of the subcommand, e.g. "set-url" in `git remote set-url`
	Action string

	// Flags are optional flags before the positional args
	Flags []Option
	// Args are positional arguments after all flags
	Args []string
	// PostSepArgs are positional args after the "--" separator
	PostSepArgs []string
}

// Subcommand returns the name of the given git command which this SubSubCmd
// executes. E.g. for `git remote add`, it would return "remote".
func (sc SubSubCmd) Subcommand() string { return sc.Name }

var actionRegex = regexp.MustCompile(`^[[:alnum:]]+[-[:alnum:]]*$`)

// CommandArgs checks all arguments in the SubSubCommand and validates them,
// returning the array of all arguments required to execute it.
func (sc SubSubCmd) CommandArgs() ([]string, error) {
	var safeArgs []string

	commandDescription, ok := commandDescriptions[sc.Name]
	if !ok {
		return nil, fmt.Errorf("invalid sub command name %q: %w", sc.Name, ErrInvalidArg)
	}
	safeArgs = append(safeArgs, sc.Name)

	if !actionRegex.MatchString(sc.Action) {
		return nil, fmt.Errorf("invalid sub command action %q: %w", sc.Action, ErrInvalidArg)
	}
	safeArgs = append(safeArgs, sc.Action)

	commandArgs, err := commandDescription.args(sc.Flags, sc.Args, sc.PostSepArgs)
	if err != nil {
		return nil, err
	}
	safeArgs = append(safeArgs, commandArgs...)

	return safeArgs, nil
}

// IsInvalidArgErr relays if the error is due to an argument validation failure
func IsInvalidArgErr(err error) bool {
	return errors.Is(err, ErrInvalidArg)
}
