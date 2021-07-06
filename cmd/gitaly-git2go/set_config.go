// +build static,system_libgit2

package main

import (
	"context"
	"encoding/gob"
	"errors"
	"flag"
	"fmt"
	"io"

	git "github.com/libgit2/git2go/v31"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git2go"
)

type setConfigSubcommand struct {
}

func (cmd *setConfigSubcommand) Flags() *flag.FlagSet {
	return flag.NewFlagSet("set_config", flag.ExitOnError)
}

func (cmd setConfigSubcommand) setConfig(request git2go.SetConfigCommand) error {
	if request.Repository == "" {
		return errors.New("missing repository")
	}

	git2goRepo, err := git.OpenRepository(request.Repository)
	if err != nil {
		return fmt.Errorf("open repository: %w", err)
	}

	conf, err := git2goRepo.Config()
	if err != nil {
		return fmt.Errorf("getting repository config: %w", err)
	}

	for key, entry := range request.Entries {
		switch v := entry.Value.(type) {
		case bool:
			err = conf.SetBool(key, v)
		case int32:
			err = conf.SetInt32(key, v)
		case string:
			err = conf.SetString(key, v)
		default:
			err = fmt.Errorf("unsupported value type: %T", entry.Value)
		}
		if err != nil {
			return fmt.Errorf("set config value for key %q: %w", key, err)
		}
	}
	return nil
}

func (cmd setConfigSubcommand) Run(_ context.Context, r io.Reader, w io.Writer) error {
	var request git2go.SetConfigCommand
	if err := gob.NewDecoder(r).Decode(&request); err != nil {
		return err
	}

	err := cmd.setConfig(request)
	return gob.NewEncoder(w).Encode(git2go.SetConfingResult{
		Error: git2go.SerializableError(err),
	})
}
