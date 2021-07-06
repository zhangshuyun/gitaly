package git2go

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"

	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
)

// ConfigEntry interface value with defined type.
type ConfigEntry struct {
	Value interface{}
}

// SetConfigCommand contains parameters to perform setting of config entries.
type SetConfigCommand struct {
	// Repository is the path to repository.
	Repository string `json:"repository"`
	// Entries key-value config entries.
	Entries map[string]ConfigEntry `json:"entries"`
}

// SetConfingResult contains results from set config action.
type SetConfingResult struct {
	// Possible Error from git2go binary.
	Error error `json:"error"`
}

// Run attempts to set all entries to config
func (s SetConfigCommand) Run(ctx context.Context, cfg config.Cfg) error {
	input := &bytes.Buffer{}
	if err := gob.NewEncoder(input).Encode(s); err != nil {
		return fmt.Errorf("resolve: %w", err)
	}

	stdout, err := run(ctx, BinaryPath(cfg.BinDir), input, "set_config")
	if err != nil {
		return err
	}

	var response SetConfingResult

	if err := gob.NewDecoder(stdout).Decode(&response); err != nil {
		return fmt.Errorf("decodе response: %w", err)
	}

	return response.Error
}
