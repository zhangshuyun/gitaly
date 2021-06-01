package git2go

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"path/filepath"
	"strings"

	"gitlab.com/gitlab-org/gitaly/v14/internal/command"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
)

var (
	// ErrInvalidArgument is returned in case the merge arguments are invalid.
	ErrInvalidArgument = errors.New("invalid parameters")
)

func binaryPathFromCfg(cfg config.Cfg) string {
	return filepath.Join(cfg.BinDir, "gitaly-git2go")
}

func run(ctx context.Context, binaryPath string, stdin io.Reader, args ...string) (*bytes.Buffer, error) {
	var stderr, stdout bytes.Buffer
	cmd, err := command.New(ctx, exec.Command(binaryPath, args...), stdin, &stdout, &stderr)
	if err != nil {
		return nil, err
	}

	if err := cmd.Wait(); err != nil {
		if _, ok := err.(*exec.ExitError); ok {
			return nil, fmt.Errorf("%s", stderr.String())
		}
		return nil, err
	}

	return &stdout, nil
}

func serialize(v interface{}) (string, error) {
	marshalled, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(marshalled), nil
}

func deserialize(serialized string, v interface{}) error {
	base64Decoder := base64.NewDecoder(base64.StdEncoding, strings.NewReader(serialized))
	jsonDecoder := json.NewDecoder(base64Decoder)
	return jsonDecoder.Decode(v)
}

func serializeTo(writer io.Writer, v interface{}) error {
	base64Encoder := base64.NewEncoder(base64.StdEncoding, writer)
	defer base64Encoder.Close()
	jsonEncoder := json.NewEncoder(base64Encoder)
	return jsonEncoder.Encode(v)
}
