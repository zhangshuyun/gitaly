package git2go

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"gitlab.com/gitlab-org/gitaly/v14/internal/command"
	"gitlab.com/gitlab-org/gitaly/v14/internal/version"
)

var (
	// ErrInvalidArgument is returned in case the merge arguments are invalid.
	ErrInvalidArgument = errors.New("invalid parameters")

	// BinaryName is a binary name with version suffix .
	BinaryName = rawBinaryName + "-" + version.GetModuleVersion()
)

const rawBinaryName = "gitaly-git2go"

// BinaryPath returns path to the executable binary.
func BinaryPath(binaryFolder string) string {
	// At first try to find the versioned binary
	path := filepath.Join(binaryFolder, BinaryName)
	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		// if not exist fall back to the old unversioned binary
		path = filepath.Join(binaryFolder, rawBinaryName)
	}
	return path
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
