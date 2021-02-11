package hook

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/golang/protobuf/jsonpb"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/streamio"
	"gitlab.com/gitlab-org/labkit/log"
)

var (
	packObjectsStreamBytes = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "gitaly_pack_objects_stream_bytes_total",
		Help: "Number of bytes of git-pack-objects data read from / returned to clients",
	}, []string{"stream"})
)

func (s *server) PackObjectsHook(stream gitalypb.HookService_PackObjectsHookServer) error {
	firstRequest, err := stream.Recv()
	if err != nil {
		return helper.ErrInternal(err)
	}

	if firstRequest.GetRepository() == nil {
		return helper.ErrInvalidArgument(errors.New("repository is empty"))
	}

	args, err := parsePackObjectsArgs(firstRequest.Args)
	if err != nil {
		return helper.ErrInvalidArgumentf("invalid pack-objects command: %v: %w", firstRequest.Args, err)
	}

	if err := s.packObjectsHook(stream, firstRequest, args); err != nil {
		return helper.ErrInternal(err)
	}

	return nil
}

func (s *server) packObjectsHook(stream gitalypb.HookService_PackObjectsHookServer, firstRequest *gitalypb.PackObjectsHookRequest, args *packObjectsArgs) error {
	ctx := stream.Context()

	h := sha256.New()
	if err := (&jsonpb.Marshaler{}).Marshal(h, firstRequest); err != nil {
		return err
	}

	var stdinBytes, stdoutBytes, stderrBytes int64
	defer func() {
		ctxlogrus.Extract(ctx).WithFields(logrus.Fields{
			"cache_key":    hex.EncodeToString(h.Sum(nil)),
			"stdin_bytes":  stdinBytes,
			"stdout_bytes": stdoutBytes,
			"stderr_bytes": stderrBytes,
		}).Info("git-pack-objects stats")
	}()

	stdin := io.TeeReader(
		streamio.NewReader(func() ([]byte, error) {
			resp, err := stream.Recv()
			p := resp.GetStdin()
			stdinBytes += int64(len(p))
			packObjectsStreamBytes.WithLabelValues("stdin").Add(float64(len(p)))
			return p, err
		}),
		h,
	)

	m := &sync.Mutex{}
	stdout := streamio.NewSyncWriter(m, func(p []byte) error {
		stdoutBytes += int64(len(p))
		packObjectsStreamBytes.WithLabelValues("stdout").Add(float64(len(p)))
		return stream.Send(&gitalypb.PackObjectsHookResponse{Stdout: p})
	})
	stderrBuf := &bytes.Buffer{}
	stderr := io.MultiWriter(
		stderrBuf,
		streamio.NewSyncWriter(m, func(p []byte) error {
			atomic.AddInt64(&stderrBytes, int64(len(p)))
			packObjectsStreamBytes.WithLabelValues("stderr").Add(float64(len(p)))
			return stream.Send(&gitalypb.PackObjectsHookResponse{Stderr: p})
		}),
	)

	cmd, err := s.gitCmdFactory.New(ctx, firstRequest.GetRepository(), args.globals(), args.subcmd(),
		git.WithStdin(stdin),
		git.WithStdout(stdout),
		git.WithStderr(stderr),
	)
	if err != nil {
		return err
	}

	if err := cmd.Wait(); err != nil {
		log.WithField("stderr", stderrBuf.String()).Error("git-pack-objects failed")
		return err
	}

	return nil
}

var (
	errNoPackObjects = errors.New("missing pack-objects")
	errNonFlagArg    = errors.New("non-flag argument")
	errNoStdout      = errors.New("missing --stdout")
)

func parsePackObjectsArgs(args []string) (*packObjectsArgs, error) {
	result := &packObjectsArgs{}

	// Check for special argument used with shallow clone:
	// https://gitlab.com/gitlab-org/git/-/blob/v2.30.0/upload-pack.c#L287-290
	if len(args) >= 2 && args[0] == "--shallow-file" && args[1] == "" {
		result.shallowFile = true
		args = args[2:]
	}

	if len(args) < 1 || args[0] != "pack-objects" {
		return nil, errNoPackObjects
	}
	args = args[1:]

	// There should always be "--stdout" somewhere. Git-pack-objects can
	// write to a file too but we don't want that in this RPC.
	// https://gitlab.com/gitlab-org/git/-/blob/v2.30.0/upload-pack.c#L296
	seenStdout := false
	for _, a := range args {
		if !strings.HasPrefix(a, "-") {
			return nil, errNonFlagArg
		}
		if a == "--stdout" {
			seenStdout = true
		} else {
			result.flags = append(result.flags, a)
		}
	}

	if !seenStdout {
		return nil, errNoStdout
	}

	return result, nil
}

type packObjectsArgs struct {
	shallowFile bool
	flags       []string
}

func (p *packObjectsArgs) globals() []git.GlobalOption {
	var globals []git.GlobalOption
	if p.shallowFile {
		globals = append(globals, git.ValueFlag{"--shallow-file", ""})
	}
	return globals
}

func (p *packObjectsArgs) subcmd() git.SubCmd {
	sc := git.SubCmd{
		Name:  "pack-objects",
		Flags: []git.Option{git.Flag{"--stdout"}},
	}
	for _, f := range p.flags {
		sc.Flags = append(sc.Flags, git.Flag{f})
	}
	return sc
}
