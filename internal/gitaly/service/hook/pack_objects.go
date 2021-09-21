package hook

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"strings"
	"syscall"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/pktline"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/hook"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/v14/streamio"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

var (
	packObjectsServedBytes = promauto.NewCounter(prometheus.CounterOpts{
		Name: "gitaly_pack_objects_served_bytes_total",
		Help: "Number of bytes of git-pack-objects data served to clients",
	})
	packObjectsCacheLookups = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "gitaly_pack_objects_cache_lookups_total",
		Help: "Number of lookups in the PackObjectsHook cache, divided by hit/miss",
	}, []string{"result"})
	packObjectsGeneratedBytes = promauto.NewCounter(prometheus.CounterOpts{
		Name: "gitaly_pack_objects_generated_bytes_total",
		Help: "Number of bytes generated in PackObjectsHook by running git-pack-objects",
	})
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

	stdin := streamio.NewReader(func() ([]byte, error) {
		resp, err := stream.Recv()
		return resp.GetStdin(), err
	})

	output := func(r io.Reader) (int64, error) {
		var n int64
		err := pktline.EachSidebandPacket(r, func(band byte, data []byte) error {
			resp := &gitalypb.PackObjectsHookResponse{}

			switch band {
			case bandStdout:
				resp.Stdout = data
			case bandStderr:
				resp.Stderr = data
			default:
				return fmt.Errorf("invalid side band: %d", band)
			}

			n += int64(len(data))
			return stream.Send(resp)
		})
		return n, err
	}

	if err := s.packObjectsHook(stream.Context(), firstRequest.Repository, firstRequest, args, stdin, output); err != nil {
		return helper.ErrInternal(err)
	}

	return nil
}

const (
	bandStdout = 1
	bandStderr = 2
)

func (s *server) packObjectsHook(ctx context.Context, repo *gitalypb.Repository, reqHash proto.Message, args *packObjectsArgs, stdinReader io.Reader, output func(io.Reader) (int64, error)) error {
	data, err := protojson.Marshal(reqHash)
	if err != nil {
		return err
	}

	h := sha256.New()
	if _, err := h.Write(data); err != nil {
		return err
	}

	stdin, err := bufferStdin(stdinReader, h)
	if err != nil {
		return err
	}

	// We do not know yet who has to close stdin. In case of a cache hit, it
	// is us. In case of a cache miss, a separate goroutine will run
	// git-pack-objects, and that goroutine may outlive the current request.
	// In that case, that separate goroutine will be responsible for closing
	// stdin.
	closeStdin := true
	defer func() {
		if closeStdin {
			stdin.Close()
		}
	}()

	key := hex.EncodeToString(h.Sum(nil))

	r, created, err := s.packObjectsCache.FindOrCreate(key, func(w io.Writer) error {
		return s.runPackObjects(ctx, w, repo, args, stdin, key)
	})
	if err != nil {
		return err
	}
	defer r.Close()

	if created {
		closeStdin = false
		packObjectsCacheLookups.WithLabelValues("miss").Inc()
	} else {
		packObjectsCacheLookups.WithLabelValues("hit").Inc()
	}

	var servedBytes int64
	defer func() {
		ctxlogrus.Extract(ctx).WithFields(logrus.Fields{
			"cache_key": key,
			"bytes":     servedBytes,
		}).Info("served bytes")
		packObjectsServedBytes.Add(float64(servedBytes))
	}()

	servedBytes, err = output(r)
	if err != nil {
		return err
	}

	return r.Wait(ctx)
}

type contextWithoutCancel struct {
	context.Context
	valueCtx context.Context
}

func (cwc *contextWithoutCancel) Value(key interface{}) interface{} { return cwc.valueCtx.Value(key) }

func cloneContextValues(ctx context.Context) context.Context {
	return &contextWithoutCancel{
		Context:  context.Background(),
		valueCtx: ctx,
	}
}

func (s *server) runPackObjects(ctx context.Context, w io.Writer, repo *gitalypb.Repository, args *packObjectsArgs, stdin io.ReadCloser, key string) error {
	// We want to keep the context for logging, but we want to block all its
	// cancelation signals (deadline, cancel etc.). This is because of
	// the following scenario. Imagine client1 calls PackObjectsHook and
	// causes runPackObjects to run in a goroutine. Now suppose that client2
	// calls PackObjectsHook with the same arguments and stdin, so it joins
	// client1 in waiting for this goroutine. Now client1 hangs up before the
	// runPackObjects goroutine is done.
	//
	// If the cancelation of client1 propagated into the runPackObjects
	// goroutine this would affect client2. We don't want that. So to prevent
	// that, we suppress the cancelation of the originating context.
	ctx = cloneContextValues(ctx)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	defer stdin.Close()

	sw := pktline.NewSidebandWriter(w)
	stdoutBufWriter := bufio.NewWriterSize(sw.Writer(bandStdout), pktline.MaxSidebandData)
	stdout := &countingWriter{W: stdoutBufWriter}
	stderrBuf := &bytes.Buffer{}
	stderr := &countingWriter{W: io.MultiWriter(sw.Writer(bandStderr), stderrBuf)}

	defer func() {
		generatedBytes := stdout.N + stderr.N
		packObjectsGeneratedBytes.Add(float64(generatedBytes))
		logger := ctxlogrus.Extract(ctx)
		logger.WithFields(logrus.Fields{
			"cache_key": key,
			"bytes":     generatedBytes,
		}).Info("generated bytes")

		if total := totalMessage(stderrBuf.Bytes()); total != "" {
			logger.WithField("pack.stat", total).Info("pack file compression statistic")
		}
	}()

	cmd, err := s.gitCmdFactory.New(ctx, repo, args.subcmd(),
		git.WithStdin(stdin),
		git.WithStdout(stdout),
		git.WithStderr(stderr),
		git.WithGlobalOption(args.globals()...),
	)
	if err != nil {
		return err
	}
	if err := cmd.Wait(); err != nil {
		return fmt.Errorf("git-pack-objects: stderr: %q err: %w", stderrBuf.String(), err)
	}
	if err := stdoutBufWriter.Flush(); err != nil {
		return fmt.Errorf("flush stdout: %w", err)
	}
	return nil
}

func totalMessage(stderr []byte) string {
	start := bytes.Index(stderr, []byte("Total "))
	if start < 0 {
		return ""
	}

	end := bytes.Index(stderr[start:], []byte("\n"))
	if end < 0 {
		return ""
	}

	return string(stderr[start : start+end])
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
		globals = append(globals, git.ValueFlag{Name: "--shallow-file", Value: ""})
	}
	return globals
}

func (p *packObjectsArgs) subcmd() git.SubCmd {
	sc := git.SubCmd{
		Name:  "pack-objects",
		Flags: []git.Option{git.Flag{Name: "--stdout"}},
	}
	for _, f := range p.flags {
		sc.Flags = append(sc.Flags, git.Flag{Name: f})
	}
	return sc
}

func bufferStdin(r io.Reader, h hash.Hash) (_ io.ReadCloser, err error) {
	f, err := os.CreateTemp("", "PackObjectsHook-stdin")
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			f.Close()
		}
	}()

	if err := os.Remove(f.Name()); err != nil {
		return nil, err
	}

	_, err = io.Copy(f, io.TeeReader(r, h))
	if err != nil {
		return nil, err
	}

	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}

	return f, nil
}

type countingWriter struct {
	W io.Writer
	N int64
}

func (cw *countingWriter) Write(p []byte) (int, error) {
	n, err := cw.W.Write(p)
	cw.N += int64(n)
	return n, err
}

func (s *server) PackObjectsHookWithSidechannel(ctx context.Context, req *gitalypb.PackObjectsHookWithSidechannelRequest) (*gitalypb.PackObjectsHookWithSidechannelResponse, error) {
	if req.GetRepository() == nil {
		return nil, helper.ErrInvalidArgument(errors.New("repository is empty"))
	}

	args, err := parsePackObjectsArgs(req.Args)
	if err != nil {
		return nil, helper.ErrInvalidArgumentf("invalid pack-objects command: %v: %w", req.Args, err)
	}

	c, err := hook.GetSidechannel(ctx)
	if err != nil {
		return nil, err
	}
	defer c.Close()

	output := func(r io.Reader) (int64, error) { return io.Copy(c, r) }
	if err := s.packObjectsHook(ctx, req.Repository, req, args, c, output); err != nil {
		if errors.Is(err, syscall.EPIPE) {
			// EPIPE is the error we get if we try to write to c after the client has
			// closed its side of the connection. By convention, we label server side
			// errors caused by the client disconnecting with the Canceled gRPC code.
			err = helper.ErrCanceled(err)
		}
		return nil, err
	}

	if err := c.Close(); err != nil {
		return nil, err
	}

	return &gitalypb.PackObjectsHookWithSidechannelResponse{}, nil
}
