package hook

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"io/ioutil"
	"os"
	"strings"

	"github.com/golang/protobuf/jsonpb"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/pktline"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/v14/streamio"
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
	if s.packObjectsCache == nil {
		return helper.ErrInternalf("packObjectsCache is not available")
	}

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

const (
	bandStdout = 1
	bandStderr = 2
)

func (s *server) packObjectsHook(stream gitalypb.HookService_PackObjectsHookServer, firstRequest *gitalypb.PackObjectsHookRequest, args *packObjectsArgs) error {
	ctx := stream.Context()

	h := sha256.New()
	if err := (&jsonpb.Marshaler{}).Marshal(h, firstRequest); err != nil {
		return err
	}

	stdin, err := bufferStdin(stream, h)
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
		return s.runPackObjects(ctx, w, firstRequest.Repository, args, stdin, key)
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

	if err := pktline.EachSidebandPacket(r, func(band byte, data []byte) error {
		resp := &gitalypb.PackObjectsHookResponse{}

		switch band {
		case bandStdout:
			resp.Stdout = data
		case bandStderr:
			resp.Stderr = data
		default:
			return fmt.Errorf("invalid side band: %d", band)
		}

		servedBytes += int64(len(data))
		return stream.Send(resp)
	}); err != nil {
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
	stdout := &countingWriter{W: sw.Writer(bandStdout)}
	stderrBuf := &bytes.Buffer{}
	stderr := &countingWriter{W: io.MultiWriter(sw.Writer(bandStderr), stderrBuf)}

	defer func() {
		generatedBytes := stdout.N + stderr.N
		packObjectsGeneratedBytes.Add(float64(generatedBytes))
		ctxlogrus.Extract(ctx).WithFields(logrus.Fields{
			"cache_key": key,
			"bytes":     generatedBytes,
		}).Info("generated bytes")
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

func bufferStdin(stream gitalypb.HookService_PackObjectsHookServer, h hash.Hash) (_ io.ReadCloser, err error) {
	f, err := ioutil.TempFile("", "PackObjectsHook-stdin")
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

	stdin := io.TeeReader(
		streamio.NewReader(func() ([]byte, error) {
			resp, err := stream.Recv()
			return resp.GetStdin(), err
		}),
		h,
	)

	_, err = io.Copy(f, stdin)
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
