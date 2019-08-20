package blob

import (
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"os/exec"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"gitlab.com/gitlab-org/gitaly/internal/command"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/helper/chunk"
	"gitlab.com/gitlab-org/gitaly/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/internal/rubyserver"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	// These limits are used as a heuristic to ignore files which can't be LFS
	// pointers. The format of these is described in
	// https://github.com/git-lfs/git-lfs/blob/master/docs/spec.md#the-pointer

	// LfsPointerMinSize is the minimum size for an lfs pointer text blob
	LfsPointerMinSize = 120
	// LfsPointerMaxSize is the minimum size for an lfs pointer text blob
	LfsPointerMaxSize = 200
)

type getLFSPointerByRevisionRequest interface {
	GetRepository() *gitalypb.Repository
	GetRevision() []byte
}

func (s *server) GetLFSPointers(req *gitalypb.GetLFSPointersRequest, stream gitalypb.BlobService_GetLFSPointersServer) error {
	ctx := stream.Context()

	if err := validateGetLFSPointersRequest(req); err != nil {
		return status.Errorf(codes.InvalidArgument, "GetLFSPointers: %v", err)
	}

	client, err := s.BlobServiceClient(ctx)
	if err != nil {
		return err
	}

	clientCtx, err := rubyserver.SetHeaders(ctx, req.GetRepository())
	if err != nil {
		return err
	}

	rubyStream, err := client.GetLFSPointers(clientCtx, req)
	if err != nil {
		return err
	}

	return rubyserver.Proxy(func() error {
		resp, err := rubyStream.Recv()
		if err != nil {
			md := rubyStream.Trailer()
			stream.SetTrailer(md)
			return err
		}
		return stream.Send(resp)
	})
}

func validateGetLFSPointersRequest(req *gitalypb.GetLFSPointersRequest) error {
	if req.GetRepository() == nil {
		return fmt.Errorf("empty Repository")
	}

	if len(req.GetBlobIds()) == 0 {
		return fmt.Errorf("empty BlobIds")
	}

	return nil
}

func (s *server) GetNewLFSPointers(in *gitalypb.GetNewLFSPointersRequest, stream gitalypb.BlobService_GetNewLFSPointersServer) error {
	ctx := stream.Context()

	if err := validateGetLfsPointersByRevisionRequest(in); err != nil {
		return status.Errorf(codes.InvalidArgument, "GetNewLFSPointers: %v", err)
	}

	client, err := s.BlobServiceClient(ctx)
	if err != nil {
		return err
	}

	clientCtx, err := rubyserver.SetHeaders(ctx, in.GetRepository())
	if err != nil {
		return err
	}

	rubyStream, err := client.GetNewLFSPointers(clientCtx, in)
	if err != nil {
		return err
	}

	return rubyserver.Proxy(func() error {
		resp, err := rubyStream.Recv()
		if err != nil {
			md := rubyStream.Trailer()
			stream.SetTrailer(md)
			return err
		}
		return stream.Send(resp)
	})
}

var getAllLFSPointersRequests = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "gitaly_get_all_lfs_pointers_total",
		Help: "Counter of go vs ruby implementation of GetAllLFSPointers",
	},
	[]string{"implementation"},
)

func init() {
	prometheus.MustRegister(getAllLFSPointersRequests)
}

func (s *server) GetAllLFSPointers(in *gitalypb.GetAllLFSPointersRequest, stream gitalypb.BlobService_GetAllLFSPointersServer) error {
	ctx := stream.Context()

	if err := validateGetLfsPointersByRevisionRequest(in); err != nil {
		return helper.ErrInvalidArgument(err)
	}

	if featureflag.IsEnabled(stream.Context(), featureflag.GetAllLFSPointersGo) {
		getAllLFSPointersRequests.WithLabelValues("go").Inc()

		if err := getAllLFSPointers(in.GetRepository(), stream); err != nil {
			return helper.ErrInternal(err)
		}

		return nil
	}

	if featureflag.IsEnabled(stream.Context(), "awk") {
		getAllLFSPointersRequests.WithLabelValues("awk").Inc()
		if err := getAllLFSPointersAwk(in.GetRepository(), stream); err != nil {
			return helper.ErrInternal(err)
		}

		return nil
	}

	if featureflag.IsEnabled(stream.Context(), "awk2") {
		getAllLFSPointersRequests.WithLabelValues("awk2").Inc()
		if err := getAllLFSPointersAwk2(in.GetRepository(), stream); err != nil {
			return helper.ErrInternal(err)
		}

		return nil
	}

	if featureflag.IsEnabled(stream.Context(), "awk3") {
		getAllLFSPointersRequests.WithLabelValues("awk3").Inc()
		if err := getAllLFSPointersAwk3(in.GetRepository(), stream); err != nil {
			return helper.ErrInternal(err)
		}

		return nil
	}

	if featureflag.IsEnabled(stream.Context(), "smallblob") {
		getAllLFSPointersRequests.WithLabelValues("smallblob").Inc()
		if err := getAllLFSPointersSmallBlob(in.GetRepository(), stream); err != nil {
			return helper.ErrInternal(err)
		}

		return nil
	}

	getAllLFSPointersRequests.WithLabelValues("ruby").Inc()

	client, err := s.BlobServiceClient(ctx)
	if err != nil {
		return err
	}

	clientCtx, err := rubyserver.SetHeaders(ctx, in.GetRepository())
	if err != nil {
		return err
	}

	rubyStream, err := client.GetAllLFSPointers(clientCtx, in)
	if err != nil {
		return err
	}

	return rubyserver.Proxy(func() error {
		resp, err := rubyStream.Recv()
		if err != nil {
			md := rubyStream.Trailer()
			stream.SetTrailer(md)
			return err
		}
		return stream.Send(resp)
	})
}

func validateGetLfsPointersByRevisionRequest(in getLFSPointerByRevisionRequest) error {
	if in.GetRepository() == nil {
		return fmt.Errorf("empty Repository")
	}

	return git.ValidateRevision(in.GetRevision())
}

type allLFSPointersSender struct {
	stream      gitalypb.BlobService_GetAllLFSPointersServer
	lfsPointers []*gitalypb.LFSPointer
}

func (s *allLFSPointersSender) Reset() { s.lfsPointers = nil }
func (s *allLFSPointersSender) Append(it chunk.Item) {
	s.lfsPointers = append(s.lfsPointers, it.(*gitalypb.LFSPointer))
}
func (s *allLFSPointersSender) Send() error {
	return s.stream.Send(&gitalypb.GetAllLFSPointersResponse{LfsPointers: s.lfsPointers})
}

func getAllLFSPointers(repository *gitalypb.Repository, stream gitalypb.BlobService_GetAllLFSPointersServer) error {
	args := []string{"rev-list", "--all", "--in-commit-order", "--objects", "--use-bitmap-index"}

	cmd, err := git.Command(stream.Context(), repository, args...)
	if err != nil {
		return err
	}

	chunker := chunk.New(&allLFSPointersSender{stream: stream, lfsPointers: nil})

	s := bufio.NewScanner(cmd)
	c, err := catfile.New(stream.Context(), repository)
	if err != nil {
		return err
	}

	for s.Scan() {
		line := strings.SplitN(s.Text(), " ", 2)
		if len(line) == 0 {
			continue
		}

		oid := line[0]

		info, err := c.Info(oid)
		if err != nil {
			return err
		}

		if info.Type != "blob" || info.Size < LfsPointerMinSize || info.Size > LfsPointerMaxSize {
			continue
		}

		r, err := c.Blob(oid)
		if err != nil {
			return err
		}

		b, err := ioutil.ReadAll(r)
		if err != nil {
			return err
		}

		if !git.IsLFSPointer(b) {
			continue
		}

		if err := chunker.Send(&gitalypb.LFSPointer{
			Oid:  oid,
			Size: info.Size,
			Data: b,
		}); err != nil {
			return err
		}
	}

	if err := cmd.Wait(); err != nil {
		return err
	}

	if err := chunker.Flush(); err != nil {
		return err
	}

	return nil
}

func getAllLFSPointersAwk(repository *gitalypb.Repository, stream gitalypb.BlobService_GetAllLFSPointersServer) error {
	var commands []*command.Command
	args := []string{"rev-list", "--all", "--in-commit-order", "--objects", "--use-bitmap-index"}

	revList, err := git.Command(stream.Context(), repository, args...)
	if err != nil {
		return err
	}

	repoPath, err := helper.GetRepoPath(repository)
	if err != nil {
		return err
	}

	commands = append(commands, revList)
	ctx := stream.Context()
	awk1, err := command.New(ctx, exec.Command("awk", "{print $1}"), revList, nil, nil)
	if err != nil {
		return err
	}
	commands = append(commands, awk1)
	catCheck, err := command.New(ctx, exec.Command("git", "-C", repoPath, "cat-file", "--batch-check", "--buffer"), awk1, nil, nil)
	if err != nil {
		return err
	}
	commands = append(commands, catCheck)
	awk2, err := command.New(ctx, exec.Command("awk", `$2 == "blob" && $3 <= 200 && $3 >= 100 {print $1}`), catCheck, nil, nil)
	if err != nil {
		return err
	}
	commands = append(commands, awk2)
	catBatch, err := command.New(ctx, exec.Command("git", "-C", repoPath, "cat-file", "--batch", "--buffer"), awk2, nil, nil)
	if err != nil {
		return err
	}
	commands = append(commands, catBatch)

	chunker := chunk.New(&allLFSPointersSender{stream: stream, lfsPointers: nil})

	r := bufio.NewReader(catBatch)

	for {
		_, err := r.Peek(1)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		info, err := catfile.ParseObjectInfo(r)
		if err != nil {
			return err
		}

		b, err := ioutil.ReadAll(io.LimitReader(r, info.Size))
		if err != nil {
			return err
		}
		delim, err := r.ReadByte()
		if err != nil {
			return err
		}
		if delim != '\n' {
			return fmt.Errorf("unexpected character %x", delim)
		}

		if !git.IsLFSPointer(b) {
			continue
		}

		if err := chunker.Send(&gitalypb.LFSPointer{
			Oid:  info.Oid,
			Size: info.Size,
			Data: b,
		}); err != nil {
			return err
		}
	}

	for i := len(commands) - 1; i >= 0; i-- {
		if err := commands[i].Wait(); err != nil {
			return err
		}
	}

	if err := chunker.Flush(); err != nil {
		return err
	}

	return nil
}

func getAllLFSPointersAwk2(repository *gitalypb.Repository, stream gitalypb.BlobService_GetAllLFSPointersServer) error {
	var commands []*command.Command

	repoPath, err := helper.GetRepoPath(repository)
	if err != nil {
		return err
	}

	ctx := stream.Context()

	cmd := exec.Command("/bin/sh")
	cmd.Dir = repoPath
	stdin := strings.NewReader(`
set -e
git rev-list --all --in-commit-order --objects --use-bitmap-index |\
  awk '{ print $1 }' |\
  git cat-file --batch-check --buffer |\
  awk '$2 == "blob" && $3 >= 100 && $3 <= 200 { print $1 }' |\
  git cat-file --batch --buffer
`)
	sh, err := command.New(ctx, cmd, stdin, nil, nil)
	if err != nil {
		return err
	}
	commands = append(commands, sh)

	chunker := chunk.New(&allLFSPointersSender{stream: stream, lfsPointers: nil})

	r := bufio.NewReader(sh)

	for {
		_, err := r.Peek(1)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		info, err := catfile.ParseObjectInfo(r)
		if err != nil {
			return err
		}

		b, err := ioutil.ReadAll(io.LimitReader(r, info.Size))
		if err != nil {
			return err
		}
		delim, err := r.ReadByte()
		if err != nil {
			return err
		}
		if delim != '\n' {
			return fmt.Errorf("unexpected character %x", delim)
		}

		if !git.IsLFSPointer(b) {
			continue
		}

		if err := chunker.Send(&gitalypb.LFSPointer{
			Oid:  info.Oid,
			Size: info.Size,
			Data: b,
		}); err != nil {
			return err
		}
	}

	for i := len(commands) - 1; i >= 0; i-- {
		if err := commands[i].Wait(); err != nil {
			return err
		}
	}

	if err := chunker.Flush(); err != nil {
		return err
	}

	return nil
}

type waitReader interface {
	io.Reader
	Wait() error
}

type pseudoCommand struct {
	r        io.Reader
	waitDone chan struct{}
	waitErr  error
}

func newPseudoCommand(in io.Reader, streamer func(io.Writer, io.Reader) error) *pseudoCommand {
	pr, pw := io.Pipe()
	psc := &pseudoCommand{
		r:        pr,
		waitDone: make(chan struct{}),
	}

	go func() {
		psc.waitErr = streamer(pw, in)
		pw.Close()
		close(psc.waitDone)
	}()

	return psc
}

func (psc *pseudoCommand) Read(p []byte) (int, error) { return psc.r.Read(p) }

func (psc *pseudoCommand) Wait() error {
	<-psc.waitDone
	return psc.waitErr
}

func getAllLFSPointersAwk3(repository *gitalypb.Repository, stream gitalypb.BlobService_GetAllLFSPointersServer) error {
	var commands []waitReader
	ctx := stream.Context()

	args := []string{"rev-list", "--all", "--in-commit-order", "--objects", "--use-bitmap-index"}
	revList, err := git.Command(ctx, repository, args...)
	if err != nil {
		return err
	}

	repoPath, err := helper.GetRepoPath(repository)
	if err != nil {
		return err
	}

	commands = append(commands, revList)

	awk1 := newPseudoCommand(revList, func(_w io.Writer, r io.Reader) error {
		w := bufio.NewWriter(_w)
		scanner := bufio.NewScanner(r)
		for scanner.Scan() {
			split := strings.SplitN(scanner.Text(), " ", 2)
			if len(split) == 0 {
				return fmt.Errorf("awk1: empty line")
			}
			if _, err := fmt.Fprintln(w,split[0]); err != nil {
				return err
			}
		}
		if err := scanner.Err(); err != nil {
			return err
		}

		return w.Flush()
	})
	commands = append(commands, awk1)

	catCheck, err := command.New(ctx, exec.Command("git", "-C", repoPath, "cat-file", "--batch-check", "--buffer"), awk1, nil, nil)
	if err != nil {
		return err
	}
	commands = append(commands, catCheck)

	awk2 := newPseudoCommand(catCheck, func(_w io.Writer, _r io.Reader) error {
		r := bufio.NewReader(_r)
		w := bufio.NewWriter(_w)

		for {
			_, err := r.Peek(1)
			if err == io.EOF {
				break
			}
			if err != nil {
				return err
			}

			info, err := catfile.ParseObjectInfo(r)
			if err != nil {
				return err
			}

			if info.Type != "blob" || info.Size < LfsPointerMinSize || info.Size > LfsPointerMaxSize {
				continue
			}

			if _, err := fmt.Fprintln(w, info.Oid); err != nil {
				return err
			}
		}

		return w.Flush()
	})

	commands = append(commands, awk2)
	catBatch, err := command.New(ctx, exec.Command("git", "-C", repoPath, "cat-file", "--batch", "--buffer"), awk2, nil, nil)
	if err != nil {
		return err
	}
	commands = append(commands, catBatch)

	chunker := chunk.New(&allLFSPointersSender{stream: stream, lfsPointers: nil})
	r := bufio.NewReader(catBatch)
	for {
		_, err := r.Peek(1)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		info, err := catfile.ParseObjectInfo(r)
		if err != nil {
			return err
		}

		b, err := ioutil.ReadAll(io.LimitReader(r, info.Size))
		if err != nil {
			return err
		}

		delim, err := r.ReadByte()
		if err != nil {
			return err
		}
		if delim != '\n' {
			return fmt.Errorf("expected newline, got 0x%x", delim)
		}

		if !git.IsLFSPointer(b) {
			continue
		}

		if err := chunker.Send(&gitalypb.LFSPointer{
			Oid:  info.Oid,
			Size: info.Size,
			Data: b,
		}); err != nil {
			return err
		}
	}

	for i := len(commands) - 1; i >= 0; i-- {
		if err := commands[i].Wait(); err != nil {
			return err
		}
	}

	return chunker.Flush()
}

func getAllLFSPointersSmallBlob(repository *gitalypb.Repository, stream gitalypb.BlobService_GetAllLFSPointersServer) error {
	var commands []*command.Command

	repoPath, err := helper.GetRepoPath(repository)
	if err != nil {
		return err
	}

	ctx := stream.Context()

	cmd := exec.Command("/tmp/smallblob/smallblob",repoPath)
	smallblob, err := command.New(ctx, cmd, nil, nil, nil)
	if err != nil {
		return err
	}
	commands = append(commands, smallblob)

	chunker := chunk.New(&allLFSPointersSender{stream: stream, lfsPointers: nil})

	r := bufio.NewReader(smallblob)

	for {
		_, err := r.Peek(1)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		info, err := catfile.ParseObjectInfo(r)
		if err != nil {
			return err
		}

		b, err := ioutil.ReadAll(io.LimitReader(r, info.Size))
		if err != nil {
			return err
		}
		delim, err := r.ReadByte()
		if err != nil {
			return err
		}
		if delim != '\n' {
			return fmt.Errorf("unexpected character %x", delim)
		}

		if !git.IsLFSPointer(b) {
			continue
		}

		if err := chunker.Send(&gitalypb.LFSPointer{
			Oid:  info.Oid,
			Size: info.Size,
			Data: b,
		}); err != nil {
			return err
		}
	}

	for i := len(commands) - 1; i >= 0; i-- {
		if err := commands[i].Wait(); err != nil {
			return err
		}
	}

	if err := chunker.Flush(); err != nil {
		return err
	}

	return nil
}
