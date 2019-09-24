package blob

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"gitlab.com/gitlab-org/gitaly/internal/command"
	"gitlab.com/gitlab-org/gitaly/internal/config"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/helper/chunk"
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
	if err := validateGetLfsPointersByRevisionRequest(in); err != nil {
		return helper.ErrInvalidArgument(err)
	}

	getAllLFSPointersRequests.WithLabelValues("go").Inc()

	if err := getAllLFSPointersRubyScript(in.GetRepository(), stream); err != nil {
		return helper.ErrInternal(err)
	}

	return nil
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

func getAllLFSPointersRubyScript(repository *gitalypb.Repository, stream gitalypb.BlobService_GetAllLFSPointersServer) error {
	repoPath, err := helper.GetRepoPath(repository)
	if err != nil {
		return err
	}

	ctx := stream.Context()

	cmd := exec.Command(
		"ruby",
		"--",
		"-",
		config.Config.Git.BinPath,
		repoPath,
		fmt.Sprintf("%d", LfsPointerMinSize),
		fmt.Sprintf("%d", LfsPointerMaxSize),
	)
	cmd.Dir = config.Config.Ruby.Dir
	ruby, err := command.New(ctx, cmd, strings.NewReader(rubyScript), nil, nil, os.Environ()...)
	if err != nil {
		return err
	}

	if err := parseCatfileOut(ruby, stream); err != nil {
		return err
	}

	return ruby.Wait()
}

func parseCatfileOut(_r io.Reader, stream gitalypb.BlobService_GetAllLFSPointersServer) error {
	chunker := chunk.New(&allLFSPointersSender{stream: stream, lfsPointers: nil})

	r := bufio.NewReader(_r)
	buf := &bytes.Buffer{}
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

		buf.Reset()
		_, err = io.CopyN(buf, r, info.Size)
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

		if !git.IsLFSPointer(buf.Bytes()) {
			continue
		}

		b := make([]byte, buf.Len())
		copy(b, buf.Bytes())
		if err := chunker.Send(&gitalypb.LFSPointer{
			Oid:  info.Oid,
			Size: info.Size,
			Data: b,
		}); err != nil {
			return err
		}
	}

	return chunker.Flush()
}

var rubyScript = `

def main(git_bin, git_dir, minSize, maxSize)
  IO.popen(%W[#{git_bin} -C #{git_dir} rev-list --all --filter=blob:limit=#{maxSize+1} --in-commit-order --objects], 'r') do |rev_list|
    # Loading bundler and rugged is slow. Let's do it while we wait for git rev-list.
    require 'bundler/setup'
    require 'rugged'

    # disable encoding conversion: we want to read and write data as-is
    rev_list.binmode
    $stdout.binmode

    repo = Rugged::Repository.new(git_dir)

    rev_list.each_line do |line|
      oid = line.split(' ', 2).first
      abort "bad rev-list line #{line.inspect}" unless oid

      header = repo.read_header(oid)
      next unless header[:len] >= minSize && header[:len] <= maxSize && header[:type] == :blob

      puts "#{oid} blob #{header[:len]}"
      $stdout.write(repo.lookup(oid).content)
      puts # newline separator, just like git cat-file
    end
  end

  abort 'rev-list failed' unless $?.success?
end

main(ARGV[0], ARGV[1], Integer(ARGV[2]), Integer(ARGV[3]))
`
