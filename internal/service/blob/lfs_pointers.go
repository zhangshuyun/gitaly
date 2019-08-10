package blob

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"regexp"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
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

func (s *allLFSPointersSender) Reset() { s.lfsPointers = make([]*gitalypb.LFSPointer, 0) }
func (s *allLFSPointersSender) Append(it chunk.Item) {
	s.lfsPointers = append(s.lfsPointers, it.(*gitalypb.LFSPointer))
}
func (s *allLFSPointersSender) Send() error {
	return s.stream.Send(&gitalypb.GetAllLFSPointersResponse{LfsPointers: s.lfsPointers})
}

func getAllLFSPointers(repository *gitalypb.Repository, stream gitalypb.BlobService_GetAllLFSPointersServer) error {
	args := []string{"rev-list", "--all", fmt.Sprintf("--filter=blob:limit=%d", LfsPointerMaxSize), "--in-commit-order", "--objects"}

	cmd, err := git.Command(stream.Context(), repository, args...)
	if err != nil {
		return err
	}

	chunker := chunk.New(&allLFSPointersSender{stream: stream, lfsPointers: make([]*gitalypb.LFSPointer, 0)})

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

		oid := strings.TrimRight(line[0], " ")
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

		data, ok := parseLFSPointer(r)
		if !ok {
			continue
		}

		chunker.Send(&gitalypb.LFSPointer{
			Oid:  oid,
			Size: info.Size,
			Data: data,
		})
	}

	if err := cmd.Wait(); err != nil {
		return err
	}

	if err := chunker.Flush(); err != nil {
		return err
	}

	return nil
}

var (
	lfsOIDRe  = regexp.MustCompile(`(?m)^oid sha256:[0-9a-f]{64}$`)
	lfsSizeRe = regexp.MustCompile(`(?m)^size [0-9]+$`)
)

func parseLFSPointer(r io.Reader) ([]byte, bool) {
	b, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, false
	}

	// ensure the version exists
	if !bytes.HasPrefix(b, []byte("version https://git-lfs.github.com/spec")) {
		return nil, false
	}

	// ensure the oid exists
	if !lfsOIDRe.Match(b) {
		return nil, false
	}

	// ensure the size exists
	if !lfsSizeRe.Match(b) {
		return nil, false
	}

	return b, true
}
