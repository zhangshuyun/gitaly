package cleanup

import (
	"fmt"
	"io"

	"github.com/golang/protobuf/proto"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/service/cleanup/internalrefs"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/service/cleanup/notifier"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper/chunk"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/v14/streamio"
)

type bfgStreamReader struct {
	firstRequest *gitalypb.ApplyBfgObjectMapStreamRequest

	server gitalypb.CleanupService_ApplyBfgObjectMapStreamServer
}

type bfgStreamWriter struct {
	entries []*gitalypb.ApplyBfgObjectMapStreamResponse_Entry

	server gitalypb.CleanupService_ApplyBfgObjectMapStreamServer
}

func (s *server) ApplyBfgObjectMapStream(server gitalypb.CleanupService_ApplyBfgObjectMapStreamServer) error {
	firstRequest, err := server.Recv()
	if err != nil {
		return helper.ErrInternal(err)
	}

	if err := validateFirstRequest(firstRequest); err != nil {
		return helper.ErrInvalidArgument(err)
	}

	ctx := server.Context()
	repo := s.localrepo(firstRequest.GetRepository())
	reader := &bfgStreamReader{firstRequest: firstRequest, server: server}
	chunker := chunk.New(&bfgStreamWriter{server: server})

	notifier, err := notifier.New(ctx, s.catfileCache, repo, chunker)
	if err != nil {
		return helper.ErrInternal(err)
	}

	// It doesn't matter if new internal references are added after this RPC
	// starts running - they shouldn't point to the objects removed by the BFG
	cleaner, err := internalrefs.NewCleaner(ctx, s.cfg, repo, notifier.Notify)
	if err != nil {
		return helper.ErrInternal(err)
	}

	if err := cleaner.ApplyObjectMap(ctx, reader.streamReader()); err != nil {
		if invalidErr, ok := err.(internalrefs.ErrInvalidObjectMap); ok {
			return helper.ErrInvalidArgument(invalidErr)
		}

		return helper.ErrInternal(err)
	}

	return helper.ErrInternal(chunker.Flush())
}

func validateFirstRequest(req *gitalypb.ApplyBfgObjectMapStreamRequest) error {
	if repo := req.GetRepository(); repo == nil {
		return fmt.Errorf("first request: repository not set")
	}

	return nil
}

func (r *bfgStreamReader) readOne() ([]byte, error) {
	if r.firstRequest != nil {
		data := r.firstRequest.GetObjectMap()
		r.firstRequest = nil
		return data, nil
	}

	req, err := r.server.Recv()
	if err != nil {
		return nil, err
	}

	return req.GetObjectMap(), nil
}

func (r *bfgStreamReader) streamReader() io.Reader {
	return streamio.NewReader(r.readOne)
}

func (w *bfgStreamWriter) Append(m proto.Message) {
	w.entries = append(
		w.entries,
		m.(*gitalypb.ApplyBfgObjectMapStreamResponse_Entry),
	)
}

func (w *bfgStreamWriter) Reset() {
	w.entries = nil
}

func (w *bfgStreamWriter) Send() error {
	msg := &gitalypb.ApplyBfgObjectMapStreamResponse{Entries: w.entries}

	return w.server.Send(msg)
}
