package remote

import (
	"fmt"

	"gitlab.com/gitlab-org/gitaly/v13/internal/gitaly/rubyserver"
	"gitlab.com/gitlab-org/gitaly/v13/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v13/proto/go/gitalypb"
)

func (s *server) UpdateRemoteMirror(stream gitalypb.RemoteService_UpdateRemoteMirrorServer) error {
	firstRequest, err := stream.Recv()
	if err != nil {
		return helper.ErrInternalf("receive first request: %v", err)
	}

	if err = validateUpdateRemoteMirrorRequest(firstRequest); err != nil {
		return helper.ErrInvalidArgument(err)
	}

	if err := s.updateRemoteMirror(stream, firstRequest); err != nil {
		return helper.ErrInternal(err)
	}

	return nil
}

// updateRemoteMirror has lots of decorated errors to help us debug
// https://gitlab.com/gitlab-org/gitaly/issues/2156.
func (s *server) updateRemoteMirror(stream gitalypb.RemoteService_UpdateRemoteMirrorServer, firstRequest *gitalypb.UpdateRemoteMirrorRequest) error {
	ctx := stream.Context()
	client, err := s.ruby.RemoteServiceClient(ctx)
	if err != nil {
		return fmt.Errorf("get stub: %v", err)
	}

	clientCtx, err := rubyserver.SetHeaders(ctx, firstRequest.GetRepository())
	if err != nil {
		return fmt.Errorf("set headers: %v", err)
	}

	rubyStream, err := client.UpdateRemoteMirror(clientCtx)
	if err != nil {
		return fmt.Errorf("create client: %v", err)
	}

	if err := rubyStream.Send(firstRequest); err != nil {
		return fmt.Errorf("first request to gitaly-ruby: %v", err)
	}

	err = rubyserver.Proxy(func() error {
		// Do not wrap errors in this callback: we must faithfully relay io.EOF
		request, err := stream.Recv()
		if err != nil {
			return err
		}

		return rubyStream.Send(request)
	})
	if err != nil {
		return fmt.Errorf("proxy request to gitaly-ruby: %v", err)
	}

	response, err := rubyStream.CloseAndRecv()
	if err != nil {
		return fmt.Errorf("close stream to gitaly-ruby: %v", err)
	}

	if err := stream.SendAndClose(response); err != nil {
		return fmt.Errorf("close stream to client: %v", err)
	}

	return nil
}

func validateUpdateRemoteMirrorRequest(req *gitalypb.UpdateRemoteMirrorRequest) error {
	if req.GetRepository() == nil {
		return fmt.Errorf("empty Repository")
	}
	if req.GetRefName() == "" {
		return fmt.Errorf("empty RefName")
	}

	return nil
}
