package conflicts

import (
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"gitlab.com/gitlab-org/gitaly-proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/internal/rubyserver"
)

func (s *server) ResolveConflicts(stream gitalypb.ConflictsService_ResolveConflictsServer) error {
	firstRequest, err := stream.Recv()
	if err != nil {
		return err
	}

	header := firstRequest.GetHeader()
	if header == nil {
		return status.Errorf(codes.InvalidArgument, "ResolveConflicts: empty ResolveConflictsRequestHeader")
	}

	if err = validateResolveConflictsHeader(header); err != nil {
		return status.Errorf(codes.InvalidArgument, "ResolveConflicts: %v", err)
	}

	ctx := stream.Context()
	client, err := s.ConflictsServiceClient(ctx)
	if err != nil {
		return err
	}

	clientCtx, err := rubyserver.SetHeaders(ctx, header.GetRepository())
	if err != nil {
		return err
	}

	rubyStream, err := client.ResolveConflicts(clientCtx)
	if err != nil {
		return err
	}

	if err := rubyStream.Send(firstRequest); err != nil {
		return err
	}

	err = rubyserver.Proxy(func() error {
		request, err := stream.Recv()
		if err != nil {
			return err
		}
		return rubyStream.Send(request)
	})

	if err != nil {
		return err
	}

	response, err := rubyStream.CloseAndRecv()
	if err != nil {
		return err
	}

	return stream.SendAndClose(response)
}

func validateResolveConflictsHeader(header *gitalypb.ResolveConflictsRequestHeader) error {
	if header.GetOurCommitOid() == "" {
		return fmt.Errorf("empty OurCommitOid")
	}
	if header.GetTargetRepository() == nil {
		return fmt.Errorf("empty TargetRepository")
	}
	if header.GetTheirCommitOid() == "" {
		return fmt.Errorf("empty TheirCommitOid")
	}
	if header.GetSourceBranch() == nil {
		return fmt.Errorf("empty SourceBranch")
	}
	if header.GetTargetBranch() == nil {
		return fmt.Errorf("empty TargetBranch")
	}
	if header.GetCommitMessage() == nil {
		return fmt.Errorf("empty CommitMessage")
	}
	if header.GetUser() == nil {
		return fmt.Errorf("empty User")
	}

	return nil
}
