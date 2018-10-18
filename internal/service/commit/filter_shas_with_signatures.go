package commit

import (
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"gitlab.com/gitlab-org/gitaly-proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/internal/rubyserver"
)

func (s *server) FilterShasWithSignatures(bidi gitalypb.CommitService_FilterShasWithSignaturesServer) error {
	firstRequest, err := bidi.Recv()
	if err != nil {
		return err
	}

	if err = verifyFirstFilterShasWithSignaturesRequest(firstRequest); err != nil {
		return err
	}

	ctx := bidi.Context()
	client, err := s.CommitServiceClient(ctx)
	if err != nil {
		return err
	}

	clientCtx, err := rubyserver.SetHeaders(ctx, firstRequest.GetRepository())
	if err != nil {
		return err
	}

	rubyBidi, err := client.FilterShasWithSignatures(clientCtx)
	if err != nil {
		return err
	}

	if err := rubyBidi.Send(firstRequest); err != nil {
		return err
	}

	return rubyserver.ProxyBidi(
		func() error {
			request, err := bidi.Recv()
			if err != nil {
				return err
			}

			return rubyBidi.Send(request)
		},
		rubyBidi,
		func() error {
			response, err := rubyBidi.Recv()
			if err != nil {
				return err
			}

			return bidi.Send(response)
		},
	)
}

func verifyFirstFilterShasWithSignaturesRequest(in *gitalypb.FilterShasWithSignaturesRequest) error {
	if in.Repository == nil {
		return status.Errorf(codes.InvalidArgument, "no repository given")
	}
	return nil
}
