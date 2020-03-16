package commit

import (
	"io"

	"gitlab.com/gitlab-org/gitaly/internal/errors"
	"gitlab.com/gitlab-org/gitaly/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/internal/git/log"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

func (s *server) FilterShasWithSignatures(bidi gitalypb.CommitService_FilterShasWithSignaturesServer) error {
	firstRequest, err := bidi.Recv()
	if err != nil {
		return err
	}

	if err = validateFirstFilterShasWithSignaturesRequest(firstRequest); err != nil {
		return helper.ErrInvalidArgument(err)
	}

	if err := s.filterShasWithSignatures(bidi, firstRequest); err != nil {
		return helper.ErrInternal(err)
	}
	return nil
}

func validateFirstFilterShasWithSignaturesRequest(in *gitalypb.FilterShasWithSignaturesRequest) error {
	if in.Repository == nil {
		return errors.ErrEmptyRepository
	}
	return nil
}

func (s *server) filterShasWithSignatures(bidi gitalypb.CommitService_FilterShasWithSignaturesServer, firstRequest *gitalypb.FilterShasWithSignaturesRequest) error {
	ctx := bidi.Context()
	c, err := catfile.New(ctx, firstRequest.GetRepository())
	if err != nil {
		return err
	}

	var request = firstRequest
	for {
		shas, err := filterCommitShasWithSignatures(c, request.GetShas())
		if err != nil {
			return err
		}

		if err := bidi.Send(&gitalypb.FilterShasWithSignaturesResponse{Shas: shas}); err != nil {
			return err
		}

		request, err = bidi.Recv()
		if err == io.EOF {
			return nil
		}

		if err != nil {
			return err
		}
	}
}

func filterCommitShasWithSignatures(c *catfile.Batch, shas [][]byte) ([][]byte, error) {
	var foundShas [][]byte
	for _, sha := range shas {
		commit, err := log.GetCommitCatfile(c, string(sha))
		if catfile.IsNotFound(err) {
			continue
		}

		if err != nil {
			return nil, err
		}

		if commit.SignatureType == gitalypb.SignatureType_NONE {
			continue
		}

		foundShas = append(foundShas, sha)
	}

	return foundShas, nil
}
