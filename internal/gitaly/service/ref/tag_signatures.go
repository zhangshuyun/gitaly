package ref

import (
	"errors"
	"fmt"
	"io/ioutil"
	"strings"

	"github.com/golang/protobuf/proto"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gitpipe"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper/chunk"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

func verifyGetTagSignaturesRequest(req *gitalypb.GetTagSignaturesRequest) error {
	if req.GetRepository() == nil {
		return errors.New("empty repository")
	}

	if len(req.GetTagRevisions()) == 0 {
		return errors.New("missing revisions")
	}

	for _, revision := range req.GetTagRevisions() {
		if strings.HasPrefix(revision, "-") && revision != "--all" && revision != "--not" {
			return fmt.Errorf("invalid revision: %q", revision)
		}
	}
	return nil
}

func (s *server) GetTagSignatures(req *gitalypb.GetTagSignaturesRequest, stream gitalypb.RefService_GetTagSignaturesServer) error {
	if err := verifyGetTagSignaturesRequest(req); err != nil {
		return helper.ErrInvalidArgument(err)
	}

	ctx := stream.Context()
	repo := s.localrepo(req.GetRepository())

	catfileProcess, err := s.catfileCache.BatchProcess(ctx, repo)
	if err != nil {
		return helper.ErrInternal(fmt.Errorf("creating catfile process: %w", err))
	}

	chunker := chunk.New(&tagSignatureSender{
		send: func(signatures []*gitalypb.GetTagSignaturesResponse_TagSignature) error {
			return stream.Send(&gitalypb.GetTagSignaturesResponse{
				Signatures: signatures,
			})
		},
	})

	gitVersion, err := git.CurrentVersion(ctx, s.gitCmdFactory)
	if err != nil {
		return helper.ErrInternalf("cannot determine Git version: %v", err)
	}

	revlistOptions := []gitpipe.RevlistOption{
		gitpipe.WithObjects(),
	}

	if gitVersion.SupportsObjectTypeFilter() {
		revlistOptions = append(revlistOptions, gitpipe.WithObjectTypeFilter(gitpipe.ObjectTypeTag))
	}

	revlistIter := gitpipe.Revlist(ctx, repo, req.GetTagRevisions(), revlistOptions...)
	catfileInfoIter := gitpipe.CatfileInfo(ctx, catfileProcess, revlistIter)
	catfileInfoIter = gitpipe.CatfileInfoFilter(ctx, catfileInfoIter, func(r gitpipe.CatfileInfoResult) bool {
		return r.ObjectInfo.Type == "tag"
	})

	catfileObjectIter := gitpipe.CatfileObject(ctx, catfileProcess, catfileInfoIter)

	for catfileObjectIter.Next() {
		tag := catfileObjectIter.Result()

		raw, err := ioutil.ReadAll(tag.ObjectReader)
		if err != nil {
			return helper.ErrInternal(err)
		}

		signatureKey, tagText := catfile.ExtractTagSignature(raw)

		if err := chunker.Send(&gitalypb.GetTagSignaturesResponse_TagSignature{
			TagId:     tag.ObjectInfo.Oid.String(),
			Signature: signatureKey,
			Content:   tagText,
		}); err != nil {
			return helper.ErrInternal(fmt.Errorf("sending tag signature chunk: %w", err))
		}
	}

	if err := catfileObjectIter.Err(); err != nil {
		return helper.ErrInternal(err)
	}

	if err := chunker.Flush(); err != nil {
		return helper.ErrInternal(err)
	}

	return nil
}

type tagSignatureSender struct {
	signatures []*gitalypb.GetTagSignaturesResponse_TagSignature
	send       func([]*gitalypb.GetTagSignaturesResponse_TagSignature) error
}

func (t *tagSignatureSender) Reset() {
	t.signatures = t.signatures[:0]
}

func (t *tagSignatureSender) Append(m proto.Message) {
	t.signatures = append(t.signatures, m.(*gitalypb.GetTagSignaturesResponse_TagSignature))
}

func (t *tagSignatureSender) Send() error {
	return t.send(t.signatures)
}
