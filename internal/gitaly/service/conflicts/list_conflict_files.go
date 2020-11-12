package conflicts

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"unicode/utf8"

	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git2go"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/rubyserver"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/streamio"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *server) listConflictFiles(request *gitalypb.ListConflictFilesRequest, stream gitalypb.ConflictsService_ListConflictFilesServer) error {
	ctx := stream.Context()

	if err := validateListConflictFilesRequest(request); err != nil {
		return helper.ErrInvalidArgument(err)
	}

	repo := git.NewRepository(request.Repository, s.cfg)

	ours, err := repo.ResolveRefish(ctx, request.OurCommitOid+"^{commit}")
	if err != nil {
		return helper.ErrPreconditionFailedf("could not lookup 'our' OID: %s", err)
	}

	theirs, err := repo.ResolveRefish(ctx, request.TheirCommitOid+"^{commit}")
	if err != nil {
		return helper.ErrPreconditionFailedf("could not lookup 'their' OID: %s", err)
	}

	repoPath, err := s.locator.GetPath(request.Repository)
	if err != nil {
		return err
	}

	conflicts, err := git2go.ConflictsCommand{
		Repository: repoPath,
		Ours:       ours,
		Theirs:     theirs,
	}.Run(ctx, s.cfg)
	if err != nil {
		if errors.Is(err, git2go.ErrInvalidArgument) {
			return helper.ErrInvalidArgument(err)
		}
		return helper.ErrInternal(err)
	}

	var conflictFiles []*gitalypb.ConflictFile
	msgSize := 0

	for _, conflict := range conflicts.Conflicts {
		if conflict.Their.Path == "" || conflict.Our.Path == "" {
			return helper.ErrPreconditionFailedf("conflict side missing")
		}

		if !utf8.Valid(conflict.Content) {
			return helper.ErrPreconditionFailed(errors.New("unsupported encoding"))
		}

		conflictFiles = append(conflictFiles, &gitalypb.ConflictFile{
			ConflictFilePayload: &gitalypb.ConflictFile_Header{
				Header: &gitalypb.ConflictFileHeader{
					CommitOid:    request.OurCommitOid,
					TheirPath:    []byte(conflict.Their.Path),
					OurPath:      []byte(conflict.Our.Path),
					AncestorPath: []byte(conflict.Ancestor.Path),
					OurMode:      conflict.Our.Mode,
				},
			},
		})

		contentReader := bytes.NewReader(conflict.Content)
		for {
			chunk := make([]byte, streamio.WriteBufferSize-msgSize)
			bytesRead, err := contentReader.Read(chunk)
			if err != nil && err != io.EOF {
				return helper.ErrInternal(err)
			}

			if bytesRead > 0 {
				conflictFiles = append(conflictFiles, &gitalypb.ConflictFile{
					ConflictFilePayload: &gitalypb.ConflictFile_Content{
						Content: chunk[:bytesRead],
					},
				})
			}

			if err == io.EOF {
				break
			}

			// We don't send a message for each chunk because the content of
			// a file may be smaller than the size limit, which means we can
			// keep adding data to the message
			msgSize += bytesRead
			if msgSize < streamio.WriteBufferSize {
				continue
			}

			if err := stream.Send(&gitalypb.ListConflictFilesResponse{
				Files: conflictFiles,
			}); err != nil {
				return helper.ErrInternal(err)
			}

			conflictFiles = conflictFiles[:0]
			msgSize = 0
		}
	}

	// Send leftover data, if any
	if len(conflictFiles) > 0 {
		if err := stream.Send(&gitalypb.ListConflictFilesResponse{
			Files: conflictFiles,
		}); err != nil {
			return helper.ErrInternal(err)
		}
	}

	return nil
}

func (s *server) ListConflictFiles(in *gitalypb.ListConflictFilesRequest, stream gitalypb.ConflictsService_ListConflictFilesServer) error {
	ctx := stream.Context()

	if featureflag.IsEnabled(ctx, featureflag.GoListConflictFiles) {
		return s.listConflictFiles(in, stream)
	}

	if err := validateListConflictFilesRequest(in); err != nil {
		return status.Errorf(codes.InvalidArgument, "ListConflictFiles: %v", err)
	}

	client, err := s.ruby.ConflictsServiceClient(ctx)
	if err != nil {
		return err
	}

	clientCtx, err := rubyserver.SetHeaders(ctx, s.locator, in.GetRepository())
	if err != nil {
		return err
	}

	rubyStream, err := client.ListConflictFiles(clientCtx, in)
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

func validateListConflictFilesRequest(in *gitalypb.ListConflictFilesRequest) error {
	if in.GetRepository() == nil {
		return fmt.Errorf("empty Repository")
	}
	if in.GetOurCommitOid() == "" {
		return fmt.Errorf("empty OurCommitOid")
	}
	if in.GetTheirCommitOid() == "" {
		return fmt.Errorf("empty TheirCommitOid")
	}

	return nil
}
