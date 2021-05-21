package conflicts

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"unicode/utf8"

	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git2go"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/streamio"
)

func (s *server) ListConflictFiles(request *gitalypb.ListConflictFilesRequest, stream gitalypb.ConflictsService_ListConflictFilesServer) error {
	ctx := stream.Context()

	if err := validateListConflictFilesRequest(request); err != nil {
		return helper.ErrInvalidArgument(err)
	}

	repo := s.localrepo(request.GetRepository())

	ours, err := repo.ResolveRevision(ctx, git.Revision(request.OurCommitOid+"^{commit}"))
	if err != nil {
		return helper.ErrPreconditionFailedf("could not lookup 'our' OID: %s", err)
	}

	theirs, err := repo.ResolveRevision(ctx, git.Revision(request.TheirCommitOid+"^{commit}"))
	if err != nil {
		return helper.ErrPreconditionFailedf("could not lookup 'their' OID: %s", err)
	}

	repoPath, err := s.locator.GetPath(request.Repository)
	if err != nil {
		return err
	}

	conflicts, err := git2go.ConflictsCommand{
		Repository: repoPath,
		Ours:       ours.String(),
		Theirs:     theirs.String(),
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
