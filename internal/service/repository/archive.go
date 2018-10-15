package repository

import (
	"context"
	"io"
	"os/exec"

	"gitlab.com/gitlab-org/gitaly-proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/internal/command"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/streamio"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func parseArchiveFormat(format gitalypb.GetArchiveRequest_Format) (*exec.Cmd, string) {
	switch format {
	case gitalypb.GetArchiveRequest_TAR:
		return nil, "tar"
	case gitalypb.GetArchiveRequest_TAR_GZ:
		return exec.Command("gzip", "-c", "-n"), "tar"
	case gitalypb.GetArchiveRequest_TAR_BZ2:
		return exec.Command("bzip2", "-c"), "tar"
	case gitalypb.GetArchiveRequest_ZIP:
		return nil, "zip"
	}

	return nil, ""
}

func handleArchive(ctx context.Context, writer io.Writer, repo *gitalypb.Repository,
	format gitalypb.GetArchiveRequest_Format, prefix, commitID string) error {
	compressCmd, formatArg := parseArchiveFormat(format)
	if len(formatArg) == 0 {
		return status.Errorf(codes.InvalidArgument, "invalid format")
	}

	archiveCommand, err := git.Command(ctx, repo, "archive",
		"--format="+formatArg, "--prefix="+prefix+"/", commitID)
	if err != nil {
		return err
	}

	if compressCmd != nil {
		command, err := command.New(ctx, compressCmd, archiveCommand, writer, nil)
		if err != nil {
			return err
		}

		if err := command.Wait(); err != nil {
			return err
		}
	} else if _, err = io.Copy(writer, archiveCommand); err != nil {
		return err
	}

	return archiveCommand.Wait()
}

func (s *server) GetArchive(in *gitalypb.GetArchiveRequest, stream gitalypb.RepositoryService_GetArchiveServer) error {
	if err := git.ValidateRevision([]byte(in.CommitId)); err != nil {
		return status.Errorf(codes.InvalidArgument, "invalid commitId: %v", err)
	}

	writer := streamio.NewWriter(func(p []byte) error {
		return stream.Send(&gitalypb.GetArchiveResponse{Data: p})
	})

	return handleArchive(stream.Context(), writer, in.Repository, in.Format, in.Prefix, in.CommitId)
}
