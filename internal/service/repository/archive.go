package repository

import (
	"context"
	"io"
	"os/exec"
	"strings"

	"gitlab.com/gitlab-org/gitaly-proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/internal/command"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/service/commit"
	"gitlab.com/gitlab-org/gitaly/streamio"
)

func (s *server) GetArchive(in *gitalypb.GetArchiveRequest, stream gitalypb.RepositoryService_GetArchiveServer) error {
	ctx := stream.Context()
	compressCmd, format := parseArchiveFormat(in.GetFormat())
	path := parsePath(in.GetPath())

	if err := validateGetArchiveRequest(in, format, path); err != nil {
		return err
	}

	if err := validateGetArchivePrecondition(ctx, in, path); err != nil {
		return err
	}

	writer := streamio.NewWriter(func(p []byte) error {
		return stream.Send(&gitalypb.GetArchiveResponse{Data: p})
	})

	return handleArchive(ctx, writer, in, compressCmd, format, path)
}

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

func parsePath(path []byte) string {
	if path == nil {
		return "."
	}

	return string(path)
}

func validateGetArchiveRequest(in *gitalypb.GetArchiveRequest, format string, path string) error {
	if err := git.ValidateRevision([]byte(in.GetCommitId())); err != nil {
		return helper.ErrInvalidArgumentf("invalid commitId: %v", err)
	}

	if len(format) == 0 {
		return helper.ErrInvalidArgumentf("invalid format")
	}

	if helper.ContainsPathTraversal(path) {
		return helper.ErrInvalidArgumentf("path can't contain directory traversal")
	}

	return nil
}

func validateGetArchivePrecondition(ctx context.Context, in *gitalypb.GetArchiveRequest, path string) error {
	if path == "." {
		return nil
	}

	c, err := catfile.New(ctx, in.GetRepository())
	if err != nil {
		return err
	}

	treeEntry, err := commit.NewTreeEntryFinder(c).FindByRevisionAndPath(in.GetCommitId(), strings.TrimRight(path, "/"))
	if err != nil {
		return err
	}

	if treeEntry == nil || len(treeEntry.Oid) == 0 {
		return helper.ErrPreconditionFailedf("path doesn't exist")
	}

	return nil
}

func handleArchive(ctx context.Context, writer io.Writer, in *gitalypb.GetArchiveRequest, compressCmd *exec.Cmd, format string, path string) error {
	archiveCommand, err := git.Command(ctx, in.GetRepository(), "archive",
		"--format="+format, "--prefix="+in.GetPrefix()+"/", in.GetCommitId(), "--", path)

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
