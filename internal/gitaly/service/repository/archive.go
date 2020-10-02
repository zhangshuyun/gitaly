package repository

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"

	"gitlab.com/gitlab-org/gitaly/v13/internal/command"
	"gitlab.com/gitlab-org/gitaly/v13/internal/git"
	"gitlab.com/gitlab-org/gitaly/v13/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v13/internal/gitaly/service/commit"
	"gitlab.com/gitlab-org/gitaly/v13/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v13/internal/storage"
	"gitlab.com/gitlab-org/gitaly/v13/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/v13/streamio"
)

func (s *server) GetArchive(in *gitalypb.GetArchiveRequest, stream gitalypb.RepositoryService_GetArchiveServer) error {
	ctx := stream.Context()
	compressCmd, format := parseArchiveFormat(in.GetFormat())

	repoRoot, err := s.locator.GetRepoPath(in.GetRepository())
	if err != nil {
		return err
	}

	path, err := storage.ValidateRelativePath(repoRoot, string(in.GetPath()))
	if err != nil {
		return helper.ErrInvalidArgument(err)
	}

	exclude := make([]string, len(in.GetExclude()))
	for i, ex := range in.GetExclude() {
		exclude[i], err = storage.ValidateRelativePath(repoRoot, string(ex))
		if err != nil {
			return helper.ErrInvalidArgument(err)
		}
	}

	if err := validateGetArchiveRequest(in, format, path); err != nil {
		return err
	}

	if err := validateGetArchivePrecondition(ctx, in, path, exclude); err != nil {
		return err
	}

	if in.GetElidePath() {
		// `git archive <commit ID>:<path>` expects exclusions to be relative to path
		pathSlash := path + string(os.PathSeparator)
		for i := range exclude {
			if !strings.HasPrefix(exclude[i], pathSlash) {
				return helper.ErrInvalidArgumentf("invalid exclude: %q is not a subdirectory of %q", exclude[i], path)
			}

			exclude[i] = exclude[i][len(pathSlash):]
		}
	}

	writer := streamio.NewWriter(func(p []byte) error {
		return stream.Send(&gitalypb.GetArchiveResponse{Data: p})
	})

	return handleArchive(ctx, writer, in, compressCmd, format, path, exclude)
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

func validateGetArchiveRequest(in *gitalypb.GetArchiveRequest, format string, path string) error {
	if err := git.ValidateRevision([]byte(in.GetCommitId())); err != nil {
		return helper.ErrInvalidArgumentf("invalid commitId: %v", err)
	}

	if len(format) == 0 {
		return helper.ErrInvalidArgumentf("invalid format")
	}

	return nil
}

func validateGetArchivePrecondition(ctx context.Context, in *gitalypb.GetArchiveRequest, path string, exclude []string) error {
	c, err := catfile.New(ctx, in.GetRepository())
	if err != nil {
		return err
	}

	f := commit.NewTreeEntryFinder(c)
	if path != "." {
		if ok, err := findGetArchivePath(f, in.GetCommitId(), path); err != nil {
			return err
		} else if !ok {
			return helper.ErrPreconditionFailedf("path doesn't exist")
		}
	}

	for i, exclude := range exclude {
		if ok, err := findGetArchivePath(f, in.GetCommitId(), exclude); err != nil {
			return err
		} else if !ok {
			return helper.ErrPreconditionFailedf("exclude[%d] doesn't exist", i)
		}
	}

	return nil
}

func findGetArchivePath(f *commit.TreeEntryFinder, commitID, path string) (ok bool, err error) {
	treeEntry, err := f.FindByRevisionAndPath(commitID, path)
	if err != nil {
		return false, err
	}

	if treeEntry == nil || len(treeEntry.Oid) == 0 {
		return false, nil
	}
	return true, nil
}

func handleArchive(ctx context.Context, writer io.Writer, in *gitalypb.GetArchiveRequest, compressCmd *exec.Cmd, format string, path string, exclude []string) error {
	var args []string
	pathspecs := make([]string, 0, len(exclude)+1)
	if !in.GetElidePath() {
		// git archive [options] <commit ID> -- <path> [exclude*]
		args = []string{in.GetCommitId()}
		pathspecs = append(pathspecs, path)
	} else if path != "." {
		// git archive [options] <commit ID>:<path> -- [exclude*]
		args = []string{in.GetCommitId() + ":" + path}
	} else {
		// git archive [options] <commit ID> -- [exclude*]
		args = []string{in.GetCommitId()}
	}

	for _, exclude := range exclude {
		pathspecs = append(pathspecs, ":(exclude)"+exclude)
	}

	env := []string{
		fmt.Sprintf("GL_REPOSITORY=%s", in.GetRepository().GetGlRepository()),
		fmt.Sprintf("GL_PROJECT_PATH=%s", in.GetRepository().GetGlProjectPath()),
	}

	archiveCommand, err := git.SafeCmdWithEnv(ctx, env, in.GetRepository(), nil, git.SubCmd{
		Name:        "archive",
		Flags:       []git.Option{git.ValueFlag{"--format", format}, git.ValueFlag{"--prefix", in.GetPrefix() + "/"}},
		Args:        args,
		PostSepArgs: pathspecs,
	})
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
