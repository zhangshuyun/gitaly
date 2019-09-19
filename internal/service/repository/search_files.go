package repository

import (
	"bufio"
	"bytes"
	"errors"
	"io"

	"gitlab.com/gitlab-org/gitaly-proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/internal/command"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/helper/lines"
	"gitlab.com/gitlab-org/gitaly/streamio"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const surroundContext = "2"

var contentDelimiter = []byte("--\n")

func (s *server) SearchFilesByContent(req *gitalypb.SearchFilesByContentRequest, stream gitalypb.RepositoryService_SearchFilesByContentServer) error {
	if err := validateSearchFilesRequest(req); err != nil {
		return helper.DecorateError(codes.InvalidArgument, err)
	}

	repo := req.GetRepository()
	if repo == nil {
		return status.Errorf(codes.InvalidArgument, "SearchFilesByContent: empty Repository")
	}

	ctx := stream.Context()
	cmd, err := git.Command(ctx, repo, "grep",
		"--ignore-case",
		"-I", // Don't match binary, there is no long-name for this one
		"--line-number",
		"--null",
		"--before-context", surroundContext,
		"--after-context", surroundContext,
		"--perl-regexp",
		"-e", // next arg is pattern, keep this last
		req.GetQuery(),
		string(req.GetRef()),
	)

	if err != nil {
		return status.Errorf(codes.Internal, "SearchFilesByContent: cmd start failed: %v", err)
	}

	if err = sendSearchFilesResultChunked(cmd, stream); err != nil {
		return status.Errorf(codes.Internal, "SearchFilesByContent: sending chunked response failed: %v", err)
	}

	return nil
}

func sendMatchInChunks(buf []byte, stream gitalypb.RepositoryService_SearchFilesByContentServer) error {
	sw := streamio.NewWriter(func(p []byte) error {
		return stream.Send(&gitalypb.SearchFilesByContentResponse{MatchData: p})
	})

	if _, err := io.Copy(sw, bytes.NewReader(buf)); err != nil {
		return err
	}

	return stream.Send(&gitalypb.SearchFilesByContentResponse{EndOfMatch: true})
}

func sendSearchFilesResultChunked(cmd *command.Command, stream gitalypb.RepositoryService_SearchFilesByContentServer) error {
	var buf []byte
	scanner := bufio.NewScanner(cmd)

	for scanner.Scan() {
		// Intentionally avoid scanner.Bytes() because that returns a []byte that
		// becomes invalid on the next loop iteration, and we want to hold on to
		// the contents of the current line for a while. Scanner.Text() is a
		// string and hence immutable.
		line := scanner.Text() + "\n"

		if line == string(contentDelimiter) {
			if err := sendMatchInChunks(buf, stream); err != nil {
				return err
			}

			buf = nil
			continue
		}

		buf = append(buf, line...)
	}

	if len(buf) > 0 {
		return sendMatchInChunks(buf, stream)
	}

	return nil
}

func (s *server) SearchFilesByName(req *gitalypb.SearchFilesByNameRequest, stream gitalypb.RepositoryService_SearchFilesByNameServer) error {
	if err := validateSearchFilesRequest(req); err != nil {
		return helper.DecorateError(codes.InvalidArgument, err)
	}

	repo := req.GetRepository()
	if repo == nil {
		return status.Errorf(codes.InvalidArgument, "SearchFilesByName: empty Repository")
	}

	ctx := stream.Context()
	cmd, err := git.Command(ctx, repo, "ls-tree", "--full-tree", "--name-status", "-r", string(req.GetRef()), req.GetQuery())
	if err != nil {
		return status.Errorf(codes.Internal, "SearchFilesByName: cmd start failed: %v", err)
	}

	lr := func(objs [][]byte) error {
		return stream.Send(&gitalypb.SearchFilesByNameResponse{Files: objs})
	}

	return lines.Send(cmd, lr, []byte{'\n'})
}

type searchFilesRequest interface {
	GetRef() []byte
	GetQuery() string
}

func validateSearchFilesRequest(req searchFilesRequest) error {
	if len(req.GetQuery()) == 0 {
		return errors.New("no query given")
	}

	if len(req.GetRef()) == 0 {
		return errors.New("no ref given")
	}

	if bytes.HasPrefix(req.GetRef(), []byte("-")) {
		return errors.New("invalid ref argument")
	}

	return nil
}
