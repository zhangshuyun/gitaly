package repository

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"math"
	"regexp"

	"gitlab.com/gitlab-org/gitaly/v14/internal/command"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper/lines"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/v14/streamio"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	surroundContext = "2"

	// searchFilesFilterMaxLength controls the maximum length of the regular
	// expression to thwart excessive resource usage when filtering
	searchFilesFilterMaxLength = 1000
)

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
	cmd, err := s.gitCmdFactory.New(ctx, repo,
		git.SubCmd{Name: "grep", Flags: []git.Option{
			git.Flag{Name: "--ignore-case"},
			git.Flag{Name: "-I"},
			git.Flag{Name: "--line-number"},
			git.Flag{Name: "--null"},
			git.ValueFlag{Name: "--before-context", Value: surroundContext},
			git.ValueFlag{Name: "--after-context", Value: surroundContext},
			git.Flag{Name: "--perl-regexp"},
			git.Flag{Name: "-e"}}, Args: []string{req.GetQuery(), string(req.GetRef())}})

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

	var filter *regexp.Regexp
	if req.GetFilter() != "" {
		if len(req.GetFilter()) > searchFilesFilterMaxLength {
			return helper.ErrInvalidArgumentf("SearchFilesByName: filter exceeds maximum length")
		}
		var err error
		filter, err = regexp.Compile(req.GetFilter())
		if err != nil {
			return helper.ErrInvalidArgumentf("SearchFilesByName: filter did not compile: %v", err)
		}
	}

	repo := req.GetRepository()
	if repo == nil {
		return status.Errorf(codes.InvalidArgument, "SearchFilesByName: empty Repository")
	}

	ctx := stream.Context()
	cmd, err := s.gitCmdFactory.New(
		ctx,
		repo,
		git.SubCmd{Name: "ls-tree", Flags: []git.Option{
			git.Flag{Name: "--full-tree"},
			git.Flag{Name: "--name-status"},
			git.Flag{Name: "-r"}}, Args: []string{string(req.GetRef()), req.GetQuery()}})
	if err != nil {
		return status.Errorf(codes.Internal, "SearchFilesByName: cmd start failed: %v", err)
	}

	lr := func(objs [][]byte) error {
		return stream.Send(&gitalypb.SearchFilesByNameResponse{Files: objs})
	}

	return lines.Send(cmd, lr, lines.SenderOpts{Delimiter: '\n', Limit: math.MaxInt32, Filter: filter})
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
