package ref

import (
	"bufio"
	"fmt"
	"strings"

	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

func (s *server) ListNewBlobs(in *gitalypb.ListNewBlobsRequest, stream gitalypb.RefService_ListNewBlobsServer) error {
	oid := in.GetCommitId()
	if err := git.ValidateObjectID(oid); err != nil {
		return helper.ErrInvalidArgument(err)
	}

	if err := s.listNewBlobs(in, stream, oid); err != nil {
		return helper.ErrInternal(err)
	}

	return nil
}

func (s *server) listNewBlobs(in *gitalypb.ListNewBlobsRequest, stream gitalypb.RefService_ListNewBlobsServer, oid string) error {
	ctx := stream.Context()
	cmdFlags := []git.Option{git.Flag{Name: "--objects"}, git.Flag{Name: "--not"}, git.Flag{Name: "--all"}}

	if in.GetLimit() > 0 {
		cmdFlags = append(cmdFlags, git.ValueFlag{Name: "--max-count", Value: fmt.Sprint(in.GetLimit())})
	}

	// the added ^ is to negate the oid since there is a --not option that comes earlier in the arg list
	revList, err := s.gitCmdFactory.New(ctx, in.GetRepository(), git.SubCmd{Name: "rev-list", Flags: cmdFlags, Args: []string{"^" + oid}})
	if err != nil {
		return err
	}

	batch, err := catfile.New(ctx, s.gitCmdFactory, in.GetRepository())
	if err != nil {
		return err
	}

	var newBlobs []*gitalypb.NewBlobObject
	scanner := bufio.NewScanner(revList)
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.SplitN(line, " ", 2)

		if len(parts) != 2 {
			continue
		}

		info, err := batch.Info(ctx, git.Revision(parts[0]))
		if err != nil {
			return err
		}

		if !info.IsBlob() {
			continue
		}

		newBlobs = append(newBlobs, &gitalypb.NewBlobObject{
			Oid:  info.Oid.String(),
			Size: info.Size,
			Path: []byte(parts[1]),
		})
		if len(newBlobs) >= 1000 {
			response := &gitalypb.ListNewBlobsResponse{NewBlobObjects: newBlobs}
			if err := stream.Send(response); err != nil {
				return err
			}

			newBlobs = newBlobs[:0]
		}
	}

	response := &gitalypb.ListNewBlobsResponse{NewBlobObjects: newBlobs}
	if err := stream.Send(response); err != nil {
		return err
	}

	return revList.Wait()
}
