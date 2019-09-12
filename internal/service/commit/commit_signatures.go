package commit

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"

	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/rubyserver"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/streamio"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var gpgSiganturePrefix = []byte("gpgsig")

func (s *server) GetCommitSignatures(request *gitalypb.GetCommitSignaturesRequest, stream gitalypb.CommitService_GetCommitSignaturesServer) error {
	if err := validateGetCommitSignaturesRequest(request); err != nil {
		return status.Errorf(codes.InvalidArgument, "GetCommitSignatures: %v", err)
	}

	return getCommitSignatures(s, request, stream)
}

func getCommitSignatures(s *server, request *gitalypb.GetCommitSignaturesRequest, stream gitalypb.CommitService_GetCommitSignaturesServer) error {
	ctx := stream.Context()

	c, err := catfile.New(ctx, request.GetRepository())
	if err != nil {
		return helper.ErrInternal(err)
	}

	for _, commitID := range request.CommitIds {
		info, err := c.Info(commitID)

		if err != nil {
			if catfile.IsNotFound(err) {
				continue
			} else {
				return helper.ErrInternal(err)
			}
		}

		if info.Type != "commit" {
			return helper.ErrInternal(fmt.Errorf("%q is not a commit", commitID))
		}

		reader, err := c.Commit(commitID)
		if err != nil {
			return helper.ErrInternal(err)
		}

		signatureKey, commitText, err := extractSignature(reader)
		if err != nil {
			return helper.ErrInternal(err)
		}

		if err = sendResponse(commitID, signatureKey, commitText, stream); err != nil {
			return helper.ErrInternal(err)
		}
	}

	return nil
}

// Gets commit signatures from ruby server
func rubyGetCommitSignatures(s *server, request *gitalypb.GetCommitSignaturesRequest, stream gitalypb.CommitService_GetCommitSignaturesServer) error {
	ctx := stream.Context()

	client, err := s.CommitServiceClient(ctx)
	if err != nil {
		return err
	}

	clientCtx, err := rubyserver.SetHeaders(ctx, request.GetRepository())
	if err != nil {
		return err
	}

	rubyStream, err := client.GetCommitSignatures(clientCtx, request)
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

func extractSignature(reader io.Reader) ([]byte, []byte, error) {
	commitText := []byte{}
	signatureKey := []byte{}
	sawSignature := false
	inSignature := false
	lineBreak := []byte("\n")
	whiteSpace := []byte(" ")
	bufferedReader := bufio.NewReader(reader)

	for {
		line, err := bufferedReader.ReadBytes('\n')

		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, nil, err
		}

		if !sawSignature && !inSignature && bytes.HasPrefix(line, gpgSiganturePrefix) {
			sawSignature, inSignature = true, true
			line = bytes.TrimPrefix(line, gpgSiganturePrefix)
		}

		if inSignature && !bytes.Equal(line, lineBreak) {
			line = bytes.TrimPrefix(line, whiteSpace)
			signatureKey = append(signatureKey, line...)
		} else if inSignature {
			inSignature = false
			commitText = append(commitText, line...)
		} else {
			commitText = append(commitText, line...)
		}
	}

	// Remove last line break from signature
	signatureKey = bytes.TrimSuffix(signatureKey, lineBreak)

	return signatureKey, commitText, nil
}

func sendResponse(commitID string, signatureKey []byte, commitText []byte, stream gitalypb.CommitService_GetCommitSignaturesServer) error {
	if len(signatureKey) <= 0 {
		return nil
	}

	err := stream.Send(&gitalypb.GetCommitSignaturesResponse{
		CommitId:  commitID,
		Signature: signatureKey,
	})
	if err != nil {
		return err
	}

	streamWriter := streamio.NewWriter(func(p []byte) error {
		return stream.Send(&gitalypb.GetCommitSignaturesResponse{SignedText: p})
	})

	msgReader := bytes.NewReader(commitText)

	_, err = io.Copy(streamWriter, msgReader)
	if err != nil {
		return fmt.Errorf("failed to send response: %v", err)
	}

	return nil
}

func validateGetCommitSignaturesRequest(request *gitalypb.GetCommitSignaturesRequest) error {
	if request.GetRepository() == nil {
		return errors.New("empty Repository")
	}

	if len(request.GetCommitIds()) == 0 {
		return errors.New("empty CommitIds")
	}

	// Do not support shorthand or invalid commit SHAs
	for _, commitID := range request.CommitIds {
		if err := git.ValidateCommitID(commitID); err != nil {
			return err
		}
	}

	return nil
}
