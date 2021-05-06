package repository

import (
	"context"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"unicode/utf8"

	"github.com/golang/protobuf/proto"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/internal/git/rawdiff"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/helper/chunk"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

func (s *server) GetRawChanges(req *gitalypb.GetRawChangesRequest, stream gitalypb.RepositoryService_GetRawChangesServer) error {
	ctx := stream.Context()

	repo := req.Repository
	batch, err := s.catfileCache.BatchProcess(stream.Context(), repo)
	if err != nil {
		return helper.ErrInternal(err)
	}

	if err := validateRawChangesRequest(ctx, req, batch); err != nil {
		return helper.ErrInvalidArgument(err)
	}

	if err := s.getRawChanges(stream, repo, batch, req.GetFromRevision(), req.GetToRevision()); err != nil {
		return helper.ErrInternal(err)
	}

	return nil
}

func validateRawChangesRequest(ctx context.Context, req *gitalypb.GetRawChangesRequest, batch catfile.Batch) error {
	if from := req.FromRevision; !git.ObjectID(from).IsZeroOID() {
		if _, err := batch.Info(ctx, git.Revision(from)); err != nil {
			return fmt.Errorf("invalid 'from' revision: %q", from)
		}
	}

	if to := req.ToRevision; !git.ObjectID(to).IsZeroOID() {
		if _, err := batch.Info(ctx, git.Revision(to)); err != nil {
			return fmt.Errorf("invalid 'to' revision: %q", to)
		}
	}

	return nil
}

func (s *server) getRawChanges(stream gitalypb.RepositoryService_GetRawChangesServer, repo *gitalypb.Repository, batch catfile.Batch, from, to string) error {
	if git.ObjectID(to).IsZeroOID() {
		return nil
	}

	if git.ObjectID(from).IsZeroOID() {
		from = git.EmptyTreeOID.String()
	}

	ctx := stream.Context()

	diffCmd, err := s.gitCmdFactory.New(ctx, repo, git.SubCmd{
		Name:  "diff",
		Flags: []git.Option{git.Flag{Name: "--raw"}, git.Flag{Name: "-z"}},
		Args:  []string{from, to},
	})
	if err != nil {
		return fmt.Errorf("start git diff: %v", err)
	}

	p := rawdiff.NewParser(diffCmd)
	chunker := chunk.New(&rawChangesSender{stream: stream})

	for {
		d, err := p.NextDiff()
		if err == io.EOF {
			break // happy path
		}
		if err != nil {
			return fmt.Errorf("read diff: %v", err)
		}

		change, err := changeFromDiff(ctx, batch, d)
		if err != nil {
			return fmt.Errorf("build change from diff line: %v", err)
		}

		if err := chunker.Send(change); err != nil {
			return fmt.Errorf("send response: %v", err)
		}
	}

	if err := diffCmd.Wait(); err != nil {
		return fmt.Errorf("wait git diff: %v", err)
	}

	return chunker.Flush()
}

type rawChangesSender struct {
	stream  gitalypb.RepositoryService_GetRawChangesServer
	changes []*gitalypb.GetRawChangesResponse_RawChange
}

func (s *rawChangesSender) Reset() { s.changes = nil }
func (s *rawChangesSender) Append(m proto.Message) {
	s.changes = append(s.changes, m.(*gitalypb.GetRawChangesResponse_RawChange))
}

func (s *rawChangesSender) Send() error {
	response := &gitalypb.GetRawChangesResponse{RawChanges: s.changes}
	return s.stream.Send(response)
}

// Ordinarily, Git uses 0000000000000000000000000000000000000000, the
// "null SHA", to represent a non-existing object. In the output of `git
// diff --raw` however there are only abbreviated SHA's, i.e. with less
// than 40 characters. Within this context the null SHA is a string that
// consists of 1 to 40 zeroes.
var zeroRegexp = regexp.MustCompile(`\A0+\z`)

const submoduleTreeEntryMode = "160000"

func changeFromDiff(ctx context.Context, batch catfile.Batch, d *rawdiff.Diff) (*gitalypb.GetRawChangesResponse_RawChange, error) {
	resp := &gitalypb.GetRawChangesResponse_RawChange{}

	newMode64, err := strconv.ParseInt(d.DstMode, 8, 32)
	if err != nil {
		return nil, err
	}
	resp.NewMode = int32(newMode64)

	oldMode64, err := strconv.ParseInt(d.SrcMode, 8, 32)
	if err != nil {
		return nil, err
	}
	resp.OldMode = int32(oldMode64)

	if err := setOperationAndPaths(d, resp); err != nil {
		return nil, err
	}

	shortBlobID := d.DstSHA
	blobMode := d.DstMode
	if zeroRegexp.MatchString(shortBlobID) {
		shortBlobID = d.SrcSHA
		blobMode = d.SrcMode
	}

	if blobMode != submoduleTreeEntryMode {
		info, err := batch.Info(ctx, git.Revision(shortBlobID))
		if err != nil {
			return nil, fmt.Errorf("find %q: %v", shortBlobID, err)
		}

		resp.BlobId = info.Oid.String()
		resp.Size = info.Size
	}

	return resp, nil
}

// InvalidUTF8PathPlaceholder is a temporary placeholder that indicates the
const InvalidUTF8PathPlaceholder = "ENCODING ERROR gitaly#1470"

func setOperationAndPaths(d *rawdiff.Diff, resp *gitalypb.GetRawChangesResponse_RawChange) error {
	if len(d.Status) == 0 {
		return fmt.Errorf("empty diff status")
	}

	resp.NewPathBytes = []byte(d.SrcPath)
	resp.OldPathBytes = []byte(d.SrcPath)

	switch d.Status[0] {
	case 'A':
		resp.Operation = gitalypb.GetRawChangesResponse_RawChange_ADDED
		resp.OldPathBytes = nil
	case 'C':
		resp.Operation = gitalypb.GetRawChangesResponse_RawChange_COPIED
		resp.NewPathBytes = []byte(d.DstPath)
	case 'D':
		resp.Operation = gitalypb.GetRawChangesResponse_RawChange_DELETED
		resp.NewPathBytes = nil
	case 'M':
		resp.Operation = gitalypb.GetRawChangesResponse_RawChange_MODIFIED
	case 'R':
		resp.Operation = gitalypb.GetRawChangesResponse_RawChange_RENAMED
		resp.NewPathBytes = []byte(d.DstPath)
	case 'T':
		resp.Operation = gitalypb.GetRawChangesResponse_RawChange_TYPE_CHANGED
	default:
		resp.Operation = gitalypb.GetRawChangesResponse_RawChange_UNKNOWN
	}

	resp.OldPath = string(resp.OldPathBytes)
	resp.NewPath = string(resp.NewPathBytes)

	if !utf8.ValidString(resp.OldPath) {
		resp.OldPath = InvalidUTF8PathPlaceholder
	}
	if !utf8.ValidString(resp.NewPath) {
		resp.NewPath = InvalidUTF8PathPlaceholder
	}

	return nil
}
