package conflicts

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/golang/protobuf/ptypes"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/conflict"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/remoterepo"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/repository"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git2go"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitalyssh"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *server) ResolveConflicts(stream gitalypb.ConflictsService_ResolveConflictsServer) error {
	firstRequest, err := stream.Recv()
	if err != nil {
		return err
	}

	header := firstRequest.GetHeader()
	if header == nil {
		return status.Errorf(codes.InvalidArgument, "ResolveConflicts: empty ResolveConflictsRequestHeader")
	}

	if err = validateResolveConflictsHeader(header); err != nil {
		return status.Errorf(codes.InvalidArgument, "ResolveConflicts: %v", err)
	}

	err = s.resolveConflicts(header, stream)
	return handleResolveConflictsErr(err, stream)
}

func handleResolveConflictsErr(err error, stream gitalypb.ConflictsService_ResolveConflictsServer) error {
	var errStr string // normalized error message
	if err != nil {
		errStr = strings.TrimPrefix(err.Error(), "resolve: ") // remove subcommand artifact
		errStr = strings.TrimSpace(errStr)                    // remove newline artifacts

		// only send back resolution errors that match expected pattern
		for _, p := range []string{
			"Missing resolution for section ID:",
			"Resolved content has no changes for file",
			"Missing resolutions for the following files:",
		} {
			if strings.HasPrefix(errStr, p) {
				// log the error since the interceptor won't catch this
				// error due to the unique way the RPC is defined to
				// handle resolution errors
				ctxlogrus.
					Extract(stream.Context()).
					WithError(err).
					Error("ResolveConflicts: unable to resolve conflict")
				return stream.SendAndClose(&gitalypb.ResolveConflictsResponse{
					ResolutionError: errStr,
				})
			}
		}

		return err
	}
	return stream.SendAndClose(&gitalypb.ResolveConflictsResponse{})
}

func validateResolveConflictsHeader(header *gitalypb.ResolveConflictsRequestHeader) error {
	if header.GetOurCommitOid() == "" {
		return fmt.Errorf("empty OurCommitOid")
	}
	if header.GetRepository() == nil {
		return fmt.Errorf("empty Repository")
	}
	if header.GetTargetRepository() == nil {
		return fmt.Errorf("empty TargetRepository")
	}
	if header.GetTheirCommitOid() == "" {
		return fmt.Errorf("empty TheirCommitOid")
	}
	if header.GetSourceBranch() == nil {
		return fmt.Errorf("empty SourceBranch")
	}
	if header.GetTargetBranch() == nil {
		return fmt.Errorf("empty TargetBranch")
	}
	if header.GetCommitMessage() == nil {
		return fmt.Errorf("empty CommitMessage")
	}
	if header.GetUser() == nil {
		return fmt.Errorf("empty User")
	}

	return nil
}

func (s *server) resolveConflicts(header *gitalypb.ResolveConflictsRequestHeader, stream gitalypb.ConflictsService_ResolveConflictsServer) error {
	b := bytes.NewBuffer(nil)
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		if _, err := b.Write(req.GetFilesJson()); err != nil {
			return err
		}
	}

	var checkKeys []map[string]interface{}
	if err := json.Unmarshal(b.Bytes(), &checkKeys); err != nil {
		return err
	}

	for _, ck := range checkKeys {
		_, sectionExists := ck["sections"]
		_, contentExists := ck["content"]
		if !sectionExists && !contentExists {
			return helper.ErrInvalidArgumentf("missing sections or content for a resolution")
		}
	}

	var resolutions []conflict.Resolution
	if err := json.Unmarshal(b.Bytes(), &resolutions); err != nil {
		return err
	}

	ctx := stream.Context()
	sourceRepo := s.localrepo(header.GetRepository())
	targetRepo, err := remoterepo.New(ctx, header.GetTargetRepository(), s.pool)
	if err != nil {
		return err
	}

	if err := s.repoWithBranchCommit(ctx,
		sourceRepo,
		targetRepo,
		header.TargetBranch,
	); err != nil {
		return err
	}

	repoPath, err := s.locator.GetRepoPath(sourceRepo)
	if err != nil {
		return err
	}

	authorDate := time.Now()
	if header.Timestamp != nil {
		authorDate, err = ptypes.Timestamp(header.Timestamp)
		if err != nil {
			return helper.ErrInvalidArgument(err)
		}
	}

	if git.ValidateObjectID(header.GetOurCommitOid()) != nil ||
		git.ValidateObjectID(header.GetTheirCommitOid()) != nil {
		return errors.New("Rugged::InvalidError: unable to parse OID - contains invalid characters")
	}

	result, err := git2go.ResolveCommand{
		MergeCommand: git2go.MergeCommand{
			Repository: repoPath,
			AuthorName: string(header.User.Name),
			AuthorMail: string(header.User.Email),
			AuthorDate: authorDate,
			Message:    string(header.CommitMessage),
			Ours:       header.GetOurCommitOid(),
			Theirs:     header.GetTheirCommitOid(),
		},
		Resolutions: resolutions,
	}.Run(stream.Context(), s.cfg)
	if err != nil {
		if errors.Is(err, git2go.ErrInvalidArgument) {
			return helper.ErrInvalidArgument(err)
		}
		return err
	}

	commitOID, err := git.NewObjectIDFromHex(result.CommitID)
	if err != nil {
		return err
	}

	if featureflag.ResolveConflictsWithHooks.IsEnabled(ctx) {
		if err := s.updater.UpdateReference(
			ctx,
			header.Repository,
			header.User,
			git.ReferenceName("refs/heads/"+string(header.GetSourceBranch())),
			commitOID,
			git.ObjectID(header.OurCommitOid),
		); err != nil {
			return err
		}
	} else {
		if err := sourceRepo.UpdateRef(
			ctx,
			git.ReferenceName("refs/heads/"+string(header.GetSourceBranch())),
			commitOID,
			"",
		); err != nil {
			return err
		}
	}

	return nil
}

func sameRepo(left, right repository.GitRepo) bool {
	lgaod := left.GetGitAlternateObjectDirectories()
	rgaod := right.GetGitAlternateObjectDirectories()
	if len(lgaod) != len(rgaod) {
		return false
	}
	sort.Strings(lgaod)
	sort.Strings(rgaod)
	for i := 0; i < len(lgaod); i++ {
		if lgaod[i] != rgaod[i] {
			return false
		}
	}
	if left.GetGitObjectDirectory() != right.GetGitObjectDirectory() {
		return false
	}
	if left.GetRelativePath() != right.GetRelativePath() {
		return false
	}
	if left.GetStorageName() != right.GetStorageName() {
		return false
	}
	return true
}

// repoWithCommit ensures that the source repo contains the same commit we
// hope to merge with from the target branch, else it will be fetched from the
// target repo. This is necessary since all merge/resolve logic occurs on the
// same filesystem
func (s *server) repoWithBranchCommit(ctx context.Context, sourceRepo *localrepo.Repo, targetRepo *remoterepo.Repo, targetBranch []byte) error {
	const peelCommit = "^{commit}"

	targetRevision := "refs/heads/" + git.Revision(string(targetBranch)) + peelCommit

	if sameRepo(sourceRepo, targetRepo) {
		_, err := sourceRepo.ResolveRevision(ctx, targetRevision)
		return err
	}

	oid, err := targetRepo.ResolveRevision(ctx, targetRevision)
	if err != nil {
		return fmt.Errorf("could not resolve target revision %q: %w", targetRevision, err)
	}

	ok, err := sourceRepo.HasRevision(ctx, git.Revision(oid)+peelCommit)
	if err != nil {
		return err
	}
	if ok {
		// target branch commit already exists in source repo; nothing
		// to do
		return nil
	}

	env, err := gitalyssh.UploadPackEnv(ctx, s.cfg, &gitalypb.SSHUploadPackRequest{
		Repository:       targetRepo.Repository,
		GitConfigOptions: []string{"uploadpack.allowAnySHA1InWant=true"},
	})
	if err != nil {
		return err
	}

	var stderr bytes.Buffer
	if err := sourceRepo.ExecAndWait(ctx,
		git.SubCmd{
			Name:  "fetch",
			Flags: []git.Option{git.Flag{Name: "--no-tags"}},
			Args:  []string{gitalyssh.GitalyInternalURL, oid.String()},
		},
		git.WithStderr(&stderr),
		git.WithEnv(env...),
		git.WithRefTxHook(ctx, sourceRepo, s.cfg),
	); err != nil {
		return fmt.Errorf("could not fetch target commit: %w, stderr: %q", err, stderr.String())
	}

	return nil
}
