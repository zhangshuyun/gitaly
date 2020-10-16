package conflicts

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sort"

	"gitlab.com/gitlab-org/gitaly/client"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/conflict"
	"gitlab.com/gitlab-org/gitaly/internal/git2go"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/rubyserver"
	"gitlab.com/gitlab-org/gitaly/internal/gitalyssh"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"gopkg.in/errgo.v2/errors"
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

	if featureflag.IsEnabled(stream.Context(), featureflag.GoResolveConflicts) {
		return s.resolveConflicts(header, stream)
	}

	ctx := stream.Context()
	client, err := s.ruby.ConflictsServiceClient(ctx)
	if err != nil {
		return err
	}

	clientCtx, err := rubyserver.SetHeaders(ctx, header.GetRepository())
	if err != nil {
		return err
	}

	rubyStream, err := client.ResolveConflicts(clientCtx)
	if err != nil {
		return err
	}

	if err := rubyStream.Send(firstRequest); err != nil {
		return err
	}

	err = rubyserver.Proxy(func() error {
		request, err := stream.Recv()
		if err != nil {
			return err
		}
		return rubyStream.Send(request)
	})

	if err != nil {
		return err
	}

	response, err := rubyStream.CloseAndRecv()
	if err != nil {
		return err
	}

	return stream.SendAndClose(response)
}

func validateResolveConflictsHeader(header *gitalypb.ResolveConflictsRequestHeader) error {
	if header.GetOurCommitOid() == "" {
		return fmt.Errorf("empty OurCommitOid")
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
		switch err {
		case io.EOF:
			return nil
		case nil:
		// do nothing, continue
		default:
			return err
		}

		if _, err := b.Write(req.GetFilesJson()); err != nil {
			return err
		}
	}

	var resolutions []conflict.Resolution
	if err := json.NewDecoder(b).Decode(&resolutions); err != nil {
		return err
	}

	if err := s.repoWithBranchCommit(
		stream.Context(),
		header.GetRepository(),
		header.GetTargetRepository(),
		header.SourceBranch,
		header.TargetBranch,
	); err != nil {
		return err
	}

	repoPath, err := s.locator.GetRepoPath(header.GetRepository())
	if err != nil {
		return err
	}

	_, err = git2go.ResolveCommand{
		MergeCommand: git2go.MergeCommand{
			Repository: repoPath,
			AuthorName: string(header.User.Name),
			AuthorMail: string(header.User.Email),
			Message:    string(header.CommitMessage),
			Ours:       header.GetOurCommitOid(),
			Theirs:     header.GetTheirCommitOid(),
		},
		Resolutions: resolutions,
	}.Run(stream.Context(), s.cfg)
	if err != nil {
		if errors.Is(git2go.ErrInvalidArgument)(err) {
			return helper.ErrInvalidArgument(err)
		}
		return err
	}

	return stream.SendAndClose(&gitalypb.ResolveConflictsResponse{
		ResolutionError: err.Error(),
	})
}

func sameRepo(left, right *gitalypb.Repository) bool {
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

const gitalyInternalURL = "ssh://gitaly/internal.git"

// repoWithCommit ensures that the source repo contains the same commit we
// hope to merge with from the target branch, else it will be fetched from the
// target repo. This is necessary since all merge/resolve logic occurs on the
// same filesystem
func (s *server) repoWithBranchCommit(ctx context.Context, srcRepo, targetRepo *gitalypb.Repository, srcBranch, targetBranch []byte) error {
	src := git.NewRepository(srcRepo)
	if sameRepo(srcRepo, targetRepo) {
		_, err := src.ResolveRefish(ctx, string(targetBranch))
		return err
	}

	target, err := git.NewRemoteRepository(ctx, targetRepo, client.NewPool())
	if err != nil {
		return err
	}

	oid, err := target.ResolveRefish(ctx, string(targetBranch))
	if err != nil {
		return err
	}

	ok, err := src.ContainsRef(ctx, oid)
	if err != nil {
		return err
	}
	if ok {
		// target branch commit already exists in source repo; nothing
		// to do
		return err
	}

	env, err := gitalyssh.UploadPackEnv(ctx, &gitalypb.SSHUploadPackRequest{Repository: targetRepo})
	if err != nil {
		return err
	}
	// to enable fetching a specific SHA:
	env = append(env, "uploadpack.allowAnySHA1InWant=true")

	srcRepoPath, err := s.locator.GetRepoPath(srcRepo)
	if err != nil {
		return err
	}

	cmd, err := git.SafeBareCmd(ctx, git.CmdStream{}, env,
		[]git.Option{git.ValueFlag{"--git-dir", srcRepoPath}},
		git.SubCmd{
			Name:  "fetch",
			Flags: []git.Option{git.Flag{Name: "--no-tags"}},
			Args:  []string{gitalyInternalURL, oid},
		},
	)
	if err != nil {
		return err
	}

	if err := cmd.Wait(); err != nil {
		return err
	}

	return nil
}
