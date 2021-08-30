package remote

import (
	"context"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"

	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

const (
	// pushBatchSize is the maximum number of branches to push in a single push call.
	pushBatchSize = 10
	// maxDivergentRefs is the maximum number of divergent refs to return in UpdateRemoteMirror's
	// response.
	maxDivergentRefs = 100
)

func (s *server) UpdateRemoteMirror(stream gitalypb.RemoteService_UpdateRemoteMirrorServer) error {
	firstRequest, err := stream.Recv()
	if err != nil {
		return helper.ErrInternalf("receive first request: %v", err)
	}

	if err = validateUpdateRemoteMirrorRequest(stream.Context(), firstRequest); err != nil {
		return helper.ErrInvalidArgument(err)
	}

	if err := s.updateRemoteMirror(stream, firstRequest); err != nil {
		return helper.ErrInternal(err)
	}

	return nil
}

func (s *server) updateRemoteMirror(stream gitalypb.RemoteService_UpdateRemoteMirrorServer, firstRequest *gitalypb.UpdateRemoteMirrorRequest) error {
	ctx := stream.Context()

	branchMatchers := firstRequest.GetOnlyBranchesMatching()
	for {
		req, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}

			return fmt.Errorf("receive: %w", err)
		}

		branchMatchers = append(branchMatchers, req.GetOnlyBranchesMatching()...)
	}

	referenceMatcher, err := newReferenceMatcher(branchMatchers)
	if err != nil {
		return fmt.Errorf("create reference matcher: %w", err)
	}

	repo := s.localrepo(firstRequest.GetRepository())
	remote := firstRequest.GetRemote()

	remoteSuffix, err := text.RandomHex(8)
	if err != nil {
		return fmt.Errorf("generating remote suffix: %w", err)
	}
	remoteName := "inmemory-" + remoteSuffix

	remoteConfig := []git.ConfigPair{
		{Key: fmt.Sprintf("remote.%s.url", remoteName), Value: remote.GetUrl()},
	}

	if authHeader := remote.GetHttpAuthorizationHeader(); authHeader != "" {
		remoteConfig = append(remoteConfig, git.ConfigPair{
			Key:   fmt.Sprintf("http.%s.extraHeader", remote.GetUrl()),
			Value: "Authorization: " + authHeader,
		})
	}

	sshCommand, clean, err := git.BuildSSHInvocation(ctx, firstRequest.GetSshKey(), firstRequest.GetKnownHosts())
	if err != nil {
		return fmt.Errorf("build ssh invocation: %w", err)
	}
	defer clean()

	remoteRefsSlice, err := repo.GetRemoteReferences(ctx, remoteName,
		localrepo.WithPatterns("refs/heads/*", "refs/tags/*"),
		localrepo.WithConfig(remoteConfig...),
		localrepo.WithSSHCommand(sshCommand),
	)
	if err != nil {
		return fmt.Errorf("get remote references: %w", err)
	}

	localRefs, err := repo.GetReferences(ctx, "refs/heads/", "refs/tags/")
	if err != nil {
		return fmt.Errorf("get local references: %w", err)
	}

	defaultBranch, err := repo.GetDefaultBranch(ctx, nil)
	if err != nil {
		return fmt.Errorf("get default branch: %w", err)
	}

	remoteRefs := make(map[git.ReferenceName]string, len(remoteRefsSlice))
	for _, ref := range remoteRefsSlice {
		if ref.IsSymbolic {
			// There should be no symbolic refs in refs/heads/ or refs/tags, so we'll just ignore
			// them if something has placed one there.
			continue
		}

		remoteRefs[ref.Name] = ref.Target
	}

	var divergentRefs [][]byte
	toUpdate := map[git.ReferenceName]string{}
	for _, localRef := range localRefs {
		if localRef.IsSymbolic {
			continue
		}

		remoteTarget, ok := remoteRefs[localRef.Name]
		if !ok {
			// ref does not exist on the mirror, it should be created
			toUpdate[localRef.Name] = localRef.Target
			delete(remoteRefs, localRef.Name)
			continue
		}

		if remoteTarget == localRef.Target {
			// ref is up to date on the mirror
			delete(remoteRefs, localRef.Name)
			continue
		}

		if firstRequest.GetKeepDivergentRefs() {
			isAncestor, err := repo.IsAncestor(ctx, git.Revision(remoteTarget), git.Revision(localRef.Target))
			if err != nil && !errors.Is(err, localrepo.InvalidCommitError(remoteTarget)) {
				return fmt.Errorf("is ancestor: %w", err)
			}

			if !isAncestor {
				// The mirror's reference has diverged from the local ref, or the mirror contains a commit
				// which is not present in the local repository.
				if referenceMatcher.MatchString(localRef.Name.String()) && len(divergentRefs) < maxDivergentRefs {
					// diverged branches on the mirror are only included in the response if they match
					// one of the branches in the selector
					divergentRefs = append(divergentRefs, []byte(localRef.Name))
				}

				delete(remoteRefs, localRef.Name)
				continue
			}
		}

		// the mirror's ref does not match ours, we should update it.
		toUpdate[localRef.Name] = localRef.Target
		delete(remoteRefs, localRef.Name)
	}

	toDelete := remoteRefs
	if len(defaultBranch) == 0 || firstRequest.GetKeepDivergentRefs() {
		toDelete = map[git.ReferenceName]string{}
	}

	for remoteRef, remoteCommitOID := range toDelete {
		isAncestor, err := repo.IsAncestor(ctx, git.Revision(remoteCommitOID), git.Revision(defaultBranch))
		if err != nil && !errors.Is(err, localrepo.InvalidCommitError(remoteCommitOID)) {
			return fmt.Errorf("is ancestor: %w", err)
		}

		if isAncestor {
			continue
		}

		// The commit in the extra branch in the remote repositoy has not been merged in to the
		// local repository's default branch. Keep it to avoid losing work.
		delete(toDelete, remoteRef)
	}

	var refspecs []string
	for prefix, references := range map[string]map[git.ReferenceName]string{
		"": toUpdate, ":": toDelete,
	} {
		for reference := range references {
			if !referenceMatcher.MatchString(reference.String()) {
				continue
			}

			refspecs = append(refspecs, prefix+reference.String())
			if reference == defaultBranch.Name {
				// The default branch needs to be pushed in the first batch of refspecs as some features
				// depend on it existing in the repository. The default branch may not exist in the repo
				// yet if this is the first mirroring push.
				last := len(refspecs) - 1
				refspecs[0], refspecs[last] = refspecs[last], refspecs[0]
			}
		}
	}

	for len(refspecs) > 0 {
		batch := refspecs
		if len(refspecs) > pushBatchSize {
			batch = refspecs[:pushBatchSize]
		}

		refspecs = refspecs[len(batch):]

		if err := repo.Push(ctx, remoteName, batch, localrepo.PushOptions{
			SSHCommand: sshCommand,
			Force:      !firstRequest.KeepDivergentRefs,
			Config:     remoteConfig,
		}); err != nil {
			return fmt.Errorf("push to mirror: %w", err)
		}
	}

	return stream.SendAndClose(&gitalypb.UpdateRemoteMirrorResponse{DivergentRefs: divergentRefs})
}

// newReferenceMatcher returns a regexp which matches references that should
// be updated in the mirror repository. Tags are always matched successfully.
// branchMatchers optionally contain patterns that are used to match branches.
// The patterns should only include the branch name without the `refs/heads/`
// prefix. "*" can be used as a wilcard in the patterns. If no branchMatchers
// are specified, all branches are matched successfully.
func newReferenceMatcher(branchMatchers [][]byte) (*regexp.Regexp, error) {
	sb := &strings.Builder{}
	sb.WriteString("^refs/tags/.+$|^refs/heads/(")

	for i, expression := range branchMatchers {
		segments := strings.Split(string(expression), "*")
		for i := range segments {
			segments[i] = regexp.QuoteMeta(segments[i])
		}

		sb.WriteString(strings.Join(segments, ".*"))

		if i < len(branchMatchers)-1 {
			sb.WriteString("|")
		}
	}

	if len(branchMatchers) == 0 {
		sb.WriteString(".+")
	}

	sb.WriteString(")$")

	return regexp.Compile(sb.String())
}

func validateUpdateRemoteMirrorRequest(ctx context.Context, req *gitalypb.UpdateRemoteMirrorRequest) error {
	if req.GetRepository() == nil {
		return fmt.Errorf("empty Repository")
	}

	if req.GetRemote() == nil {
		return fmt.Errorf("missing Remote")
	}

	if req.GetRemote().GetUrl() == "" {
		return fmt.Errorf("remote is missing URL")
	}

	return nil
}
