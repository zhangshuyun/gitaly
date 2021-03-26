package remote

import (
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"

	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/rubyserver"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

// PushBatchSize is the maximum number of branches to push in a single push call.
const PushBatchSize = 10

func (s *server) UpdateRemoteMirror(stream gitalypb.RemoteService_UpdateRemoteMirrorServer) error {
	firstRequest, err := stream.Recv()
	if err != nil {
		return helper.ErrInternalf("receive first request: %v", err)
	}

	if err = validateUpdateRemoteMirrorRequest(firstRequest); err != nil {
		return helper.ErrInvalidArgument(err)
	}

	if featureflag.IsEnabled(stream.Context(), featureflag.GoUpdateRemoteMirror) {
		if err := s.goUpdateRemoteMirror(stream, firstRequest); err != nil {
			return helper.ErrInternal(err)
		}

		return nil
	}

	if err := s.updateRemoteMirror(stream, firstRequest); err != nil {
		return helper.ErrInternal(err)
	}

	return nil
}

// updateRemoteMirror has lots of decorated errors to help us debug
// https://gitlab.com/gitlab-org/gitaly/issues/2156.
func (s *server) updateRemoteMirror(stream gitalypb.RemoteService_UpdateRemoteMirrorServer, firstRequest *gitalypb.UpdateRemoteMirrorRequest) error {
	ctx := stream.Context()
	client, err := s.ruby.RemoteServiceClient(ctx)
	if err != nil {
		return fmt.Errorf("get stub: %v", err)
	}

	clientCtx, err := rubyserver.SetHeaders(ctx, s.locator, firstRequest.GetRepository())
	if err != nil {
		return fmt.Errorf("set headers: %v", err)
	}

	rubyStream, err := client.UpdateRemoteMirror(clientCtx)
	if err != nil {
		return fmt.Errorf("create client: %v", err)
	}

	if err := rubyStream.Send(firstRequest); err != nil {
		return fmt.Errorf("first request to gitaly-ruby: %v", err)
	}

	err = rubyserver.Proxy(func() error {
		// Do not wrap errors in this callback: we must faithfully relay io.EOF
		request, err := stream.Recv()
		if err != nil {
			return err
		}

		return rubyStream.Send(request)
	})
	if err != nil {
		return fmt.Errorf("proxy request to gitaly-ruby: %v", err)
	}

	response, err := rubyStream.CloseAndRecv()
	if err != nil {
		return fmt.Errorf("close stream to gitaly-ruby: %v", err)
	}

	if err := stream.SendAndClose(response); err != nil {
		return fmt.Errorf("close stream to client: %v", err)
	}

	return nil
}

func (s *server) goUpdateRemoteMirror(stream gitalypb.RemoteService_UpdateRemoteMirrorServer, firstRequest *gitalypb.UpdateRemoteMirrorRequest) error {
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

	repo := localrepo.New(s.gitCmdFactory, firstRequest.GetRepository(), s.cfg)
	remoteRefsSlice, err := repo.GetRemoteReferences(ctx, firstRequest.GetRefName(), "refs/heads/*", "refs/tags/*")
	if err != nil {
		return fmt.Errorf("get remote references: %w", err)
	}

	localRefs, err := repo.GetReferences(ctx, "refs/heads/", "refs/tags/")
	if err != nil {
		return fmt.Errorf("get local references: %w", err)
	}

	if len(localRefs) == 0 {
		// https://gitlab.com/gitlab-org/gitaly/-/issues/3503
		return errors.New("close stream to gitaly-ruby: rpc error: code = Unknown desc = NoMethodError: undefined method `id' for nil:NilClass")
	}

	remoteRefs := make(map[git.ReferenceName]string, len(remoteRefsSlice))
	for _, ref := range remoteRefsSlice {
		remoteRefs[ref.Name] = ref.Target
	}

	var divergentRefs [][]byte
	toUpdate := map[git.ReferenceName]string{}
	for _, localRef := range localRefs {
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
				divergentRefs = append(divergentRefs, []byte(localRef.Name))
				delete(remoteRefs, localRef.Name)
				continue
			}
		}

		if localRef.Name == "refs/heads/tag" {
			// https://gitlab.com/gitlab-org/gitaly/-/issues/3502
			return errors.New("close stream to gitaly-ruby: rpc error: code = Unknown desc = Gitlab::Git::CommandError: fatal: tag shorthand without <tag>")
		}

		// the mirror's ref does not match ours, we should update it.
		toUpdate[localRef.Name] = localRef.Target
		delete(remoteRefs, localRef.Name)
	}

	toDelete := remoteRefs
	if firstRequest.GetKeepDivergentRefs() {
		toDelete = map[git.ReferenceName]string{}
	}

	seen := map[string]struct{}{}
	var refspecs []string
	for prefix, references := range map[string]map[git.ReferenceName]string{
		"": toUpdate, ":": toDelete,
	} {
		for reference := range references {
			if !referenceMatcher.MatchString(reference.String()) {
				continue
			}

			refspecs = append(refspecs, prefix+reference.String())

			// https://gitlab.com/gitlab-org/gitaly/-/issues/3504
			name := strings.TrimPrefix(reference.String(), "refs/heads/")
			if strings.HasPrefix(reference.String(), "refs/tags/") {
				name = strings.TrimPrefix(reference.String(), "refs/tags/")
			}

			if _, ok := seen[name]; ok {
				return errors.New("close stream to gitaly-ruby: rpc error: code = Unknown desc = Gitlab::Git::CommandError: error: src refspec master matches more than one")
			}

			seen[name] = struct{}{}
		}
	}

	if len(refspecs) > 0 {
		sshCommand, clean, err := git.BuildSSHInvocation(ctx, firstRequest.GetSshKey(), firstRequest.GetKnownHosts())
		if err != nil {
			return fmt.Errorf("build ssh invocation: %w", err)
		}
		defer clean()

		for len(refspecs) > 0 {
			batch := refspecs
			if len(refspecs) > PushBatchSize {
				batch = refspecs[:PushBatchSize]
			}

			refspecs = refspecs[len(batch):]

			// The refs could have been modified on the mirror during after we fetched them.
			// This could cause divergent refs to be force pushed over even with keep_divergent_refs set.
			// This could be addressed by force pushing only if the current ref still matches what
			// we received in the original fetch. https://gitlab.com/gitlab-org/gitaly/-/issues/3505
			if err := repo.Push(ctx, firstRequest.GetRefName(), batch, localrepo.PushOptions{SSHCommand: sshCommand}); err != nil {
				return fmt.Errorf("push to mirror: %w", err)
			}
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

func validateUpdateRemoteMirrorRequest(req *gitalypb.UpdateRemoteMirrorRequest) error {
	if req.GetRepository() == nil {
		return fmt.Errorf("empty Repository")
	}
	if req.GetRefName() == "" {
		return fmt.Errorf("empty RefName")
	}

	return nil
}
