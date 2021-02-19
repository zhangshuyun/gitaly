package operations

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/golang/protobuf/ptypes"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/internal/git/remoterepo"
	"gitlab.com/gitlab-org/gitaly/internal/git2go"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/rubyserver"
	"gitlab.com/gitlab-org/gitaly/internal/gitalyssh"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/internal/storage"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type indexError string

func (err indexError) Error() string { return string(err) }

func errorWithStderr(err error, stderr *bytes.Buffer) error {
	return fmt.Errorf("%w, stderr: %q", err, stderr)
}

// UserCommitFiles allows for committing from a set of actions. See the protobuf documentation
// for details.
func (s *Server) UserCommitFiles(stream gitalypb.OperationService_UserCommitFilesServer) error {
	firstRequest, err := stream.Recv()
	if err != nil {
		return err
	}

	header := firstRequest.GetHeader()
	if header == nil {
		return status.Errorf(codes.InvalidArgument, "UserCommitFiles: empty UserCommitFilesRequestHeader")
	}

	if err = validateUserCommitFilesHeader(header); err != nil {
		return status.Errorf(codes.InvalidArgument, "UserCommitFiles: %v", err)
	}

	ctx := stream.Context()

	if featureflag.IsEnabled(ctx, featureflag.GoUserCommitFiles) {
		if err := s.userCommitFiles(ctx, header, stream); err != nil {
			ctxlogrus.AddFields(ctx, logrus.Fields{
				"repository_storage":       header.Repository.StorageName,
				"repository_relative_path": header.Repository.RelativePath,
				"branch_name":              header.BranchName,
				"start_branch_name":        header.StartBranchName,
				"start_sha":                header.StartSha,
				"force":                    header.Force,
			})

			if startRepo := header.GetStartRepository(); startRepo != nil {
				ctxlogrus.AddFields(ctx, logrus.Fields{
					"start_repository_storage":       startRepo.StorageName,
					"start_repository_relative_path": startRepo.RelativePath,
				})
			}

			var (
				response        gitalypb.UserCommitFilesResponse
				indexError      indexError
				preReceiveError preReceiveError
			)

			switch {
			case errors.As(err, &indexError):
				response = gitalypb.UserCommitFilesResponse{IndexError: indexError.Error()}
			case errors.As(err, new(git2go.IndexError)):
				response = gitalypb.UserCommitFilesResponse{IndexError: err.Error()}
			case errors.As(err, new(git2go.DirectoryExistsError)):
				response = gitalypb.UserCommitFilesResponse{IndexError: "A directory with this name already exists"}
			case errors.As(err, new(git2go.FileExistsError)):
				response = gitalypb.UserCommitFilesResponse{IndexError: "A file with this name already exists"}
			case errors.As(err, new(git2go.FileNotFoundError)):
				response = gitalypb.UserCommitFilesResponse{IndexError: "A file with this name doesn't exist"}
			case errors.As(err, &preReceiveError):
				response = gitalypb.UserCommitFilesResponse{PreReceiveError: preReceiveError.Error()}
			case errors.As(err, new(git2go.InvalidArgumentError)):
				return helper.ErrInvalidArgument(err)
			default:
				return err
			}

			ctxlogrus.Extract(ctx).WithError(err).Error("user commit files failed")
			return stream.SendAndClose(&response)
		}

		return nil
	}

	client, err := s.ruby.OperationServiceClient(ctx)
	if err != nil {
		return err
	}

	clientCtx, err := rubyserver.SetHeaders(ctx, s.locator, header.GetRepository())
	if err != nil {
		return err
	}

	rubyStream, err := client.UserCommitFiles(clientCtx)
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

func validatePath(rootPath, relPath string) (string, error) {
	if relPath == "" {
		return "", indexError("You must provide a file path")
	} else if strings.Contains(relPath, "//") {
		// This is a workaround to address a quirk in porting the RPC from Ruby to Go.
		// GitLab's QA pipeline runs tests with filepath 'invalid://file/name/here'.
		// Go's filepath.Clean returns 'invalid:/file/name/here'. The Ruby implementation's
		// filepath normalization accepted the path as is. Adding a file with this path to the
		// index via Rugged failed with an invalid path error. As Go's cleaning resulted a valid
		// filepath, adding the file succeeded, which made the QA pipeline's specs fail.
		//
		// The Rails code expects to receive an error prefixed with 'invalid path', which is done
		// here to retain compatibility.
		return "", indexError(fmt.Sprintf("invalid path: '%s'", relPath))
	}

	path, err := storage.ValidateRelativePath(rootPath, relPath)
	if err != nil {
		if errors.Is(err, storage.ErrRelativePathEscapesRoot) {
			return "", indexError("Path cannot include directory traversal")
		}

		return "", err
	}

	return path, nil
}

func (s *Server) userCommitFiles(ctx context.Context, header *gitalypb.UserCommitFilesRequestHeader, stream gitalypb.OperationService_UserCommitFilesServer) error {
	repoPath, err := s.locator.GetRepoPath(header.Repository)
	if err != nil {
		return fmt.Errorf("get repo path: %w", err)
	}

	remoteRepo := header.GetStartRepository()
	if sameRepository(header.GetRepository(), remoteRepo) {
		// Some requests set a StartRepository that refers to the same repository as the target repository.
		// This check never works behind Praefect. See: https://gitlab.com/gitlab-org/gitaly/-/issues/3294
		// Plain Gitalies still benefit from identifying the case and avoiding unnecessary RPC to resolve the
		// branch.
		remoteRepo = nil
	}

	localRepo := localrepo.New(s.gitCmdFactory, header.Repository, s.cfg)

	targetBranchName := "refs/heads/" + string(header.BranchName)
	targetBranchCommit, err := localRepo.ResolveRevision(ctx, git.Revision(targetBranchName+"^{commit}"))
	if err != nil {
		if !errors.Is(err, git.ErrReferenceNotFound) {
			return fmt.Errorf("resolve target branch commit: %w", err)
		}

		// the branch is being created
	}

	parentCommitOID := header.StartSha
	if parentCommitOID == "" {
		parentCommitOID, err = s.resolveParentCommit(
			ctx,
			localRepo,
			remoteRepo,
			targetBranchName,
			targetBranchCommit.String(),
			string(header.StartBranchName),
		)
		if err != nil {
			return fmt.Errorf("resolve parent commit: %w", err)
		}
	}

	if parentCommitOID != targetBranchCommit.String() {
		if err := s.fetchMissingCommit(ctx, header.Repository, remoteRepo, parentCommitOID); err != nil {
			return fmt.Errorf("fetch missing commit: %w", err)
		}
	}

	type action struct {
		header  *gitalypb.UserCommitFilesActionHeader
		content []byte
	}

	var pbActions []action

	for {
		req, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}

			return fmt.Errorf("receive request: %w", err)
		}

		switch payload := req.GetAction().GetUserCommitFilesActionPayload().(type) {
		case *gitalypb.UserCommitFilesAction_Header:
			pbActions = append(pbActions, action{header: payload.Header})
		case *gitalypb.UserCommitFilesAction_Content:
			if len(pbActions) == 0 {
				return errors.New("content sent before action")
			}

			// append the content to the previous action
			content := &pbActions[len(pbActions)-1].content
			*content = append(*content, payload.Content...)
		default:
			return fmt.Errorf("unhandled action payload type: %T", payload)
		}
	}

	actions := make([]git2go.Action, 0, len(pbActions))
	for _, pbAction := range pbActions {
		if _, ok := gitalypb.UserCommitFilesActionHeader_ActionType_name[int32(pbAction.header.Action)]; !ok {
			return fmt.Errorf("NoMethodError: undefined method `downcase' for %d:Integer", pbAction.header.Action)
		}

		path, err := validatePath(repoPath, string(pbAction.header.FilePath))
		if err != nil {
			return fmt.Errorf("validate path: %w", err)
		}

		content := io.Reader(bytes.NewReader(pbAction.content))
		if pbAction.header.Base64Content {
			content = base64.NewDecoder(base64.StdEncoding, content)
		}

		switch pbAction.header.Action {
		case gitalypb.UserCommitFilesActionHeader_CREATE:
			blobID, err := localRepo.WriteBlob(ctx, path, content)
			if err != nil {
				return fmt.Errorf("write created blob: %w", err)
			}

			actions = append(actions, git2go.CreateFile{
				OID:            blobID,
				Path:           path,
				ExecutableMode: pbAction.header.ExecuteFilemode,
			})
		case gitalypb.UserCommitFilesActionHeader_CHMOD:
			actions = append(actions, git2go.ChangeFileMode{
				Path:           path,
				ExecutableMode: pbAction.header.ExecuteFilemode,
			})
		case gitalypb.UserCommitFilesActionHeader_MOVE:
			prevPath, err := validatePath(repoPath, string(pbAction.header.PreviousPath))
			if err != nil {
				return fmt.Errorf("validate previous path: %w", err)
			}

			var oid string
			if !pbAction.header.InferContent {
				var err error
				oid, err = localRepo.WriteBlob(ctx, path, content)
				if err != nil {
					return err
				}
			}

			actions = append(actions, git2go.MoveFile{
				Path:    prevPath,
				NewPath: path,
				OID:     oid,
			})
		case gitalypb.UserCommitFilesActionHeader_UPDATE:
			oid, err := localRepo.WriteBlob(ctx, path, content)
			if err != nil {
				return fmt.Errorf("write updated blob: %w", err)
			}

			actions = append(actions, git2go.UpdateFile{
				Path: path,
				OID:  oid,
			})
		case gitalypb.UserCommitFilesActionHeader_DELETE:
			actions = append(actions, git2go.DeleteFile{
				Path: path,
			})
		case gitalypb.UserCommitFilesActionHeader_CREATE_DIR:
			actions = append(actions, git2go.CreateDirectory{
				Path: path,
			})
		}
	}

	now := time.Now()
	if header.Timestamp != nil {
		now, err = ptypes.Timestamp(header.Timestamp)
		if err != nil {
			return helper.ErrInvalidArgument(err)
		}
	}

	committer := git2go.NewSignature(string(header.User.Name), string(header.User.Email), now)
	author := committer
	if len(header.CommitAuthorName) > 0 && len(header.CommitAuthorEmail) > 0 {
		author = git2go.NewSignature(string(header.CommitAuthorName), string(header.CommitAuthorEmail), now)
	}

	commitID, err := s.git2go.Commit(ctx, git2go.CommitParams{
		Repository: repoPath,
		Author:     author,
		Committer:  committer,
		Message:    string(header.CommitMessage),
		Parent:     parentCommitOID,
		Actions:    actions,
	})
	if err != nil {
		return fmt.Errorf("commit: %w", err)
	}

	hasBranches, err := localRepo.HasBranches(ctx)
	if err != nil {
		return fmt.Errorf("was repo created: %w", err)
	}

	oldRevision := parentCommitOID
	if targetBranchCommit == "" {
		oldRevision = git.ZeroOID.String()
	} else if header.Force {
		oldRevision = targetBranchCommit.String()
	}

	if err := s.updateReferenceWithHooks(ctx, header.Repository, header.User, targetBranchName, commitID, oldRevision); err != nil {
		if errors.As(err, &updateRefError{}) {
			return status.Errorf(codes.FailedPrecondition, err.Error())
		}

		return fmt.Errorf("update reference: %w", err)
	}

	return stream.SendAndClose(&gitalypb.UserCommitFilesResponse{BranchUpdate: &gitalypb.OperationBranchUpdate{
		CommitId:      commitID,
		RepoCreated:   !hasBranches,
		BranchCreated: parentCommitOID == "",
	}})
}

func sameRepository(repoA, repoB *gitalypb.Repository) bool {
	return repoA.GetStorageName() == repoB.GetStorageName() &&
		repoA.GetRelativePath() == repoB.GetRelativePath()
}

func (s *Server) resolveParentCommit(ctx context.Context, local git.Repository, remote *gitalypb.Repository, targetBranch, targetBranchCommit, startBranch string) (string, error) {
	if remote == nil && startBranch == "" {
		return targetBranchCommit, nil
	}

	repo := local
	if remote != nil {
		var err error
		repo, err = remoterepo.New(ctx, remote, s.conns)
		if err != nil {
			return "", fmt.Errorf("remote repository: %w", err)
		}
	}

	if hasBranches, err := repo.HasBranches(ctx); err != nil {
		return "", fmt.Errorf("has branches: %w", err)
	} else if !hasBranches {
		// GitLab sends requests to UserCommitFiles where target repository
		// and start repository are the same. If the request hits Gitaly directly,
		// Gitaly could check if the repos are the same by comparing their storages
		// and relative paths and simply resolve the branch locally. When request is proxied
		// through Praefect, the start repository's storage is not rewritten, thus Gitaly can't
		// identify the repos as being the same.
		//
		// If the start repository is set, we have to resolve the branch there as it
		// might be on a different commit than the local repository. As Gitaly can't identify
		// the repositories are the same behind Praefect, it has to perform an RPC to resolve
		// the branch. The resolving would fail as the branch does not yet exist in the start
		// repository, which is actually the local repository.
		//
		// Due to this, we check if the remote has any branches. If not, we likely hit this case
		// and we're creating the first branch. If so, we'll just return the commit that was
		// already resolved locally.
		//
		// See: https://gitlab.com/gitlab-org/gitaly/-/issues/3294
		return targetBranchCommit, nil
	}

	branch := targetBranch
	if startBranch != "" {
		branch = "refs/heads/" + startBranch
	}
	refish := branch + "^{commit}"

	commit, err := repo.ResolveRevision(ctx, git.Revision(refish))
	if err != nil {
		return "", fmt.Errorf("resolving refish %q in %T: %w", refish, repo, err)
	}

	return commit.String(), nil
}

func (s *Server) fetchMissingCommit(ctx context.Context, local, remote *gitalypb.Repository, commitID string) error {
	if _, err := localrepo.New(s.gitCmdFactory, local, s.cfg).ResolveRevision(ctx, git.Revision(commitID+"^{commit}")); err != nil {
		if !errors.Is(err, git.ErrReferenceNotFound) || remote == nil {
			return fmt.Errorf("lookup parent commit: %w", err)
		}

		if err := s.fetchRemoteObject(ctx, local, remote, commitID); err != nil {
			return fmt.Errorf("fetch parent commit: %w", err)
		}
	}

	return nil
}

func (s *Server) fetchRemoteObject(ctx context.Context, local, remote *gitalypb.Repository, sha string) error {
	env, err := gitalyssh.UploadPackEnv(ctx, s.cfg, &gitalypb.SSHUploadPackRequest{
		Repository:       remote,
		GitConfigOptions: []string{"uploadpack.allowAnySHA1InWant=true"},
	})
	if err != nil {
		return fmt.Errorf("upload pack env: %w", err)
	}

	stderr := &bytes.Buffer{}
	cmd, err := s.gitCmdFactory.New(ctx, local, nil,
		git.SubCmd{
			Name:  "fetch",
			Flags: []git.Option{git.Flag{Name: "--no-tags"}},
			Args:  []string{"ssh://gitaly/internal.git", sha},
		},
		git.WithEnv(env...),
		git.WithStderr(stderr),
		git.WithRefTxHook(ctx, local, s.cfg),
	)
	if err != nil {
		return err
	}

	if err := cmd.Wait(); err != nil {
		return errorWithStderr(err, stderr)
	}

	return nil
}

func validateUserCommitFilesHeader(header *gitalypb.UserCommitFilesRequestHeader) error {
	if header.GetRepository() == nil {
		return fmt.Errorf("empty Repository")
	}
	if header.GetUser() == nil {
		return fmt.Errorf("empty User")
	}
	if len(header.GetCommitMessage()) == 0 {
		return fmt.Errorf("empty CommitMessage")
	}
	if len(header.GetBranchName()) == 0 {
		return fmt.Errorf("empty BranchName")
	}

	startSha := header.GetStartSha()
	if len(startSha) > 0 {
		err := git.ValidateObjectID(startSha)
		if err != nil {
			return err
		}
	}

	return nil
}
