package operations

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"gitlab.com/gitlab-org/gitaly/internal/command"
	"gitlab.com/gitlab-org/gitaly/internal/git"
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
func (s *server) UserCommitFiles(stream gitalypb.OperationService_UserCommitFilesServer) error {
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
			var (
				response        gitalypb.UserCommitFilesResponse
				indexError      indexError
				preReceiveError preReceiveError
			)

			switch {
			case errors.As(err, &indexError):
				response = gitalypb.UserCommitFilesResponse{IndexError: indexError.Error()}
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
	}

	path, err := storage.ValidateRelativePath(rootPath, relPath)
	if err != nil {
		if errors.Is(err, storage.ErrRelativePathEscapesRoot) {
			return "", indexError("Path cannot include directory traversal")
		}

		return "", err
	}

	if relPath != path {
		return "", indexError("Path cannot include directory traversal")
	}

	return path, nil
}

func (s *server) userCommitFiles(ctx context.Context, header *gitalypb.UserCommitFilesRequestHeader, stream gitalypb.OperationService_UserCommitFilesServer) error {
	repoPath, err := s.locator.GetRepoPath(header.Repository)
	if err != nil {
		return fmt.Errorf("get repo path: %w", err)
	}

	localRepo := git.NewRepository(header.Repository)

	targetBranchName := "refs/heads/" + string(header.BranchName)
	targetBranchCommit, err := localRepo.ResolveRefish(ctx, targetBranchName+"^{commit}")
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
			header.StartRepository,
			targetBranchName,
			targetBranchCommit,
			string(header.StartBranchName),
		)
		if err != nil {
			return fmt.Errorf("resolve parent commit: %w", err)
		}
	}

	if parentCommitOID != targetBranchCommit {
		if err := s.fetchMissingCommit(ctx, header.Repository, header.StartRepository, parentCommitOID); err != nil {
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

	authorName := header.User.Name
	if len(header.CommitAuthorName) > 0 {
		authorName = header.CommitAuthorName
	}

	authorEmail := header.User.Email
	if len(header.CommitAuthorEmail) > 0 {
		authorEmail = header.CommitAuthorEmail
	}

	commitID, err := s.git2go.Commit(ctx, git2go.CommitParams{
		Repository: repoPath,
		Author:     git2go.NewSignature(string(authorName), string(authorEmail), time.Now()),
		Message:    string(header.CommitMessage),
		Parent:     parentCommitOID,
		Actions:    actions,
	})
	if err != nil {
		return fmt.Errorf("commit: %w", err)
	}

	hasBranches, err := hasBranches(ctx, header.Repository)
	if err != nil {
		return fmt.Errorf("was repo created: %w", err)
	}

	oldRevision := parentCommitOID
	if targetBranchCommit == "" {
		oldRevision = git.NullSHA
	} else if header.Force {
		oldRevision = targetBranchCommit
	}

	if err := s.updateReferenceWithHooks(ctx, header.Repository, header.User, targetBranchName, commitID, oldRevision); err != nil {
		return fmt.Errorf("update reference: %w", err)
	}

	return stream.SendAndClose(&gitalypb.UserCommitFilesResponse{BranchUpdate: &gitalypb.OperationBranchUpdate{
		CommitId:      commitID,
		RepoCreated:   !hasBranches,
		BranchCreated: parentCommitOID == "",
	}})
}

func (s *server) resolveParentCommit(ctx context.Context, local git.Repository, remote *gitalypb.Repository, targetBranch, targetBranchCommit, startBranch string) (string, error) {
	if remote == nil && startBranch == "" {
		return targetBranchCommit, nil
	}

	repo := local
	if remote != nil {
		var err error
		repo, err = git.NewRemoteRepository(ctx, remote, s.conns)
		if err != nil {
			return "", fmt.Errorf("remote repository: %w", err)
		}
	}

	branch := targetBranch
	if startBranch != "" {
		branch = "refs/heads/" + startBranch
	}

	return repo.ResolveRefish(ctx, branch+"^{commit}")
}

func (s *server) fetchMissingCommit(ctx context.Context, local, remote *gitalypb.Repository, commitID string) error {
	if _, err := git.NewRepository(local).ResolveRefish(ctx, commitID+"^{commit}"); err != nil {
		if !errors.Is(err, git.ErrReferenceNotFound) || remote == nil {
			return fmt.Errorf("lookup parent commit: %w", err)
		}

		if err := s.fetchRemoteObject(ctx, local, remote, commitID); err != nil {
			return fmt.Errorf("fetch parent commit: %w", err)
		}
	}

	return nil
}

func (s *server) fetchRemoteObject(ctx context.Context, local, remote *gitalypb.Repository, sha string) error {
	env, err := gitalyssh.UploadPackEnv(ctx, &gitalypb.SSHUploadPackRequest{
		Repository:       remote,
		GitConfigOptions: []string{"uploadpack.allowAnySHA1InWant=true"},
	})
	if err != nil {
		return fmt.Errorf("upload pack env: %w", err)
	}

	stderr := &bytes.Buffer{}
	cmd, err := git.SafeCmdWithEnv(ctx, env, local, nil,
		git.SubCmd{
			Name:  "fetch",
			Flags: []git.Option{git.Flag{Name: "--no-tags"}},
			Args:  []string{"ssh://gitaly/internal.git", sha},
		},
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

func hasBranches(ctx context.Context, repo *gitalypb.Repository) (bool, error) {
	stderr := &bytes.Buffer{}
	cmd, err := git.SafeCmd(ctx, repo, nil,
		git.SubCmd{
			Name:  "show-ref",
			Flags: []git.Option{git.Flag{Name: "--heads"}, git.Flag{"--dereference"}},
		},
		git.WithStderr(stderr),
	)
	if err != nil {
		return false, err
	}

	if err := cmd.Wait(); err != nil {
		if status, ok := command.ExitStatus(err); ok && status == 1 {
			return false, nil
		}

		return false, errorWithStderr(err, stderr)
	}

	return true, nil
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
		err := git.ValidateCommitID(startSha)
		if err != nil {
			return err
		}
	}

	return nil
}
