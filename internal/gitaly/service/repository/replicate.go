package repository

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"gitlab.com/gitlab-org/gitaly/internal/command"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/safe"
	"gitlab.com/gitlab-org/gitaly/internal/storage"
	"gitlab.com/gitlab-org/gitaly/internal/tempdir"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/streamio"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ErrInvalidSourceRepository is returned when attempting to replicate from an invalid source repository.
var ErrInvalidSourceRepository = status.Error(codes.NotFound, "invalid source repository")

func (s *server) ReplicateRepository(ctx context.Context, in *gitalypb.ReplicateRepositoryRequest) (*gitalypb.ReplicateRepositoryResponse, error) {
	if err := validateReplicateRepository(in); err != nil {
		return nil, helper.ErrInvalidArgument(err)
	}

	repoPath, err := s.locator.GetPath(in.GetRepository())
	if err != nil {
		return nil, helper.ErrInternal(err)
	}

	if !storage.IsGitDirectory(repoPath) {
		if err = s.create(ctx, in, repoPath); err != nil {
			if errors.Is(err, ErrInvalidSourceRepository) {
				return nil, ErrInvalidSourceRepository
			}

			return nil, helper.ErrInternal(err)
		}
	}

	// We're not using the context of the errgroup here, as an error
	// returned by either of the called functions would cancel the
	// respective other function. Given that we're doing RPC calls in
	// them, cancellation of the calls would mean that the remote side
	// may still modify the repository even though the local side has
	// returned already.
	g, _ := errgroup.WithContext(ctx)
	outgoingCtx := helper.IncomingToOutgoing(ctx)

	syncFuncs := []func(context.Context, *gitalypb.ReplicateRepositoryRequest) error{
		s.syncInfoAttributes,
		s.syncRepository,
	}

	for _, f := range syncFuncs {
		f := f // rescoping f
		g.Go(func() error { return f(outgoingCtx, in) })
	}

	if err := g.Wait(); err != nil {
		return nil, helper.ErrInternal(err)
	}

	return &gitalypb.ReplicateRepositoryResponse{}, nil
}

func validateReplicateRepository(in *gitalypb.ReplicateRepositoryRequest) error {
	if in.GetRepository() == nil {
		return errors.New("repository cannot be empty")
	}

	if in.GetSource() == nil {
		return errors.New("source repository cannot be empty")
	}

	if in.GetRepository().GetRelativePath() != in.GetSource().GetRelativePath() {
		return errors.New("both source and repository should have the same relative path")
	}

	if in.GetRepository().GetStorageName() == in.GetSource().GetStorageName() {
		return errors.New("repository and source have the same storage")
	}

	return nil
}

func (s *server) create(ctx context.Context, in *gitalypb.ReplicateRepositoryRequest, repoPath string) error {
	// if the directory exists, remove it
	if _, err := os.Stat(repoPath); err == nil {
		tempDir, err := tempdir.ForDeleteAllRepositories(s.locator, in.GetRepository().GetStorageName())
		if err != nil {
			return err
		}

		if err = os.Rename(repoPath, filepath.Join(tempDir, filepath.Base(repoPath))); err != nil {
			return fmt.Errorf("error deleting invalid repo: %v", err)
		}

		ctxlogrus.Extract(ctx).WithField("repo_path", repoPath).Warn("removed invalid repository")
	}

	if err := s.createFromSnapshot(ctx, in); err != nil {
		return fmt.Errorf("could not create repository from snapshot: %w", err)
	}

	return nil
}

func (s *server) createFromSnapshot(ctx context.Context, in *gitalypb.ReplicateRepositoryRequest) error {
	tempRepo, tempPath, err := tempdir.NewAsRepository(ctx, in.GetRepository(), s.locator)
	if err != nil {
		return fmt.Errorf("create temporary directory: %w", err)
	}

	if _, err := s.CreateRepository(ctx, &gitalypb.CreateRepositoryRequest{
		Repository: tempRepo,
	}); err != nil {
		return fmt.Errorf("create repository: %w", err)
	}

	repoClient, err := s.newRepoClient(ctx, in.GetSource().GetStorageName())
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}

	stream, err := repoClient.GetSnapshot(ctx, &gitalypb.GetSnapshotRequest{Repository: in.GetSource()})
	if err != nil {
		return fmt.Errorf("get snapshot: %w", err)
	}

	// We need to catch a possible 'invalid repository' error from GetSnapshot. On an empty read,
	// BSD tar exits with code 0 so we'd receive the error when waiting for the command. GNU tar on
	// Linux exits with a non-zero code, which causes Go to return an os.ExitError hiding the original
	// error reading from stdin. To get access to the error on both Linux and macOS, we read the first
	// message from the stream here to get access to the possible 'invalid repository' first on both
	// platforms.
	firstBytes, err := stream.Recv()
	if err != nil {
		if st, ok := status.FromError(err); ok {
			if st.Code() == codes.NotFound && strings.HasPrefix(st.Message(), "GetRepoPath: not a git repository:") {
				return ErrInvalidSourceRepository
			}
		}

		return fmt.Errorf("first snapshot read: %w", err)
	}

	snapshotReader := io.MultiReader(
		bytes.NewReader(firstBytes.GetData()),
		streamio.NewReader(func() ([]byte, error) {
			resp, err := stream.Recv()
			return resp.GetData(), err
		}),
	)

	stderr := &bytes.Buffer{}
	cmd, err := command.New(ctx, exec.Command("tar", "-C", tempPath, "-xvf", "-"), snapshotReader, nil, stderr)
	if err != nil {
		return fmt.Errorf("create tar command: %w", err)
	}

	if err = cmd.Wait(); err != nil {
		return fmt.Errorf("wait for tar, stderr: %q, err: %w", stderr, err)
	}

	targetPath, err := s.locator.GetPath(in.GetRepository())
	if err != nil {
		return fmt.Errorf("locate repository: %w", err)
	}

	if err = os.MkdirAll(filepath.Dir(targetPath), 0755); err != nil {
		return fmt.Errorf("create parent directories: %w", err)
	}

	if err := os.Rename(tempPath, targetPath); err != nil {
		return fmt.Errorf("move temporary directory to target path: %w", err)
	}

	return nil
}

func (s *server) syncRepository(ctx context.Context, in *gitalypb.ReplicateRepositoryRequest) error {
	remoteClient, err := s.newRemoteClient(ctx)
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}

	resp, err := remoteClient.FetchInternalRemote(ctx, &gitalypb.FetchInternalRemoteRequest{
		Repository:       in.GetRepository(),
		RemoteRepository: in.GetSource(),
	})
	if err != nil {
		return fmt.Errorf("fetch internal remote: %w", err)
	}

	if !resp.Result {
		return errors.New("FetchInternalRemote failed")
	}

	return nil
}

func (s *server) syncInfoAttributes(ctx context.Context, in *gitalypb.ReplicateRepositoryRequest) error {
	repoClient, err := s.newRepoClient(ctx, in.GetSource().GetStorageName())
	if err != nil {
		return err
	}

	repoPath, err := s.locator.GetRepoPath(in.GetRepository())
	if err != nil {
		return err
	}

	infoPath := filepath.Join(repoPath, "info")
	attributesPath := filepath.Join(infoPath, "attributes")

	if err := os.MkdirAll(infoPath, 0755); err != nil {
		return err
	}

	fw, err := safe.CreateFileWriter(attributesPath)
	if err != nil {
		return err
	}
	defer fw.Close()

	stream, err := repoClient.GetInfoAttributes(ctx, &gitalypb.GetInfoAttributesRequest{
		Repository: in.GetSource(),
	})
	if err != nil {
		return err
	}

	if _, err := io.Copy(fw, streamio.NewReader(func() ([]byte, error) {
		resp, err := stream.Recv()
		return resp.GetAttributes(), err
	})); err != nil {
		return err
	}

	if err = fw.Commit(); err != nil {
		return err
	}

	if err := os.Chmod(attributesPath, attributesFileMode); err != nil {
		return err
	}

	return os.Rename(attributesPath, attributesPath)
}

// newRemoteClient creates a new RemoteClient that talks to the same gitaly server
func (s *server) newRemoteClient(ctx context.Context) (gitalypb.RemoteServiceClient, error) {
	conn, err := s.conns.Dial(ctx, fmt.Sprintf("unix:%s", s.cfg.GitalyInternalSocketPath()), s.cfg.Auth.Token)
	if err != nil {
		return nil, err
	}

	return gitalypb.NewRemoteServiceClient(conn), nil
}

// newRepoClient creates a new RepositoryClient that talks to the gitaly of the source repository
func (s *server) newRepoClient(ctx context.Context, storageName string) (gitalypb.RepositoryServiceClient, error) {
	gitalyServerInfo, err := helper.ExtractGitalyServer(ctx, storageName)
	if err != nil {
		return nil, err
	}

	conn, err := s.conns.Dial(ctx, gitalyServerInfo.Address, gitalyServerInfo.Token)
	if err != nil {
		return nil, err
	}

	return gitalypb.NewRepositoryServiceClient(conn), nil
}
