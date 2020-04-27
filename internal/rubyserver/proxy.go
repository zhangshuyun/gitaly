package rubyserver

import (
	"context"
	"io"
	"os"
	"strings"

	"gitlab.com/gitlab-org/gitaly/internal/config"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// Headers prefixed with this string get whitelisted automatically
const rubyFeaturePrefix = "gitaly-feature-ruby-"

const (
	storagePathHeader  = "gitaly-storage-path"
	repoPathHeader     = "gitaly-repo-path"
	glRepositoryHeader = "gitaly-gl-repository"
	repoAltDirsHeader  = "gitaly-repo-alt-dirs"
)

// SetHeadersWithoutRepoCheck adds headers that tell gitaly-ruby the full
// path to the repository. It is not an error if the repository does not
// yet exist. This can be used on RPC calls that will create a
// repository.
func SetHeadersWithoutRepoCheck(ctx context.Context, repo *gitalypb.Repository) (context.Context, error) {
	return setHeaders(ctx, repo, false)
}

// SetHeaders adds headers that tell gitaly-ruby the full path to the repository.
func SetHeaders(ctx context.Context, repo *gitalypb.Repository) (context.Context, error) {
	return setHeaders(ctx, repo, true)
}

// SetHeaders adds headers that tell gitaly-ruby the full path to the repository.
func SetHeadersWithStorages(ctx context.Context, storages config.Storages, repo *gitalypb.Repository) (context.Context, error) {
	return setHeadersWithStorages(ctx, storages, repo, true)
}

func setHeaders(ctx context.Context, repo *gitalypb.Repository, mustExist bool) (context.Context, error) {
	return setHeadersWithStorages(ctx, config.Config.Storages, repo, mustExist)
}

func setHeadersWithStorages(ctx context.Context, storages config.Storages, repo *gitalypb.Repository, mustExist bool) (context.Context, error) {
	storagePath, ok := storages.GetPath(repo.GetStorageName())
	if !ok {
		return nil, status.Errorf(codes.InvalidArgument, "Storage can not be found by name '%s'", repo.GetStorageName())
	}

	var err error
	var repoPath string
	if mustExist {
		repoPath, err = helper.GetValidatedRepoPath(repo, storages)
	} else {
		repoPath, err = helper.GetRepositoryPath(repo, storages)
	}
	if err != nil {
		return nil, err
	}

	repoAltDirs := repo.GetGitAlternateObjectDirectories()
	repoAltDirs = append(repoAltDirs, repo.GetGitObjectDirectory())
	repoAltDirsCombined := strings.Join(repoAltDirs, string(os.PathListSeparator))

	md := metadata.Pairs(
		storagePathHeader, storagePath,
		repoPathHeader, repoPath,
		glRepositoryHeader, repo.GlRepository,
		repoAltDirsHeader, repoAltDirsCombined,
	)

	// list of http/2 headers that will be forwarded as-is to gitaly-ruby
	proxyHeaderWhitelist := []string{"gitaly-servers"}

	if inMD, ok := metadata.FromIncomingContext(ctx); ok {
		// Automatically whitelist any Ruby-specific feature flag
		for header := range inMD {
			if strings.HasPrefix(header, rubyFeaturePrefix) {
				proxyHeaderWhitelist = append(proxyHeaderWhitelist, header)
			}
		}

		for _, header := range proxyHeaderWhitelist {
			for _, v := range inMD[header] {
				md = metadata.Join(md, metadata.Pairs(header, v))
			}
		}
	}

	newCtx := metadata.NewOutgoingContext(ctx, md)
	return newCtx, nil
}

// Proxy calls recvSend until it receives an error. The error is returned
// to the caller unless it is io.EOF.
func Proxy(recvSend func() error) (err error) {
	for err == nil {
		err = recvSend()
	}

	if err == io.EOF {
		err = nil
	}
	return err
}

// CloseSender captures the CloseSend method from gRPC streams.
type CloseSender interface {
	CloseSend() error
}

// ProxyBidi works like Proxy but runs multiple callbacks simultaneously.
// It returns immediately if proxying one of the callbacks fails. If the
// response stream is done, ProxyBidi returns immediately without waiting
// for the client stream to finish proxying.
func ProxyBidi(requestFunc func() error, requestStream CloseSender, responseFunc func() error) error {
	requestErr := make(chan error, 1)
	go func() {
		requestErr <- Proxy(requestFunc)
	}()

	responseErr := make(chan error, 1)
	go func() {
		responseErr <- Proxy(responseFunc)
	}()

	for {
		select {
		case err := <-requestErr:
			if err != nil {
				return err
			}
			requestStream.CloseSend()
		case err := <-responseErr:
			return err
		}
	}
}
