package rubyserver

import (
	"context"
	"io"
	"os"
	"strings"

	"gitlab.com/gitlab-org/gitaly-proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"google.golang.org/grpc/metadata"
)

// ProxyHeaderWhitelist is the list of http/2 headers that will be
// forwarded as-is to gitaly-ruby.
var ProxyHeaderWhitelist = []string{"gitaly-servers"}

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

func setHeaders(ctx context.Context, repo *gitalypb.Repository, mustExist bool) (context.Context, error) {
	storagePath, err := helper.GetStorageByName(repo.GetStorageName())
	if err != nil {
		return nil, err
	}

	var repoPath string
	if mustExist {
		repoPath, err = helper.GetRepoPath(repo)
	} else {
		repoPath, err = helper.GetPath(repo)
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

	if inMD, ok := metadata.FromIncomingContext(ctx); ok {
		for _, header := range ProxyHeaderWhitelist {
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
