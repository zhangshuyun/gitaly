package rubyserver

import (
	"context"
	"io"
	"os"
	"strings"

	"gitlab.com/gitlab-org/gitaly/v14/internal/storage"
	"gitlab.com/gitlab-org/gitaly/v14/internal/transaction/txinfo"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"google.golang.org/grpc/metadata"
)

// Headers prefixed with this string get allowlisted automatically
const rubyFeaturePrefix = "gitaly-feature-ruby-"

const (
	storagePathHeader  = "gitaly-storage-path"
	repoPathHeader     = "gitaly-repo-path"
	glRepositoryHeader = "gitaly-gl-repository"
	repoAltDirsHeader  = "gitaly-repo-alt-dirs"
)

// SetHeaders adds headers that tell gitaly-ruby the full path to the repository.
func SetHeaders(ctx context.Context, locator storage.Locator, repo *gitalypb.Repository) (context.Context, error) {
	return setHeaders(ctx, locator, repo, true)
}

func setHeaders(ctx context.Context, locator storage.Locator, repo *gitalypb.Repository, mustExist bool) (context.Context, error) {
	storagePath, err := locator.GetStorageByName(repo.GetStorageName())
	if err != nil {
		return nil, err
	}

	var repoPath string
	if mustExist {
		repoPath, err = locator.GetRepoPath(repo)
	} else {
		repoPath, err = locator.GetPath(repo)
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
	proxyHeaderAllowlist := []string{
		"gitaly-servers",
		txinfo.TransactionMetadataKey,
	}

	if inMD, ok := metadata.FromIncomingContext(ctx); ok {
		// Automatically allowlist any Ruby-specific feature flag
		for header := range inMD {
			if strings.HasPrefix(header, rubyFeaturePrefix) {
				proxyHeaderAllowlist = append(proxyHeaderAllowlist, header)
			}
		}

		for _, header := range proxyHeaderAllowlist {
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
