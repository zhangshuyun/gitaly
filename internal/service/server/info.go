package server

import (
	"io/ioutil"
	"os"
	"path"

	"gitlab.com/gitlab-org/gitaly-proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/internal/config"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/version"
	"golang.org/x/net/context"
)

func (s *server) ServerInfo(ctx context.Context, in *gitalypb.ServerInfoRequest) (*gitalypb.ServerInfoResponse, error) {
	gitVersion, err := git.Version()

	var storageStatuses []*gitalypb.ServerInfoResponse_StorageStatus
	for _, shard := range config.Config.Storages {
		readable, writeable := shardCheck(shard.Path)
		storageStatuses = append(storageStatuses, &gitalypb.ServerInfoResponse_StorageStatus{
			StorageName: shard.Name,
			Readable:    readable,
			Writeable:   writeable,
		})

	}

	return &gitalypb.ServerInfoResponse{
		ServerVersion:   version.GetVersion(),
		GitVersion:      gitVersion,
		StorageStatuses: storageStatuses,
	}, err
}

func shardCheck(shardPath string) (readable bool, writeable bool) {
	if _, err := os.Stat(shardPath); err == nil {
		readable = true
	}

	// the path uses a `+` to avoid naming collisions
	testPath := path.Join(shardPath, "+testWrite")

	content := []byte("testWrite")
	if err := ioutil.WriteFile(testPath, content, 0644); err == nil {
		writeable = true
	}
	os.Remove(testPath)

	return
}
