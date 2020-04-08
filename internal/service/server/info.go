package server

import (
	"context"
	"io/ioutil"
	"os"
	"path"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"gitlab.com/gitlab-org/gitaly/internal/config"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/helper/fstype"
	"gitlab.com/gitlab-org/gitaly/internal/storage"
	"gitlab.com/gitlab-org/gitaly/internal/version"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

func (s *server) ServerInfo(ctx context.Context, in *gitalypb.ServerInfoRequest) (*gitalypb.ServerInfoResponse, error) {
	gitVersion, err := git.Version()
	if err != nil {
		return nil, helper.ErrInternal(err)
	}

	var storageStatuses []*gitalypb.ServerInfoResponse_StorageStatus
	for _, shard := range config.Config.Storages {
		readable, writeable := shardCheck(shard.Path)
		fsType := fstype.FileSystem(shard.Path)

		gitalyMetadata, err := storage.ReadMetadataFile(shard)
		if err != nil {
			ctxlogrus.Extract(ctx).WithField("storage", shard).WithError(err).Error("reading gitaly metadata file")
		}

		storageStatuses = append(storageStatuses, &gitalypb.ServerInfoResponse_StorageStatus{
			StorageName:  shard.Name,
			Readable:     readable,
			Writeable:    writeable,
			FsType:       fsType,
			FilesystemId: gitalyMetadata.GitalyFilesystemID,
		})
	}

	return &gitalypb.ServerInfoResponse{
		ServerVersion:   version.GetVersion(),
		GitVersion:      gitVersion,
		StorageStatuses: storageStatuses,
	}, nil
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
