package storage

import (
	"os"
	"path/filepath"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"gitlab.com/gitlab-org/gitaly-proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
)

func (s *server) ListDirectories(req *gitalypb.ListDirectoriesRequest, stream gitalypb.StorageService_ListDirectoriesServer) error {
	storageDir, err := helper.GetStorageByName(req.StorageName)
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "storage lookup failed: %v", err)
	}

	storageDir = storageDir + "/"

	maxDepth := dirDepth(storageDir) + req.GetDepth()

	var dirs []string
	err = filepath.Walk(storageDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() {
			relPath := strings.TrimPrefix(path, storageDir)
			if relPath == "" {
				return nil
			}

			dirs = append(dirs, relPath)

			if len(dirs) > 100 {
				stream.Send(&gitalypb.ListDirectoriesResponse{Paths: dirs})
				dirs = dirs[:]
			}

			if dirDepth(path)+1 > maxDepth {
				return filepath.SkipDir
			}

			return nil
		}

		return nil
	})

	if len(dirs) > 0 {
		stream.Send(&gitalypb.ListDirectoriesResponse{Paths: dirs})
	}

	return err
}

func dirDepth(dir string) uint32 {
	return uint32(len(strings.Split(dir, string(os.PathSeparator)))) + 1
}
