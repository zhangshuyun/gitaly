package repository

import (
	"regexp"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"gitlab.com/gitlab-org/gitaly-proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/internal/archive"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/streamio"
)

var objectFiles = []*regexp.Regexp{
	regexp.MustCompile(`/[[:xdigit:]]{2}/[[:xdigit:]]{38}\z`),
	regexp.MustCompile(`/pack/pack\-[[:xdigit:]]{40}\.(pack|idx)\z`),
}

func (s *server) GetSnapshot(in *gitalypb.GetSnapshotRequest, stream gitalypb.RepositoryService_GetSnapshotServer) error {
	path, err := helper.GetRepoPath(in.Repository)
	if err != nil {
		return err
	}

	writer := streamio.NewWriter(func(p []byte) error {
		return stream.Send(&gitalypb.GetSnapshotResponse{Data: p})
	})

	// Building a raw archive may race with `git push`, but GitLab can enforce
	// concurrency control if necessary. Using `TarBuilder` means we can keep
	// going even if some files are added or removed during the operation.
	builder := archive.NewTarBuilder(path, writer)

	// Pick files directly by filename so we can get a snapshot even if the
	// repository is corrupted. https://gitirc.eu/gitrepository-layout.html
	// documents the various files and directories. We exclude the following
	// on purpose:
	//
	//   * branches - legacy, not replicated by git fetch
	//   * commondir - may differ between sites
	//   * config - may contain credentials, and cannot be managed by client
	//   * custom-hooks - GitLab-specific, no supported in Geo, may differ between sites
	//   * hooks - symlink, may differ between sites
	//   * {shared,}index[.*] - not found in bare repositories
	//   * info/{attributes,exclude,grafts} - not replicated by git fetch
	//   * info/refs - dumb protocol only
	//   * logs/* - not replicated by git fetch
	//   * modules/* - not replicated by git fetch
	//   * objects/info/* - unneeded (dumb protocol) or to do with alternates
	//   * worktrees/* - not replicated by git fetch

	// References
	builder.FileIfExist("HEAD")
	builder.FileIfExist("packed-refs")
	builder.RecursiveDirIfExist("refs")
	builder.RecursiveDirIfExist("branches")

	// The packfiles + any loose objects.
	builder.RecursiveDirIfExist("objects", objectFiles...)

	// In case this repository is a shallow clone. Seems unlikely, but better
	// safe than sorry.
	builder.FileIfExist("shallow")

	if err := builder.Close(); err != nil {
		return status.Errorf(codes.Internal, "Building snapshot failed: %v", err)
	}

	return nil
}
