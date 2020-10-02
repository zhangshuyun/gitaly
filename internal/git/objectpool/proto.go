package objectpool

import (
	"gitlab.com/gitlab-org/gitaly/v13/internal/storage"
	"gitlab.com/gitlab-org/gitaly/v13/proto/go/gitalypb"
)

// FromProto returns an object pool object from a git repository object
func FromProto(locator storage.Locator, o *gitalypb.ObjectPool) (*ObjectPool, error) {
	return NewObjectPool(locator, o.GetRepository().GetStorageName(), o.GetRepository().GetRelativePath())
}

// ToProto returns a new struct that is the protobuf definition of the ObjectPool
func (o *ObjectPool) ToProto() *gitalypb.ObjectPool {
	return &gitalypb.ObjectPool{
		Repository: &gitalypb.Repository{
			StorageName:  o.GetStorageName(),
			RelativePath: o.GetRelativePath(),
		},
	}
}
