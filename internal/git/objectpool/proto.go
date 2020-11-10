package objectpool

import (
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

// FromProto returns an object pool object from a git repository object
func FromProto(cfg config.Cfg, o *gitalypb.ObjectPool) (*ObjectPool, error) {
	return NewObjectPool(cfg, o.GetRepository().GetStorageName(), o.GetRepository().GetRelativePath())
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
