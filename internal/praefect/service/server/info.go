package server

import (
	"context"
	"sync"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc"
)

// ServerInfo sends ServerInfoRequest to all of a praefect server's internal gitaly nodes and aggregates the results into
// a response
func (s *Server) ServerInfo(ctx context.Context, in *gitalypb.ServerInfoRequest) (*gitalypb.ServerInfoResponse, error) {
	var once sync.Once

	var gitVersion, serverVersion string

	var wg sync.WaitGroup

	storageStatuses := make([]*gitalypb.ServerInfoResponse_StorageStatus, len(s.conns))

	i := 0
	for virtualStorage, storages := range s.conns {
		wg.Add(1)

		virtualStorage := virtualStorage
		storages := storages
		var storage string
		var conn *grpc.ClientConn

		// Pick one storage from the map.
		for storage, conn = range storages {
			break
		}

		go func(i int) {
			defer wg.Done()

			client := gitalypb.NewServerServiceClient(conn)
			resp, err := client.ServerInfo(ctx, &gitalypb.ServerInfoRequest{})
			if err != nil {
				ctxlogrus.Extract(ctx).WithField("storage", storage).WithError(err).Error("error getting server info")
				return
			}

			// From the perspective of the praefect client, a server info call should result in the server infos
			// of virtual storages. Each virtual storage has one or more nodes, but only the primary node's server info
			// needs to be returned. It's a common pattern in gitaly configs for all gitaly nodes in a fleet to use the same config.toml
			// whereby there are many storage names but only one of them is actually used by any given gitaly node:
			//
			// below is the config.toml for all three internal gitaly nodes
			// [[storage]]
			// name = "internal-gitaly-0"
			// path = "/var/opt/gitlab/git-data"
			//
			// [[storage]]
			// name = "internal-gitaly-1"
			// path = "/var/opt/gitlab/git-data"
			//
			// [[storage]]
			// name = "internal-gitaly-2"
			// path = "/var/opt/gitlab/git-data"
			//
			// technically, any storage's storage status can be returned in the virtual storage's server info,
			// but to be consistent we will choose the storage with the same name as the internal gitaly storage name.
			for _, storageStatus := range resp.GetStorageStatuses() {
				if storageStatus.StorageName == storage {
					storageStatuses[i] = storageStatus
					// the storage name in the response needs to be rewritten to be the virtual storage name
					// because the praefect client has no concept of internal gitaly nodes that are behind praefect.
					// From the perspective of the praefect client, the primary internal gitaly node's storage status is equivalent
					// to the virtual storage's storage status.
					storageStatuses[i].StorageName = virtualStorage
					storageStatuses[i].Writeable = storageStatus.Writeable
					storageStatuses[i].ReplicationFactor = uint32(len(storages))
					break
				}
			}

			once.Do(func() {
				gitVersion, serverVersion = resp.GetGitVersion(), resp.GetServerVersion()
			})
		}(i)

		i++
	}

	wg.Wait()

	return &gitalypb.ServerInfoResponse{
		ServerVersion:   serverVersion,
		GitVersion:      gitVersion,
		StorageStatuses: filterEmptyStorageStatuses(storageStatuses),
	}, nil
}

func filterEmptyStorageStatuses(storageStatuses []*gitalypb.ServerInfoResponse_StorageStatus) []*gitalypb.ServerInfoResponse_StorageStatus {
	var n int

	for _, storageStatus := range storageStatuses {
		if storageStatus != nil {
			storageStatuses[n] = storageStatus
			n++
		}
	}
	return storageStatuses[:n]
}
