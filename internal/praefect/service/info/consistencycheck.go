package info

import (
	"context"
	"io"

	"gitlab.com/gitlab-org/gitaly/v13/internal/middleware/metadatahandler"
	"gitlab.com/gitlab-org/gitaly/v13/internal/praefect/datastore"
	"gitlab.com/gitlab-org/gitaly/v13/internal/praefect/nodes"
	"gitlab.com/gitlab-org/gitaly/v13/proto/go/gitalypb"
	"gitlab.com/gitlab-org/labkit/correlation"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *Server) validateConsistencyCheckRequest(req *gitalypb.ConsistencyCheckRequest) error {
	if req.GetTargetStorage() == "" {
		return status.Error(codes.InvalidArgument, "missing target storage")
	}
	if req.GetVirtualStorage() == "" {
		return status.Error(codes.InvalidArgument, "missing virtual storage")
	}
	if req.GetReferenceStorage() == req.GetTargetStorage() {
		return status.Errorf(
			codes.InvalidArgument,
			"target storage %q cannot match reference storage %q",
			req.GetTargetStorage(), req.GetReferenceStorage(),
		)
	}
	return nil
}

func (s *Server) getNodes(req *gitalypb.ConsistencyCheckRequest) (target, reference nodes.Node, _ error) {
	shard, err := s.nodeMgr.GetShard(req.GetVirtualStorage())
	if err != nil {
		return nil, nil, status.Error(codes.NotFound, err.Error())
	}

	// search for target node amongst all nodes in shard
	for _, n := range append(shard.Secondaries, shard.Primary) {
		if n.GetStorage() == req.GetTargetStorage() {
			target = n
			break
		}
	}
	if target == nil {
		return nil, nil, status.Errorf(
			codes.NotFound,
			"unable to find target storage %q",
			req.GetTargetStorage(),
		)
	}

	// set reference node to default or requested storage
	switch {
	case req.GetReferenceStorage() == "" && req.GetTargetStorage() == shard.Primary.GetStorage():
		return nil, nil, status.Errorf(
			codes.InvalidArgument,
			"target storage %q is same as current primary, must provide alternate reference",
			req.GetTargetStorage(),
		)
	case req.GetReferenceStorage() == "":
		reference = shard.Primary // default
	case req.GetReferenceStorage() != "":
		for _, secondary := range append(shard.Secondaries, shard.Primary) {
			if secondary.GetStorage() == req.GetReferenceStorage() {
				reference = secondary
				break
			}
		}
		if reference == nil {
			return nil, nil, status.Errorf(
				codes.NotFound,
				"unable to find reference storage %q in nodes for shard %q",
				req.GetReferenceStorage(),
				req.GetVirtualStorage(),
			)
		}
	}

	return target, reference, nil
}

func walkRepos(ctx context.Context, walkerQ chan<- string, reference nodes.Node) error {
	defer close(walkerQ)

	iClient := gitalypb.NewInternalGitalyClient(reference.GetConnection())
	req := &gitalypb.WalkReposRequest{
		StorageName: reference.GetStorage(),
	}

	walkStream, err := iClient.WalkRepos(ctx, req)
	if err != nil {
		return err
	}

	for {
		resp, err := walkStream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		walkerQ <- resp.GetRelativePath()
	}
}

func checksumRepo(ctx context.Context, relpath string, node nodes.Node) (string, error) {
	cli := gitalypb.NewRepositoryServiceClient(node.GetConnection())
	resp, err := cli.CalculateChecksum(ctx, &gitalypb.CalculateChecksumRequest{
		Repository: &gitalypb.Repository{
			RelativePath: relpath,
			StorageName:  node.GetStorage(),
		},
	})
	if err != nil {
		return "", err
	}

	return resp.GetChecksum(), nil
}

type checksumResult struct {
	virtualStorage   string
	relativePath     string
	target           string
	reference        string
	targetStorage    string
	referenceStorage string
}

func checksumRepos(ctx context.Context, relpathQ <-chan string, checksumResultQ chan<- checksumResult, target, reference nodes.Node, virtualStorage string) error {
	defer close(checksumResultQ)

	for repoRelPath := range relpathQ {
		cs := checksumResult{
			virtualStorage:   virtualStorage,
			relativePath:     repoRelPath,
			targetStorage:    target.GetStorage(),
			referenceStorage: reference.GetStorage(),
		}

		g, ctx := errgroup.WithContext(ctx)

		g.Go(func() (err error) {
			cs.target, err = checksumRepo(ctx, repoRelPath, target)
			if status.Code(err) == codes.NotFound {
				// missing repo on target is okay, we need to
				// replicate from reference
				return nil
			}
			return err
		})

		g.Go(func() (err error) {
			cs.reference, err = checksumRepo(ctx, repoRelPath, reference)
			return err
		})

		if err := g.Wait(); err != nil {
			return err
		}

		checksumResultQ <- cs
	}

	return nil
}

func scheduleReplication(ctx context.Context, csr checksumResult, q datastore.ReplicationEventQueue, resp *gitalypb.ConsistencyCheckResponse) error {
	event, err := q.Enqueue(ctx, datastore.ReplicationEvent{
		Job: datastore.ReplicationJob{
			Change:            datastore.UpdateRepo,
			VirtualStorage:    csr.virtualStorage,
			RelativePath:      csr.relativePath,
			TargetNodeStorage: csr.targetStorage,
			SourceNodeStorage: csr.referenceStorage,
		},
		Meta: datastore.Params{metadatahandler.CorrelationIDKey: correlation.ExtractFromContext(ctx)},
	})

	if err != nil {
		return err
	}

	resp.ReplJobId = event.ID

	return nil
}

func ensureConsistency(ctx context.Context, disableReconcile bool, checksumResultQ <-chan checksumResult, q datastore.ReplicationEventQueue, stream gitalypb.PraefectInfoService_ConsistencyCheckServer) error {
	for {
		var csr checksumResult
		select {
		case res, ok := <-checksumResultQ:
			if !ok {
				return nil
			}
			csr = res
		case <-ctx.Done():
			return ctx.Err()
		}

		resp := &gitalypb.ConsistencyCheckResponse{
			RepoRelativePath:  csr.relativePath,
			ReferenceChecksum: csr.reference,
			TargetChecksum:    csr.target,
			ReferenceStorage:  csr.referenceStorage,
		}

		if csr.reference != csr.target && !disableReconcile {
			if err := scheduleReplication(ctx, csr, q, resp); err != nil {
				return err
			}
		}

		if err := stream.Send(resp); err != nil {
			return err
		}
	}
}

func (s *Server) ConsistencyCheck(req *gitalypb.ConsistencyCheckRequest, stream gitalypb.PraefectInfoService_ConsistencyCheckServer) error {
	if err := s.validateConsistencyCheckRequest(req); err != nil {
		return err
	}

	// target is the node we are checking, reference is the node we are
	// checking against (e.g. the primary node)
	target, reference, err := s.getNodes(req)
	if err != nil {
		return err
	}

	walkerQ := make(chan string)
	checksumResultQ := make(chan checksumResult)

	g, ctx := errgroup.WithContext(stream.Context())

	// the following goroutines form a pipeline where data flows from top
	// to bottom
	g.Go(func() error {
		return walkRepos(ctx, walkerQ, reference)
	})
	g.Go(func() error {
		return checksumRepos(ctx, walkerQ, checksumResultQ, target, reference, req.GetVirtualStorage())
	})
	g.Go(func() error {
		return ensureConsistency(ctx, req.GetDisableReconcilliation(), checksumResultQ, s.queue, stream)
	})

	return g.Wait()
}
