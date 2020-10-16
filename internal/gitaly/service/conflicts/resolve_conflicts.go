package conflicts

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"

	"gitlab.com/gitlab-org/gitaly/client"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/repository"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/rubyserver"
	"gitlab.com/gitlab-org/gitaly/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *server) ResolveConflicts(stream gitalypb.ConflictsService_ResolveConflictsServer) error {
	firstRequest, err := stream.Recv()
	if err != nil {
		return err
	}

	header := firstRequest.GetHeader()
	if header == nil {
		return status.Errorf(codes.InvalidArgument, "ResolveConflicts: empty ResolveConflictsRequestHeader")
	}

	if err = validateResolveConflictsHeader(header); err != nil {
		return status.Errorf(codes.InvalidArgument, "ResolveConflicts: %v", err)
	}

	if featureflag.IsEnabled(stream.Context(), featureflag.GoResolveConflicts) {
		return s.resolveConflicts(header, stream)
	}

	ctx := stream.Context()
	client, err := s.ruby.ConflictsServiceClient(ctx)
	if err != nil {
		return err
	}

	clientCtx, err := rubyserver.SetHeaders(ctx, header.GetRepository())
	if err != nil {
		return err
	}

	rubyStream, err := client.ResolveConflicts(clientCtx)
	if err != nil {
		return err
	}

	if err := rubyStream.Send(firstRequest); err != nil {
		return err
	}

	err = rubyserver.Proxy(func() error {
		request, err := stream.Recv()
		if err != nil {
			return err
		}
		return rubyStream.Send(request)
	})

	if err != nil {
		return err
	}

	response, err := rubyStream.CloseAndRecv()
	if err != nil {
		return err
	}

	return stream.SendAndClose(response)
}

func validateResolveConflictsHeader(header *gitalypb.ResolveConflictsRequestHeader) error {
	if header.GetOurCommitOid() == "" {
		return fmt.Errorf("empty OurCommitOid")
	}
	if header.GetTargetRepository() == nil {
		return fmt.Errorf("empty TargetRepository")
	}
	if header.GetTheirCommitOid() == "" {
		return fmt.Errorf("empty TheirCommitOid")
	}
	if header.GetSourceBranch() == nil {
		return fmt.Errorf("empty SourceBranch")
	}
	if header.GetTargetBranch() == nil {
		return fmt.Errorf("empty TargetBranch")
	}
	if header.GetCommitMessage() == nil {
		return fmt.Errorf("empty CommitMessage")
	}
	if header.GetUser() == nil {
		return fmt.Errorf("empty User")
	}

	return nil
}

type conflictFile struct {
	OldPath  string            `json:"old_path"`
	NewPath  string            `json:"new_path"`
	Sections map[string]string `json:"sections"`
}

func cfgContainsRepo(cfg config.Cfg, r repository.GitRepo) bool {
	for _, s := range cfg.Storages {
		if r.GetStorageName() == s.Name {
			return true
		}
	}
	return false
}

func (s *server) resolveConflicts(header *gitalypb.ResolveConflictsRequestHeader, stream gitalypb.ConflictsService_ResolveConflictsServer) error {
	b := bytes.NewBuffer(nil)
	for {
		req, err := stream.Recv()
		switch err {
		case io.EOF:
			return nil
		case nil:
		// do nothing, continue
		default:
			return err
		}

		if _, err := b.Write(req.GetFilesJson()); err != nil {
			return err
		}
	}

	var conflicts []map[string]conflictFile
	if err := json.NewDecoder(b).Decode(&conflicts); err != nil {
		return err
	}

	srcRepo := git.NewRepository(header.GetRepository())

	var dstRepo git.Repository
	switch dst := header.GetTargetRepository(); {
	case dst == nil:
		dstRepo = srcRepo
	case cfgContainsRepo(s.cfg, dst):
		dstRepo = git.NewRepository(dst)
	default:
		var err error
		dstRepo, err = git.NewRemoteRepository(stream.Context(), dst, client.NewPool())
		if err != nil {
			return err
		}
	}

	targetOID, err := dstRepo.ResolveRefish(ctx, string(header.TargetBranch))
	if err != nil {
		return err
	}

	return nil
}
