package repository

import (
	"context"
	"errors"
	"io/ioutil"
	"os/exec"

	"gitlab.com/gitlab-org/gitaly/internal/command"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/metadata"
)

type writeRefRequest interface {
	GetRef() []byte
	GetRevision() []byte
	GetOldRevision() []byte
	GetRepository() *gitalypb.Repository
}

func (s *server) WriteRefTx(ctx context.Context, req *gitalypb.WriteRefTxRequest) (*gitalypb.WriteRefTxResponse, error) {
	if req.GetTransaction().Step == gitalypb.Transaction_PRECOMMIT {
		if err := validateWriteRefRequest(req); err != nil {
			return nil, helper.ErrInvalidArgument(err)
		}
	}

	if req.Transaction == nil {
		return nil, helper.ErrInvalidArgument(errors.New("transaction is empty"))
	}

	return s.transactionalWriteRef(ctx, req)
}

func (s *server) transactionalWriteRef(ctx context.Context, req *gitalypb.WriteRefTxRequest) (*gitalypb.WriteRefTxResponse, error) {
	var resp gitalypb.WriteRefTxResponse

	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, helper.ErrInternal(errors.New("couldn't get metadata"))
	}
	if len(md.Get("transaction_id")) == 0 {
		return nil, helper.ErrInternal(errors.New("expected request_id"))
	}

	transactionID := md.Get("transaction_id")[0]

	switch req.Transaction.Step {
	case gitalypb.Transaction_PRECOMMIT:
		err := preCommit(ctx, req)
		if err != nil {
			return nil, err
		}

		rollback, err := rollbackRef(req)
		if err != nil {
			return nil, err
		}

		s.transactions.Start(transactionID, rollback)
	case gitalypb.Transaction_COMMIT:
		if err := writeRef(ctx, req); err != nil {
			return nil, helper.ErrInternal(err)
		}

		if err := s.transactions.Commit(transactionID); err != nil {
			return nil, err
		}
	case gitalypb.Transaction_ROLLBACK:
		if err := s.transactions.Rollback(transactionID); err != nil {
			return nil, err
		}
	default:
		return nil, errors.New("unknown transaction step")
	}

	return &resp, nil
}

func preCommit(ctx context.Context, req *gitalypb.WriteRefTxRequest) error {
	if req.GetOldRevision() == nil {
		return nil
	}

	cmd, err := git.SafeCmd(ctx, req.GetRepository(), nil, git.SubCmd{
		Name: "rev-parse",
		Args: []string{string(req.GetRef())},
	})
	if err != nil {
		return err
	}

	if err := cmd.Wait(); err != nil {
		return err
	}

	rev, err := ioutil.ReadAll(cmd)
	if err != nil {
		return err
	}

	if text.ChompBytes(rev) != string(req.GetOldRevision()) {
		return errors.New("not ready")
	}

	return nil
}

func rollbackSymbolicRef(req *gitalypb.WriteRefTxRequest) (*command.Command, error) {
	return git.SafeCmd(context.Background(), req.GetRepository(), nil, git.SubCmd{Name: "symbolic-ref", Args: []string{string(req.GetRef()), string(req.GetOldRevision())}})
}

func rollbackRef(req *gitalypb.WriteRefTxRequest) (command.Cmd, error) {
	if string(req.Ref) == "HEAD" {
		return rollbackSymbolicRef(req)
	}

	repoPath, err := helper.GetRepoPath(req.GetRepository())
	if err != nil {
		return nil, err
	}

	args := []string{"-C", repoPath, "update-ref"}

	if req.GetOldRevision() == nil {
		args = append(args, "-d", string(req.GetRevision()))
	} else {
		args = append(args, string(req.GetOldRevision()), string(req.GetRevision()))
	}

	c := exec.Command("git", args...)

	return c, nil
}
