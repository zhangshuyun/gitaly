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
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, helper.ErrInternal(errors.New("could not get metadata from context"))
	}

	transactionSteps := md.Get("transaction_step")
	if len(transactionSteps) == 0 {
		return nil, nil
	}

	transactionStep := transactionSteps[len(transactionSteps)-1]

	if transactionStep == "precommit" {
		if err := validateWriteRefRequest(req); err != nil {
			return nil, helper.ErrInvalidArgument(err)
		}
	}

	return s.transactionalWriteRef(ctx, req, transactionStep)
}

func (s *server) transactionalWriteRef(ctx context.Context, req *gitalypb.WriteRefTxRequest, transactionStep string) (*gitalypb.WriteRefTxResponse, error) {
	var resp gitalypb.WriteRefTxResponse

	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, helper.ErrInternal(errors.New("couldn't get metadata"))
	}
	if len(md.Get("transaction_id")) == 0 {
		return nil, helper.ErrInternal(errors.New("expected request_id"))
	}

	transactionID := md.Get("transaction_id")[0]

	switch transactionStep {
	case "prepare":
		err := prepare(ctx, req)
		if err != nil {
			return nil, err
		}

		s.transactions.Start(transactionID)
	case "precommit":
		rollback, err := rollbackRef(req)
		if err != nil {
			return nil, err
		}
		commit, err := preCommit(req)
		if err != nil {
			return nil, err
		}

		s.transactions.SetRollback(transactionID, rollback)
		s.transactions.PreCommit(transactionID, commit)
	case "commit":
		if err := s.transactions.Commit(transactionID); err != nil {
			return nil, err
		}
	case "rollback":
		if err := s.transactions.Rollback(transactionID); err != nil {
			return nil, err
		}
	default:
		return nil, errors.New("unknown transaction step")
	}

	return &resp, nil
}

func prepare(ctx context.Context, req *gitalypb.WriteRefTxRequest) error {
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

func rollbackSymbolicRef(req *gitalypb.WriteRefTxRequest) (command.Cmd, error) {
	repoPath, err := helper.GetRepoPath(req.GetRepository())
	if err != nil {
		return nil, err
	}

	return exec.Command("git", "-C", repoPath, "symoblic-ref", string(req.GetRef()), string(req.GetOldRevision())), nil
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

func preCommit(req *gitalypb.WriteRefTxRequest) (command.Cmd, error) {
	if string(req.Ref) == "HEAD" {
		return preCommitSymbolicRef(req)
	}

	repoPath, err := helper.GetRepoPath(req.GetRepository())
	if err != nil {
		return nil, err
	}

	args := []string{"-C", repoPath, "update-ref", string(req.GetRevision())}

	if req.GetOldRevision() != nil {
		args = append(args, string(req.GetOldRevision()))
	}

	c := exec.Command("git", args...)

	return c, nil
}

func preCommitSymbolicRef(req *gitalypb.WriteRefTxRequest) (command.Cmd, error) {
	repoPath, err := helper.GetRepoPath(req.GetRepository())
	if err != nil {
		return nil, err
	}

	return exec.Command("git", "-C", repoPath, "symoblic-ref", string(req.GetRef()), string(req.GetRevision())), nil
}
