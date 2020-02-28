package repository

import (
	"context"
	"errors"
	"io/ioutil"
	"os/exec"

	"github.com/sirupsen/logrus"
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

	logrus.WithField("transaction_steps", transactionSteps).Info("got transaction steps")
	if len(transactionSteps) == 0 {
		return nil, nil
	}

	logrus.WithField("transaction_steps", transactionSteps).Info("got transaction steps")

	transactionStep := transactionSteps[len(transactionSteps)-1]

	if transactionStep == "precommit" {
		logrus.WithField("relative_path", req.GetRepository().GetRelativePath()).WithField("ref", string(req.GetRef())).WithField("rev", string(req.GetRevision())).WithField("old_rev", string(req.GetOldRevision())).Info("WriteRefReqTx")
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
	case "vote":
		logrus.WithField("transaction_step", "vote").Info("transaction!")

		s.transactions.Begin(transactionID)
		err := vote(ctx, req)
		if err != nil {
			return nil, err
		}
	case "precommit":
		logrus.WithField("transaction_step", "precommit").Info("transaction!")
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
		logrus.WithField("transaction_step", "commit").Info("transaction!")
		if err := s.transactions.Commit(transactionID); err != nil {
			return nil, err
		}
	case "rollback":
		logrus.WithField("transaction_step", "rollback").Info("transaction!")
		if err := s.transactions.Rollback(transactionID); err != nil {
			return nil, err
		}
	default:
		return nil, errors.New("unknown transaction step")
	}

	return &resp, nil
}

func vote(ctx context.Context, req *gitalypb.WriteRefTxRequest) error {
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

func rollbackSymbolicRef(req *gitalypb.WriteRefTxRequest) (*exec.Cmd, error) {
	repoPath, err := helper.GetRepoPath(req.GetRepository())
	if err != nil {
		return nil, err
	}

	return exec.Command("git", "-C", repoPath, "symoblic-ref", string(req.GetRef()), string(req.GetOldRevision())), nil
}

func rollbackRef(req *gitalypb.WriteRefTxRequest) (*exec.Cmd, error) {
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

func preCommit(req *gitalypb.WriteRefTxRequest) (*exec.Cmd, error) {
	if string(req.Ref) == "HEAD" {
		return preCommitSymbolicRef(req)
	}

	repoPath, err := helper.GetRepoPath(req.GetRepository())
	if err != nil {
		return nil, err
	}

	args := []string{"-C", repoPath, "update-ref", string(req.GetRef()), string(req.GetRevision())}

	if req.GetOldRevision() != nil {
		args = append(args, string(req.GetOldRevision()))
	}

	c := exec.Command("git", args...)

	logrus.WithField("DA COMMAND ARGS!!!", args).Info("1234567")

	return c, nil
}

func preCommitSymbolicRef(req *gitalypb.WriteRefTxRequest) (*exec.Cmd, error) {
	repoPath, err := helper.GetRepoPath(req.GetRepository())
	if err != nil {
		return nil, err
	}

	return exec.Command("git", "-C", repoPath, "symoblic-ref", string(req.GetRef()), string(req.GetRevision())), nil
}
