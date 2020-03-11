package git

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os/exec"

	"gitlab.com/gitlab-org/gitaly/internal/helper"

	"gitlab.com/gitlab-org/gitaly/internal/git/repository"
	"gitlab.com/gitlab-org/gitaly/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

/*
func init() {
	gob.Register(gitalypb.Repository{})
}

*/

type Transaction struct {
	Args []string
	repo *gitalypb.Repository
}

func NewTransaction(repo *gitalypb.Repository, args []string) *Transaction {
	return &Transaction{
		Args: args,
		repo: repo,
	}
}

func GetCurrentTransactionID(ctx context.Context, repo repository.GitRepo) (string, error) {
	getTxID, err := SafeStdinCmd(ctx, repo, nil, SubCmd{
		Name: "rev-parse",
		Args: []string{"refs/tx/transaction"},
	})
	if err != nil {
		return "", err
	}

	txID, err := ioutil.ReadAll(getTxID)
	if err != nil {
		return "", err
	}

	if err = getTxID.Wait(); err != nil {
		return "", err
	}

	return text.ChompBytes(txID), nil
}

func GetCurrentTransaction(repo *gitalypb.Repository, transactionID string) (*Transaction, error) {
	repoPath, err := helper.GetRepoPath(repo)
	if err != nil {
		return nil, err
	}

	var stdout bytes.Buffer
	c := exec.Command("git", "-C", repoPath, "cat-file", "-p", transactionID)
	c.Stdout = &stdout

	if err := c.Run(); err != nil {
		fmt.Printf("\n\nwhoa %v\n\n", err)
		return nil, err
	}

	var transaction Transaction
	if err := gob.NewDecoder(&stdout).Decode(&transaction); err != nil {
		return nil, err
	}

	transaction.repo = repo

	return &transaction, nil
}

func (t *Transaction) Begin(ctx context.Context) error {
	// TODO: issue here of creating blobs that may never get used
	// we don't want to clutter git's object database with these, even though they should get cleaned up during
	// gc
	repoPath, err := helper.GetRepoPath(t.repo)
	if err != nil {
		return err
	}

	r, w := io.Pipe()

	var stdout, stderr bytes.Buffer

	c := exec.Command("git", "-C", repoPath, "hash-object", "-w", "--stdin")
	c.Stdout = &stdout
	c.Stderr = &stderr
	c.Stdin = r

	if err := c.Start(); err != nil {
		return err
	}

	if err = gob.NewEncoder(w).Encode(t); err != nil {
		return err
	}
	w.Close()

	if err := c.Wait(); err != nil {
		return err
	}
	r.Close()

	blobID := text.ChompBytes(stdout.Bytes())

	lockCmd, err := SafeCmd(ctx, t.repo, nil, SubCmd{
		Name: "update-ref",
		Args: []string{"refs/tx/transaction", blobID, "0000000000000000000000000000000000000000"},
	})
	if err != nil {
		return err
	}

	if err = lockCmd.Wait(); err != nil {
		return err
	}

	return nil
}

func (t *Transaction) Commit(ctx context.Context, transactionID string) error {
	name := t.Args[0]
	var args []string

	if len(t.Args) > 1 {
		args = t.Args[1:]
	}

	txID, err := GetCurrentTransactionID(ctx, t.repo)
	if err != nil {
		return err
	}

	if txID != transactionID {
		return errors.New("you are not allowed to commit this transaction")
	}

	c := exec.Command(name, args...)
	if err := c.Run(); err != nil {
		return err
	}

	repoPath, err := helper.GetRepoPath(t.repo)
	if err != nil {
		return err
	}

	c = exec.Command("git", "-C", repoPath, "update-ref", "-d", "refs/tx/transaction")

	return c.Run()
}
