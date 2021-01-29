package git2go

import (
	"context"
	"time"

	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
)

type RevertCommand struct {
	// Repository is the path to execute the revert in.
	Repository string `json:"repository"`
	// AuthorName is the author name of revert commit.
	AuthorName string `json:"author_name"`
	// AuthorMail is the author mail of revert commit.
	AuthorMail string `json:"author_mail"`
	// AuthorDate is the author date of revert commit.
	AuthorDate time.Time `json:"author_date"`
	// Message is the message to be used for the revert commit.
	Message string `json:"message"`
	// Ours is the commit that the revert is applied to.
	Ours string `json:"ours"`
	// Revert is the commit to be reverted.
	Revert string `json:"revert"`
	// Mainline is the parent to be considered the mainline
	Mainline uint `json:"mainline"`
}

func (r RevertCommand) Run(ctx context.Context, cfg config.Cfg) (string, error) {
	return runWithGob(ctx, cfg, "revert", r)
}
