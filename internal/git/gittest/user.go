package gittest

import (
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

const (
	// GlID is the ID of the default user.
	GlID = "user-123"
)

var (
	// TestUser is the default user for tests.
	TestUser = &gitalypb.User{
		Name:       []byte("Jane Doe"),
		Email:      []byte("janedoe@gitlab.com"),
		GlId:       GlID,
		GlUsername: "janedoe",
	}
)
