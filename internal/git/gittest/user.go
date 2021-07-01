package gittest

import (
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

const (
	// GlID is the ID of the default user.
	GlID = "user-123"

	// Timezone is the Timezone of the default user.
	Timezone = "Asia/Shanghai"
	// TimezoneOffset is ISO 8601-like format of the default user Timezone.
	TimezoneOffset = "+0800"
)

var (
	// TestUser is the default user for tests.
	TestUser = &gitalypb.User{
		Name:       []byte("Jane Doe"),
		Email:      []byte("janedoe@gitlab.com"),
		GlId:       GlID,
		GlUsername: "janedoe",
		Timezone:   Timezone,
	}
)
