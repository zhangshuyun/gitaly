package git

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
)

func TestCheckRefFormat(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	for _, tc := range []struct {
		desc    string
		tagName string
		ok      bool
		err     error
	}{
		// Just trivial tests here, most of this is tested in
		// internal/gitaly/service/operations/tags_test.go
		{
			desc:    "unqualified name",
			tagName: "my-name",
			ok:      false,
			err:     CheckRefFormatError{},
		},
		{
			desc:    "fully-qualified name",
			tagName: "refs/heads/my-name",
			ok:      true,
			err:     nil,
		},
		{
			desc:    "basic tag",
			tagName: "refs/tags/my-tag",
			ok:      true,
			err:     nil,
		},
		{
			desc:    "invalid tag",
			tagName: "refs/tags/my tag",
			ok:      false,
			err:     CheckRefFormatError{},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			ok, err := CheckRefFormat(ctx, tc.tagName)
			require.Equal(t, tc.err, err)
			require.Equal(t, tc.ok, ok)
		})
	}
}
