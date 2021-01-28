package localrepo

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
)

type ReaderFunc func([]byte) (int, error)

func (fn ReaderFunc) Read(b []byte) (int, error) { return fn(b) }

func TestRepo_WriteBlob(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	pbRepo, repoPath, clean := testhelper.InitBareRepo(t)
	defer clean()

	repo := New(pbRepo, config.Config)

	for _, tc := range []struct {
		desc       string
		attributes string
		input      io.Reader
		sha        string
		error      error
		content    string
	}{
		{
			desc:  "error reading",
			input: ReaderFunc(func([]byte) (int, error) { return 0, assert.AnError }),
			error: fmt.Errorf("%w, stderr: %q", assert.AnError, []byte{}),
		},
		{
			desc:    "successful empty blob",
			input:   strings.NewReader(""),
			content: "",
		},
		{
			desc:    "successful blob",
			input:   strings.NewReader("some content"),
			content: "some content",
		},
		{
			desc:    "LF line endings left unmodified",
			input:   strings.NewReader("\n"),
			content: "\n",
		},
		{
			desc:    "CRLF converted to LF due to global git config",
			input:   strings.NewReader("\r\n"),
			content: "\n",
		},
		{
			desc:       "line endings preserved in binary files",
			input:      strings.NewReader("\r\n"),
			attributes: "file-path binary",
			content:    "\r\n",
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			require.NoError(t,
				ioutil.WriteFile(filepath.Join(repoPath, "info", "attributes"), []byte(tc.attributes), os.ModePerm),
			)

			sha, err := repo.WriteBlob(ctx, "file-path", tc.input)
			require.Equal(t, tc.error, err)
			if tc.error != nil {
				return
			}

			content, err := repo.ReadObject(ctx, sha)
			require.NoError(t, err)
			assert.Equal(t, tc.content, string(content))
		})
	}
}

func TestFormatTag(t *testing.T) {
	for _, tc := range []struct {
		desc       string
		objectID   string
		objectType string
		tagName    []byte
		userName   []byte
		userEmail  []byte
		tagBody    []byte
		authorDate time.Time
		err        error
	}{
		// Just trivial tests here, most of this is tested in
		// internal/gitaly/service/operations/tags_test.go
		{
			desc:       "basic signature",
			objectID:   "0000000000000000000000000000000000000000",
			objectType: "commit",
			tagName:    []byte("my-tag"),
			userName:   []byte("root"),
			userEmail:  []byte("root@localhost"),
			tagBody:    []byte(""),
		},
		{
			desc:       "basic signature",
			objectID:   "0000000000000000000000000000000000000000",
			objectType: "commit",
			tagName:    []byte("my-tag\ninjection"),
			userName:   []byte("root"),
			userEmail:  []byte("root@localhost"),
			tagBody:    []byte(""),
			err:        FormatTagError{expectedLines: 4, actualLines: 5},
		},
		{
			desc:       "signature with fixed time",
			objectID:   "0000000000000000000000000000000000000000",
			objectType: "commit",
			tagName:    []byte("my-tag"),
			userName:   []byte("root"),
			userEmail:  []byte("root@localhost"),
			tagBody:    []byte(""),
			authorDate: time.Unix(12345, 0),
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			signature, err := FormatTag(tc.objectID, tc.objectType, tc.tagName, tc.userName, tc.userEmail, tc.tagBody, tc.authorDate)
			if err != nil {
				require.Equal(t, tc.err, err)
				require.Equal(t, "", signature)
			} else {
				require.NoError(t, err)
				require.Contains(t, signature, "object ")
				require.Contains(t, signature, "tag ")
				require.Contains(t, signature, "tagger ")
			}
		})
	}
}

func TestRepo_WriteTag(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	pbRepo, repoPath, clean := testhelper.NewTestRepo(t)
	defer clean()

	repo := New(pbRepo, config.Config)

	for _, tc := range []struct {
		desc       string
		objectID   string
		objectType string
		tagName    []byte
		userName   []byte
		userEmail  []byte
		tagBody    []byte
		authorDate time.Time
	}{
		// Just trivial tests here, most of this is tested in
		// internal/gitaly/service/operations/tags_test.go
		{
			desc:       "basic signature",
			objectID:   "c7fbe50c7c7419d9701eebe64b1fdacc3df5b9dd",
			objectType: "commit",
			tagName:    []byte("my-tag"),
			userName:   []byte("root"),
			userEmail:  []byte("root@localhost"),
			tagBody:    []byte(""),
		},
		{
			desc:       "signature with time",
			objectID:   "c7fbe50c7c7419d9701eebe64b1fdacc3df5b9dd",
			objectType: "commit",
			tagName:    []byte("tag-with-timestamp"),
			userName:   []byte("root"),
			userEmail:  []byte("root@localhost"),
			tagBody:    []byte(""),
			authorDate: time.Unix(12345, 0),
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			tagObjID, err := repo.WriteTag(ctx, tc.objectID, tc.objectType, tc.tagName, tc.userName, tc.userEmail, tc.tagBody, tc.authorDate)
			require.NoError(t, err)

			repoTagObjID := testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "rev-parse", tagObjID)
			require.Equal(t, text.ChompBytes(repoTagObjID), tagObjID)
		})
	}
}

func TestRepo_ReadObject(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	testRepo, _, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()

	repo := New(testRepo, config.Config)

	for _, tc := range []struct {
		desc    string
		oid     string
		content string
		error   error
	}{
		{
			desc:  "invalid object",
			oid:   git.ZeroOID.String(),
			error: InvalidObjectError(git.ZeroOID.String()),
		},
		{
			desc: "valid object",
			// README in gitlab-test
			oid:     "3742e48c1108ced3bf45ac633b34b65ac3f2af04",
			content: "Sample repo for testing gitlab features\n",
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			content, err := repo.ReadObject(ctx, tc.oid)
			require.Equal(t, tc.error, err)
			require.Equal(t, tc.content, string(content))
		})
	}
}
