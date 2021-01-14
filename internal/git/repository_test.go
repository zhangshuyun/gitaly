package git

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

const (
	MasterID      = "1e292f8fedd741b75372e19097c76d327140c312"
	NonexistentID = "ba4f184e126b751d1bffad5897f263108befc780"
)

func TestLocalRepository(t *testing.T) {
	TestRepository(t, func(t testing.TB, pbRepo *gitalypb.Repository) Repository {
		t.Helper()
		return NewRepository(pbRepo)
	})
}

func TestLocalRepository_ContainsRef(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	testRepo, _, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()

	repo := NewRepository(testRepo)

	testcases := []struct {
		desc      string
		ref       string
		contained bool
	}{
		{
			desc:      "unqualified master branch",
			ref:       "master",
			contained: true,
		},
		{
			desc:      "fully qualified master branch",
			ref:       "refs/heads/master",
			contained: true,
		},
		{
			desc:      "nonexistent branch",
			ref:       "refs/heads/nonexistent",
			contained: false,
		},
	}

	for _, tc := range testcases {
		t.Run(tc.desc, func(t *testing.T) {
			contained, err := repo.HasRevision(ctx, Revision(tc.ref))
			require.NoError(t, err)
			require.Equal(t, tc.contained, contained)
		})
	}
}

func TestLocalRepository_GetReference(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	testRepo, _, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()

	repo := NewRepository(testRepo)

	testcases := []struct {
		desc     string
		ref      string
		expected Reference
	}{
		{
			desc:     "fully qualified master branch",
			ref:      "refs/heads/master",
			expected: NewReference("refs/heads/master", MasterID),
		},
		{
			desc:     "unqualified master branch fails",
			ref:      "master",
			expected: Reference{},
		},
		{
			desc:     "nonexistent branch",
			ref:      "refs/heads/nonexistent",
			expected: Reference{},
		},
		{
			desc:     "nonexistent branch",
			ref:      "nonexistent",
			expected: Reference{},
		},
	}

	for _, tc := range testcases {
		t.Run(tc.desc, func(t *testing.T) {
			ref, err := repo.GetReference(ctx, ReferenceName(tc.ref))
			if tc.expected.Name == "" {
				require.True(t, errors.Is(err, ErrReferenceNotFound))
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.expected, ref)
			}
		})
	}
}

func TestLocalRepository_GetReferences(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	testRepo, _, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()

	repo := NewRepository(testRepo)

	testcases := []struct {
		desc    string
		pattern string
		match   func(t *testing.T, refs []Reference)
	}{
		{
			desc:    "master branch",
			pattern: "refs/heads/master",
			match: func(t *testing.T, refs []Reference) {
				require.Equal(t, []Reference{
					NewReference("refs/heads/master", MasterID),
				}, refs)
			},
		},
		{
			desc:    "all references",
			pattern: "",
			match: func(t *testing.T, refs []Reference) {
				require.Len(t, refs, 94)
			},
		},
		{
			desc:    "branches",
			pattern: "refs/heads/",
			match: func(t *testing.T, refs []Reference) {
				require.Len(t, refs, 91)
			},
		},
		{
			desc:    "branches",
			pattern: "refs/heads/nonexistent",
			match: func(t *testing.T, refs []Reference) {
				require.Empty(t, refs)
			},
		},
	}

	for _, tc := range testcases {
		t.Run(tc.desc, func(t *testing.T) {
			refs, err := repo.GetReferences(ctx, tc.pattern)
			require.NoError(t, err)
			tc.match(t, refs)
		})
	}
}

type ReaderFunc func([]byte) (int, error)

func (fn ReaderFunc) Read(b []byte) (int, error) { return fn(b) }

func TestLocalRepository_WriteBlob(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	pbRepo, repoPath, clean := testhelper.InitBareRepo(t)
	defer clean()

	// write attributes file so we can verify WriteBlob runs the files through filters as
	// appropriate
	require.NoError(t, ioutil.WriteFile(filepath.Join(repoPath, "info", "attributes"), []byte(`
crlf binary
lf   text
	`), os.ModePerm))

	repo := NewRepository(pbRepo)

	for _, tc := range []struct {
		desc    string
		path    string
		input   io.Reader
		sha     string
		error   error
		content string
	}{
		{
			desc:  "error reading",
			input: ReaderFunc(func([]byte) (int, error) { return 0, assert.AnError }),
			error: fmt.Errorf("%w, stderr: %q", assert.AnError, []byte{}),
		},
		{
			desc:    "successful empty blob",
			input:   strings.NewReader(""),
			sha:     "e69de29bb2d1d6434b8b29ae775ad8c2e48c5391",
			content: "",
		},
		{
			desc:    "successful blob",
			input:   strings.NewReader("some content"),
			sha:     "f0eec86f614944a81f87d879ebdc9a79aea0d7ea",
			content: "some content",
		},
		{
			desc:    "line endings not normalized",
			path:    "crlf",
			input:   strings.NewReader("\r\n"),
			sha:     "d3f5a12faa99758192ecc4ed3fc22c9249232e86",
			content: "\r\n",
		},
		{
			desc:    "line endings normalized",
			path:    "lf",
			input:   strings.NewReader("\r\n"),
			sha:     "8b137891791fe96927ad78e64b0aad7bded08bdc",
			content: "\n",
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			sha, err := repo.WriteBlob(ctx, tc.path, tc.input)
			require.Equal(t, tc.error, err)
			if tc.error != nil {
				return
			}

			assert.Equal(t, tc.sha, sha)
			content, err := repo.ReadObject(ctx, sha)
			require.NoError(t, err)
			assert.Equal(t, tc.content, string(content))
		})
	}
}

func TestLocalRepository_FormatTag(t *testing.T) {
	for _, tc := range []struct {
		desc       string
		objectID   string
		objectType string
		tagName    []byte
		userName   []byte
		userEmail  []byte
		tagBody    []byte
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
	} {
		t.Run(tc.desc, func(t *testing.T) {
			signature, err := FormatTag(tc.objectID, tc.objectType, tc.tagName, tc.userName, tc.userEmail, tc.tagBody)
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

func TestLocalRepository_WriteTag(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	pbRepo, repoPath, clean := testhelper.NewTestRepo(t)
	defer clean()

	repo := NewRepository(pbRepo)

	for _, tc := range []struct {
		desc       string
		objectID   string
		objectType string
		tagName    []byte
		userName   []byte
		userEmail  []byte
		tagBody    []byte
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
	} {
		t.Run(tc.desc, func(t *testing.T) {
			tagObjID, err := repo.WriteTag(ctx, tc.objectID, tc.objectType, tc.tagName, tc.userName, tc.userEmail, tc.tagBody)
			require.NoError(t, err)

			repoTagObjID := testhelper.MustRunCommand(t, nil, "git", "-C", repoPath, "rev-parse", tagObjID)
			require.Equal(t, text.ChompBytes(repoTagObjID), tagObjID)
		})
	}
}

func TestLocalRepository_ReadObject(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	testRepo, _, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()

	repo := NewRepository(testRepo)

	for _, tc := range []struct {
		desc    string
		oid     string
		content string
		error   error
	}{
		{
			desc:  "invalid object",
			oid:   NullSHA,
			error: InvalidObjectError(NullSHA),
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

func TestLocalRepository_GetBranches(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	testRepo, _, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()

	repo := NewRepository(testRepo)

	refs, err := repo.GetBranches(ctx)
	require.NoError(t, err)
	require.Len(t, refs, 91)
}

func TestLocalRepository_UpdateRef(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	testRepo, _, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()

	defer func(oldValue string) {
		config.Config.Ruby.Dir = oldValue
	}(config.Config.Ruby.Dir)
	config.Config.Ruby.Dir = "/var/empty"

	otherRef, err := NewRepository(testRepo).GetReference(ctx, "refs/heads/gitaly-test-ref")
	require.NoError(t, err)

	testcases := []struct {
		desc   string
		ref    string
		newrev string
		oldrev string
		verify func(t *testing.T, repo *LocalRepository, err error)
	}{
		{
			desc:   "successfully update master",
			ref:    "refs/heads/master",
			newrev: otherRef.Target,
			oldrev: MasterID,
			verify: func(t *testing.T, repo *LocalRepository, err error) {
				require.NoError(t, err)
				ref, err := repo.GetReference(ctx, "refs/heads/master")
				require.NoError(t, err)
				require.Equal(t, ref.Target, otherRef.Target)
			},
		},
		{
			desc:   "update fails with stale oldrev",
			ref:    "refs/heads/master",
			newrev: otherRef.Target,
			oldrev: NonexistentID,
			verify: func(t *testing.T, repo *LocalRepository, err error) {
				require.Error(t, err)
				ref, err := repo.GetReference(ctx, "refs/heads/master")
				require.NoError(t, err)
				require.Equal(t, ref.Target, MasterID)
			},
		},
		{
			desc:   "update fails with invalid newrev",
			ref:    "refs/heads/master",
			newrev: NonexistentID,
			oldrev: MasterID,
			verify: func(t *testing.T, repo *LocalRepository, err error) {
				require.Error(t, err)
				ref, err := repo.GetReference(ctx, "refs/heads/master")
				require.NoError(t, err)
				require.Equal(t, ref.Target, MasterID)
			},
		},
		{
			desc:   "successfully update master with empty oldrev",
			ref:    "refs/heads/master",
			newrev: otherRef.Target,
			oldrev: "",
			verify: func(t *testing.T, repo *LocalRepository, err error) {
				require.NoError(t, err)
				ref, err := repo.GetReference(ctx, "refs/heads/master")
				require.NoError(t, err)
				require.Equal(t, ref.Target, otherRef.Target)
			},
		},
		{
			desc:   "updating unqualified branch fails",
			ref:    "master",
			newrev: otherRef.Target,
			oldrev: MasterID,
			verify: func(t *testing.T, repo *LocalRepository, err error) {
				require.Error(t, err)
				ref, err := repo.GetReference(ctx, "refs/heads/master")
				require.NoError(t, err)
				require.Equal(t, ref.Target, MasterID)
			},
		},
		{
			desc:   "deleting master succeeds",
			ref:    "refs/heads/master",
			newrev: strings.Repeat("0", 40),
			oldrev: MasterID,
			verify: func(t *testing.T, repo *LocalRepository, err error) {
				require.NoError(t, err)
				_, err = repo.GetReference(ctx, "refs/heads/master")
				require.Error(t, err)
			},
		},
		{
			desc:   "creating new branch succeeds",
			ref:    "refs/heads/new",
			newrev: MasterID,
			oldrev: strings.Repeat("0", 40),
			verify: func(t *testing.T, repo *LocalRepository, err error) {
				require.NoError(t, err)
				ref, err := repo.GetReference(ctx, "refs/heads/new")
				require.NoError(t, err)
				require.Equal(t, ref.Target, MasterID)
			},
		},
	}

	for _, tc := range testcases {
		t.Run(tc.desc, func(t *testing.T) {
			// Re-create repo for each testcase.
			testRepo, _, cleanup := testhelper.NewTestRepo(t)
			defer cleanup()

			repo := NewRepository(testRepo)
			err := repo.UpdateRef(ctx, ReferenceName(tc.ref), tc.newrev, tc.oldrev)

			tc.verify(t, repo, err)
		})
	}
}

func TestLocalRepository_FetchRemote(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	_, remoteRepoPath, cleanup := testhelper.NewTestRepo(t)
	defer cleanup()

	initBareWithRemote := func(t *testing.T, remote string) (*LocalRepository, string, testhelper.Cleanup) {
		t.Helper()

		testRepo, testRepoPath, cleanup := testhelper.InitBareRepo(t)

		cmd := exec.Command(config.Config.Git.BinPath, "-C", testRepoPath, "remote", "add", remote, remoteRepoPath)
		err := cmd.Run()
		if err != nil {
			cleanup()
			t.Log(err)
			t.FailNow()
		}

		return NewRepository(testRepo), testRepoPath, cleanup
	}

	defer func(oldValue string) {
		config.Config.Ruby.Dir = oldValue
	}(config.Config.Ruby.Dir)
	config.Config.Ruby.Dir = "/var/empty"

	t.Run("invalid name", func(t *testing.T) {
		repo := NewRepository(nil)

		err := repo.FetchRemote(ctx, " ", FetchOpts{})
		require.True(t, errors.Is(err, ErrInvalidArg))
		require.Contains(t, err.Error(), `"remoteName" is blank or empty`)
	})

	t.Run("unknown remote", func(t *testing.T) {
		testRepo, _, cleanup := testhelper.InitBareRepo(t)
		defer cleanup()

		repo := NewRepository(testRepo)
		var stderr bytes.Buffer
		err := repo.FetchRemote(ctx, "stub", FetchOpts{Stderr: &stderr})
		require.Error(t, err)
		require.Contains(t, stderr.String(), "'stub' does not appear to be a git repository")
	})

	t.Run("ok", func(t *testing.T) {
		repo, testRepoPath, cleanup := initBareWithRemote(t, "origin")
		defer cleanup()

		var stderr bytes.Buffer
		require.NoError(t, repo.FetchRemote(ctx, "origin", FetchOpts{Stderr: &stderr}))

		require.Empty(t, stderr.String(), "it should not produce output as it is called with --quite flag by default")

		fetchHeadData, err := ioutil.ReadFile(filepath.Join(testRepoPath, "FETCH_HEAD"))
		require.NoError(t, err, "it should create FETCH_HEAD with info about fetch")

		fetchHead := string(fetchHeadData)
		require.Contains(t, fetchHead, "e56497bb5f03a90a51293fc6d516788730953899	not-for-merge	branch ''test''")
		require.Contains(t, fetchHead, "8a2a6eb295bb170b34c24c76c49ed0e9b2eaf34b	not-for-merge	tag 'v1.1.0'")

		sha, err := repo.ResolveRevision(ctx, Revision("refs/remotes/origin/master^{commit}"))
		require.NoError(t, err, "the object from remote should exists in local after fetch done")
		require.Equal(t, "1e292f8fedd741b75372e19097c76d327140c312", sha)
	})

	t.Run("with env", func(t *testing.T) {
		_, sourceRepoPath, sourceCleanup := testhelper.NewTestRepo(t)
		defer sourceCleanup()

		testRepo, testRepoPath, testCleanup := testhelper.NewTestRepo(t)
		defer testCleanup()

		repo := NewRepository(testRepo)
		testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "remote", "add", "source", sourceRepoPath)

		var stderr bytes.Buffer
		require.NoError(t, repo.FetchRemote(ctx, "source", FetchOpts{Stderr: &stderr, Env: []string{"GIT_TRACE=1"}}))
		require.Contains(t, stderr.String(), "trace: built-in: git fetch --quiet source --end-of-options")
	})

	t.Run("with globals", func(t *testing.T) {
		_, sourceRepoPath, sourceCleanup := testhelper.NewTestRepo(t)
		defer sourceCleanup()

		testRepo, testRepoPath, testCleanup := testhelper.NewTestRepo(t)
		defer testCleanup()

		repo := NewRepository(testRepo)
		testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "remote", "add", "source", sourceRepoPath)

		require.NoError(t, repo.FetchRemote(ctx, "source", FetchOpts{}))

		testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "branch", "--track", "testing-fetch-prune", "refs/remotes/source/markdown")
		testhelper.MustRunCommand(t, nil, "git", "-C", sourceRepoPath, "branch", "-D", "markdown")

		require.NoError(t, repo.FetchRemote(
			ctx,
			"source",
			FetchOpts{
				Global: []GlobalOption{ConfigPair{Key: "fetch.prune", Value: "true"}},
			}),
		)

		contains, err := repo.HasRevision(ctx, Revision("refs/remotes/source/markdown"))
		require.NoError(t, err)
		require.False(t, contains, "remote tracking branch should be pruned as it no longer exists on the remote")
	})

	t.Run("with prune", func(t *testing.T) {
		_, sourceRepoPath, sourceCleanup := testhelper.NewTestRepo(t)
		defer sourceCleanup()

		testRepo, testRepoPath, testCleanup := testhelper.NewTestRepo(t)
		defer testCleanup()

		repo := NewRepository(testRepo)

		testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "remote", "add", "source", sourceRepoPath)
		require.NoError(t, repo.FetchRemote(ctx, "source", FetchOpts{}))

		testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "branch", "--track", "testing-fetch-prune", "refs/remotes/source/markdown")
		testhelper.MustRunCommand(t, nil, "git", "-C", sourceRepoPath, "branch", "-D", "markdown")

		require.NoError(t, repo.FetchRemote(ctx, "source", FetchOpts{Prune: true}))

		contains, err := repo.HasRevision(ctx, Revision("refs/remotes/source/markdown"))
		require.NoError(t, err)
		require.False(t, contains, "remote tracking branch should be pruned as it no longer exists on the remote")
	})

	t.Run("with no tags", func(t *testing.T) {
		repo, testRepoPath, cleanup := initBareWithRemote(t, "origin")
		defer cleanup()

		tagsBefore := testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "tag", "--list")
		require.Empty(t, tagsBefore)

		require.NoError(t, repo.FetchRemote(ctx, "origin", FetchOpts{Tags: FetchOptsTagsNone, Force: true}))

		tagsAfter := testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "tag", "--list")
		require.Empty(t, tagsAfter)

		containsBranches, err := repo.HasRevision(ctx, Revision("'test'"))
		require.NoError(t, err)
		require.False(t, containsBranches)

		containsTags, err := repo.HasRevision(ctx, Revision("v1.1.0"))
		require.NoError(t, err)
		require.False(t, containsTags)
	})
}
