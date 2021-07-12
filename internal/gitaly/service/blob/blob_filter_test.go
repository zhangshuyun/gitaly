package blob

import (
	"bytes"
	"errors"
	"io/ioutil"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/localrepo"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"golang.org/x/text/transform"
)

func TestBlobFilter(t *testing.T) {
	cfg, repo, _, _ := setup(t)
	localRepo := localrepo.NewTestRepo(t, cfg, repo)

	ctx, cancel := testhelper.Context()
	defer cancel()

	var batchCheckOutput bytes.Buffer
	err := localRepo.ExecAndWait(ctx, git.SubCmd{
		Name: "cat-file",
		Flags: []git.Option{
			git.Flag{Name: "--batch-all-objects"},
			git.Flag{Name: "--batch-check=%(objecttype) %(objectsize) %(objectname)"},
		},
	}, git.WithStdout(&batchCheckOutput))
	require.NoError(t, err)

	// We're manually reimplementing filtering of objects here. While this may seem hacky, it
	// asserts that we can parse largish output directly produced by the command. Also, given
	// that the system under test is implemented completely different, it cross-checks both
	// implementations against each other.
	var expectedOIDs []string
	for _, line := range strings.Split(batchCheckOutput.String(), "\n") {
		if len(line) == 0 {
			continue
		}
		objectInfo := strings.SplitN(line, " ", 3)
		require.Len(t, objectInfo, 3)
		objectSize, err := strconv.Atoi(objectInfo[1])
		require.NoError(t, err)

		if objectInfo[0] == "blob" && objectSize <= lfsPointerMaxSize {
			expectedOIDs = append(expectedOIDs, objectInfo[2])
		}
	}
	require.Greater(t, len(expectedOIDs), 100)

	for _, tc := range []struct {
		desc           string
		input          string
		maxSize        uint64
		expectedOutput string
		expectedErr    error
	}{
		{
			desc:           "empty",
			input:          "",
			expectedOutput: "",
		},
		{
			desc:        "newline only",
			input:       "\n",
			expectedErr: errors.New("invalid line \"\""),
		},
		{
			desc:        "invalid blob",
			input:       "x",
			expectedErr: errors.New("invalid trailing line"),
		},
		{
			desc:           "single blob",
			input:          "blob 140 1234\n",
			expectedOutput: "1234\n",
		},
		{
			desc:           "multiple blobs",
			input:          "blob 140 1234\nblob 150 4321\n",
			expectedOutput: "1234\n4321\n",
		},
		{
			desc:           "mixed blobs and other objects",
			input:          "blob 140 1234\ntree 150 4321\ncommit 50123 123123\n",
			maxSize:        160,
			expectedOutput: "1234\n",
		},
		{
			desc:           "big blob gets filtered",
			input:          "blob 140 1234\nblob 201 4321\n",
			maxSize:        200,
			expectedOutput: "1234\n",
		},
		{
			desc:        "missing trailing newline",
			input:       "blob 140 1234",
			expectedErr: errors.New("invalid trailing line"),
		},
		{
			desc:        "invalid object size",
			input:       "blob 140 1234\nblob x 4321\n",
			maxSize:     1,
			expectedErr: errors.New("invalid blob size \"x\""),
		},
		{
			desc:        "missing field",
			input:       "blob 1234\n",
			expectedErr: errors.New("invalid line \"blob 1234\""),
		},
		{
			desc:           "real-repo output",
			input:          batchCheckOutput.String(),
			maxSize:        lfsPointerMaxSize,
			expectedOutput: strings.Join(expectedOIDs, "\n") + "\n",
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			reader := transform.NewReader(strings.NewReader(tc.input), blobFilter{
				maxSize: tc.maxSize,
			})
			output, err := ioutil.ReadAll(reader)
			require.Equal(t, tc.expectedErr, err)
			require.Equal(t, tc.expectedOutput, string(output))
		})
	}
}
