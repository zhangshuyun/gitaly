package ref

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

func TestPackRefsSuccessfulRequest(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	stop, serverSocketPath := runRefServiceServer(t)
	defer stop()

	client, conn := newRefServiceClient(t, serverSocketPath)
	defer conn.Close()

	testRepo, testRepoPath, cleanupFn := testhelper.NewTestRepo(t)
	defer cleanupFn()

	packedRefs := linesInPackfile(t, testRepoPath)

	// creates some new heads
	newBranches := 10
	for i := 0; i < newBranches; i++ {
		testhelper.CreateLooseRef(t, testRepoPath, fmt.Sprintf("new-ref-%d", i))
	}

	// pack all refs
	_, err := client.PackRefs(ctx, &gitalypb.PackRefsRequest{Repository: testRepo})
	require.NoError(t, err)

	files, err := ioutil.ReadDir(filepath.Join(testRepoPath, "refs/heads"))
	require.NoError(t, err)
	assert.Len(t, files, 0, "git pack-refs --all should have packed all refs in refs/heads")
	assert.Equal(t, packedRefs+newBranches, linesInPackfile(t, testRepoPath), fmt.Sprintf("should have added %d new lines to the packfile", newBranches))

	// ensure all refs are reachable
	for i := 0; i < newBranches; i++ {
		testhelper.MustRunCommand(t, nil, "git", "-C", testRepoPath, "show-ref", fmt.Sprintf("refs/heads/new-ref-%d", i))
	}
}

func linesInPackfile(t *testing.T, repoPath string) int {
	packFile, err := os.Open(filepath.Join(repoPath, "packed-refs"))
	require.NoError(t, err)
	defer packFile.Close()
	scanner := bufio.NewScanner(packFile)
	var refs int
	for scanner.Scan() {
		if strings.HasPrefix(scanner.Text(), "#") {
			continue
		}
		refs++
	}
	return refs
}
