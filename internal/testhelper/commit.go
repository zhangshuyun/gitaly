package testhelper

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/helper/text"
)

// CreateCommitOpts holds extra options for CreateCommit.
type CreateCommitOpts struct {
	Message  string
	ParentID string
}

const (
	committerName  = "Scrooge McDuck"
	committerEmail = "scrooge@mcduck.com"
)

// CreateCommit makes a new empty commit and updates the named branch to point to it.
func CreateCommit(t testing.TB, repoPath, branchName string, opts *CreateCommitOpts) string {
	message := "message"
	// The ID of an arbitrary commit known to exist in the test repository.
	parentID := "1a0b36b3cdad1d2ee32457c102a8c0b7056fa863"

	if opts != nil {
		if opts.Message != "" {
			message = opts.Message
		}

		if opts.ParentID != "" {
			parentID = opts.ParentID
		}
	}

	// message can be very large, passing it directly in args would blow things up!
	stdin := bytes.NewBufferString(message)

	// Use 'commit-tree' instead of 'commit' because we are in a bare
	// repository. What we do here is the same as "commit -m message
	// --allow-empty".
	commitArgs := []string{
		"-c", fmt.Sprintf("user.name=%s", committerName),
		"-c", fmt.Sprintf("user.email=%s", committerEmail),
		"-C", repoPath,
		"commit-tree", "-F", "-", "-p", parentID, parentID + "^{tree}",
	}
	newCommit := MustRunCommand(t, stdin, "git", commitArgs...)
	newCommitID := text.ChompBytes(newCommit)

	MustRunCommand(t, nil, "git", "-C", repoPath, "update-ref", "refs/heads/"+branchName, newCommitID)
	return newCommitID
}

// CreateCommitInAlternateObjectDirectory runs a command such that its created
// objects will live in an alternate objects directory. It returns the current
// head after the command is run and the alternate objects directory path
func CreateCommitInAlternateObjectDirectory(t testing.TB, repoPath, altObjectsDir string, cmd *exec.Cmd) (currentHead []byte) {
	gitPath := path.Join(repoPath, ".git")

	altObjectsPath := path.Join(gitPath, altObjectsDir)
	gitObjectEnv := []string{
		fmt.Sprintf("GIT_OBJECT_DIRECTORY=%s", altObjectsPath),
		fmt.Sprintf("GIT_ALTERNATE_OBJECT_DIRECTORIES=%s", path.Join(gitPath, "objects")),
	}
	require.NoError(t, os.MkdirAll(altObjectsPath, 0755))

	// Because we set 'gitObjectEnv', the new objects created by this command
	// will go into 'find-commits-alt-test-repo/.git/alt-objects'.
	cmd.Env = append(cmd.Env, gitObjectEnv...)
	if output, err := cmd.Output(); err != nil {
		stderr := err.(*exec.ExitError).Stderr
		t.Fatalf("stdout: %s, stderr: %s", output, stderr)
	}

	cmd = exec.Command("git", "-C", repoPath, "rev-parse", "HEAD")
	cmd.Env = gitObjectEnv
	currentHead, err := cmd.Output()
	require.NoError(t, err)

	return currentHead[:len(currentHead)-1]
}

// CommitBlobWithName will create a commit for the specified blob with the
// specified name. This enables testing situations where the filepath is not
// possible due to filesystem constraints (e.g. non-UTF characters). The commit
// ID is returned.
func CommitBlobWithName(t testing.TB, testRepoPath, blobID, fileName, commitMessage string) string {
	mktreeIn := strings.NewReader(fmt.Sprintf("100644 blob %s\t%s", blobID, fileName))
	treeID := text.ChompBytes(MustRunCommand(t, mktreeIn, "git", "-C", testRepoPath, "mktree"))

	return text.ChompBytes(
		MustRunCommand(t, nil, "git",
			"-c", fmt.Sprintf("user.name=%s", committerName),
			"-c", fmt.Sprintf("user.email=%s", committerEmail),
			"-C", testRepoPath, "commit-tree", treeID, "-m", commitMessage),
	)
}

// CreateCommitOnNewBranch creates a branch and a commit, returning the commit sha and the branch name respectivelyi
func CreateCommitOnNewBranch(t testing.TB, repoPath string) (string, string) {
	nonce, err := text.RandomHex(4)
	require.NoError(t, err)
	newBranch := "branch-" + nonce

	sha := CreateCommit(t, repoPath, newBranch, &CreateCommitOpts{
		Message: "a new branch and commit " + nonce,
	})

	return sha, newBranch
}
