package gittest

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/helper/text"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
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
func CreateCommit(t testing.TB, cfg config.Cfg, repoPath, branchName string, opts *CreateCommitOpts) string {
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
	newCommit := ExecStream(t, cfg, stdin, commitArgs...)
	newCommitID := text.ChompBytes(newCommit)

	Exec(t, cfg, "-C", repoPath, "update-ref", "refs/heads/"+branchName, newCommitID)
	return newCommitID
}

// CreateCommitInAlternateObjectDirectory runs a command such that its created
// objects will live in an alternate objects directory. It returns the current
// head after the command is run and the alternate objects directory path
func CreateCommitInAlternateObjectDirectory(t testing.TB, gitBin, repoPath, altObjectsDir string, cmd *exec.Cmd) (currentHead []byte) {
	gitPath := filepath.Join(repoPath, ".git")

	altObjectsPath := filepath.Join(gitPath, altObjectsDir)
	gitObjectEnv := []string{
		fmt.Sprintf("GIT_OBJECT_DIRECTORY=%s", altObjectsPath),
		fmt.Sprintf("GIT_ALTERNATE_OBJECT_DIRECTORIES=%s", filepath.Join(gitPath, "objects")),
	}
	require.NoError(t, os.MkdirAll(altObjectsPath, 0755))

	// Because we set 'gitObjectEnv', the new objects created by this command
	// will go into 'find-commits-alt-test-repo/.git/alt-objects'.
	cmd.Env = append(cmd.Env, gitObjectEnv...)
	if output, err := cmd.Output(); err != nil {
		stderr := err.(*exec.ExitError).Stderr
		t.Fatalf("stdout: %s, stderr: %s", output, stderr)
	}

	cmd = exec.Command(gitBin, "-C", repoPath, "rev-parse", "HEAD")
	cmd.Env = gitObjectEnv
	currentHead, err := cmd.Output()
	require.NoError(t, err)

	return currentHead[:len(currentHead)-1]
}

// CommitBlobWithName will create a commit for the specified blob with the
// specified name. This enables testing situations where the filepath is not
// possible due to filesystem constraints (e.g. non-UTF characters). The commit
// ID is returned.
func CommitBlobWithName(t testing.TB, cfg config.Cfg, testRepoPath, blobID, fileName, commitMessage string) string {
	mktreeIn := strings.NewReader(fmt.Sprintf("100644 blob %s\t%s", blobID, fileName))
	treeID := text.ChompBytes(ExecStream(t, cfg, mktreeIn, "-C", testRepoPath, "mktree"))

	return text.ChompBytes(
		Exec(t, cfg,
			"-c", fmt.Sprintf("user.name=%s", committerName),
			"-c", fmt.Sprintf("user.email=%s", committerEmail),
			"-C", testRepoPath, "commit-tree", treeID, "-m", commitMessage),
	)
}

// CreateCommitOnNewBranch creates a branch and a commit, returning the commit sha and the branch name respectivelyi
func CreateCommitOnNewBranch(t testing.TB, cfg config.Cfg, repoPath string) (string, string) {
	nonce, err := text.RandomHex(4)
	require.NoError(t, err)
	newBranch := "branch-" + nonce

	sha := CreateCommit(t, cfg, repoPath, newBranch, &CreateCommitOpts{
		Message: "a new branch and commit " + nonce,
	})

	return sha, newBranch
}

// authorSortofEqual tests if two `CommitAuthor`s have the same name and email.
//  useful when creating commits in the tests.
func authorSortofEqual(a, b *gitalypb.CommitAuthor) bool {
	if (a == nil) != (b == nil) {
		return false
	}
	return bytes.Equal(a.GetName(), b.GetName()) &&
		bytes.Equal(a.GetEmail(), b.GetEmail())
}

// AuthorsEqual tests if two `CommitAuthor`s are equal
func AuthorsEqual(a *gitalypb.CommitAuthor, b *gitalypb.CommitAuthor) bool {
	return authorSortofEqual(a, b) &&
		a.GetDate().Seconds == b.GetDate().Seconds
}

// GitCommitEqual tests if two `GitCommit`s are equal
func GitCommitEqual(a, b *gitalypb.GitCommit) error {
	if !authorSortofEqual(a.GetAuthor(), b.GetAuthor()) {
		return fmt.Errorf("author does not match: %v != %v", a.GetAuthor(), b.GetAuthor())
	}
	if !authorSortofEqual(a.GetCommitter(), b.GetCommitter()) {
		return fmt.Errorf("commiter does not match: %v != %v", a.GetCommitter(), b.GetCommitter())
	}
	if !bytes.Equal(a.GetBody(), b.GetBody()) {
		return fmt.Errorf("body differs: %q != %q", a.GetBody(), b.GetBody())
	}
	if !bytes.Equal(a.GetSubject(), b.GetSubject()) {
		return fmt.Errorf("subject differs: %q != %q", a.GetSubject(), b.GetSubject())
	}
	if strings.Compare(a.GetId(), b.GetId()) != 0 {
		return fmt.Errorf("id does not match: %q != %q", a.GetId(), b.GetId())
	}
	if len(a.GetParentIds()) != len(b.GetParentIds()) {
		return fmt.Errorf("ParentId does not match: %v != %v", a.GetParentIds(), b.GetParentIds())
	}

	for i, pid := range a.GetParentIds() {
		pid2 := b.GetParentIds()[i]
		if strings.Compare(pid, pid2) != 0 {
			return fmt.Errorf("parent id mismatch: %v != %v", pid, pid2)
		}
	}

	return nil
}
