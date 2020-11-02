package testhelper

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"io/ioutil"
	"math/big"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_logrus "github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	grpc_ctxtags "github.com/grpc-ecosystem/go-grpc-middleware/tags"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/command"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/helper/fieldextractors"
	"gitlab.com/gitlab-org/gitaly/internal/helper/text"
	gitalylog "gitlab.com/gitlab-org/gitaly/internal/log"
	"gitlab.com/gitlab-org/gitaly/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/internal/storage"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// TestRelativePath is the path inside its storage of the gitlab-test repo
const (
	TestRelativePath    = "gitlab-test.git"
	RepositoryAuthToken = "the-secret-token"
	DefaultStorageName  = "default"
	testGitEnv          = "testdata/git-env"
	GlRepository        = "project-1"
	GlID                = "user-123"
	GlProjectPath       = "gitlab-org/gitlab-test"
)

var (
	configureOnce sync.Once
	testDirectory string

	TestUser = &gitalypb.User{
		Name:       []byte("Jane Doe"),
		Email:      []byte("janedoe@gitlab.com"),
		GlId:       GlID,
		GlUsername: "janedoe",
	}
)

// Configure sets up the global test configuration. On failure,
// terminates the program.
func Configure() func() {
	configureOnce.Do(func() {
		gitalylog.Configure("json", "info")

		var err error
		testDirectory, err = ioutil.TempDir("", "gitaly-test-*")
		if err != nil {
			log.Fatal(err)
		}

		config.Config.Logging.Dir = filepath.Join(testDirectory, "log")
		if err := os.Mkdir(config.Config.Logging.Dir, 0755); err != nil {
			os.RemoveAll(testDirectory)
			log.Fatal(err)
		}

		config.Config.Storages = []config.Storage{
			{Name: "default", Path: GitlabTestStoragePath()},
		}

		config.Config.SocketPath = "/bogus"
		config.Config.GitlabShell.Dir = "/"

		config.Config.InternalSocketDir = filepath.Join(testDirectory, "internal-socket")
		if err := os.Mkdir(config.Config.InternalSocketDir, 0755); err != nil {
			os.RemoveAll(testDirectory)
			log.Fatal(err)
		}

		config.Config.BinDir = filepath.Join(testDirectory, "bin")
		if err := os.Mkdir(config.Config.BinDir, 0755); err != nil {
			os.RemoveAll(testDirectory)
			log.Fatal(err)
		}

		for _, f := range []func() error{
			ConfigureRuby,
			ConfigureGit,
			config.Validate,
		} {
			if err := f(); err != nil {
				os.RemoveAll(testDirectory)
				log.Fatalf("error configuring tests: %v", err)
			}
		}
	})

	return func() {
		if err := os.RemoveAll(testDirectory); err != nil {
			log.Fatalf("error removing test directory: %v", err)
		}
	}
}

// MustReadFile returns the content of a file or fails at once.
func MustReadFile(t testing.TB, filename string) []byte {
	content, err := ioutil.ReadFile(filename)
	if err != nil {
		t.Fatal(err)
	}

	return content
}

// GitlabTestStoragePath returns the storage path to the gitlab-test repo.
func GitlabTestStoragePath() string {
	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		log.Fatal("Could not get caller info")
	}
	return filepath.Join(filepath.Dir(currentFile), "testdata", "data")
}

// GitalyServersMetadata returns a metadata pair for gitaly-servers to be used in
// inter-gitaly operations.
func GitalyServersMetadata(t testing.TB, serverSocketPath string) metadata.MD {
	gitalyServers := storage.GitalyServers{
		"default": storage.ServerInfo{
			Address: serverSocketPath,
			Token:   RepositoryAuthToken,
		},
	}

	gitalyServersJSON, err := json.Marshal(gitalyServers)
	if err != nil {
		t.Fatal(err)
	}

	return metadata.Pairs("gitaly-servers", base64.StdEncoding.EncodeToString(gitalyServersJSON))
}

// MergeOutgoingMetadata merges provided metadata-s and returns context with resulting value.
func MergeOutgoingMetadata(ctx context.Context, md ...metadata.MD) context.Context {
	ctxmd, ok := metadata.FromOutgoingContext(ctx)
	if !ok {
		return metadata.NewOutgoingContext(ctx, metadata.Join(md...))
	}

	return metadata.NewOutgoingContext(ctx, metadata.Join(append(md, ctxmd)...))
}

// MergeIncomingMetadata merges provided metadata-s and returns context with resulting value.
func MergeIncomingMetadata(ctx context.Context, md ...metadata.MD) context.Context {
	ctxmd, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return metadata.NewIncomingContext(ctx, metadata.Join(md...))
	}

	return metadata.NewIncomingContext(ctx, metadata.Join(append(md, ctxmd)...))
}

// isValidRepoPath checks whether a valid git repository exists at the given path.
func isValidRepoPath(absolutePath string) bool {
	if _, err := os.Stat(filepath.Join(absolutePath, "objects")); err != nil {
		return false
	}

	return true
}

// TestRepository returns the `Repository` object for the gitlab-test repo.
// Tests should be calling this function instead of cloning the repo themselves.
// Tests that involve modifications to the repo should copy/clone the repo
// via the `Repository` returned from this function.
func TestRepository() *gitalypb.Repository {
	repo := &gitalypb.Repository{
		StorageName:  "default",
		RelativePath: TestRelativePath,
		GlRepository: GlRepository,
	}

	storagePath, _ := config.Config.StoragePath(repo.GetStorageName())
	if !isValidRepoPath(filepath.Join(storagePath, repo.RelativePath)) {
		panic("Test repo not found, did you run `make prepare-tests`?")
	}

	return repo
}

// RequireGrpcError asserts the passed err is of the same code as expectedCode.
func RequireGrpcError(t testing.TB, err error, expectedCode codes.Code) {
	t.Helper()

	if err == nil {
		t.Fatal("Expected an error, got nil")
	}

	// Check that the code matches
	status, _ := status.FromError(err)
	if code := status.Code(); code != expectedCode {
		t.Fatalf("Expected an error with code %v, got %v. The error was %q", expectedCode, code, err.Error())
	}
}

// MustRunCommand runs a command with an optional standard input and returns the standard output, or fails.
func MustRunCommand(t testing.TB, stdin io.Reader, name string, args ...string) []byte {
	if t != nil {
		t.Helper()
	}

	var cmd *exec.Cmd
	if name == "git" {
		cmd = exec.Command(command.GitPath(), args...)
		cmd.Env = os.Environ()
		cmd.Env = append(command.GitEnv, cmd.Env...)
		cmd.Env = append(cmd.Env,
			"GIT_AUTHOR_DATE=1572776879 +0100",
			"GIT_COMMITTER_DATE=1572776879 +0100",
		)
	} else {
		cmd = exec.Command(name, args...)
	}

	if stdin != nil {
		cmd.Stdin = stdin
	}

	output, err := cmd.Output()
	if err != nil {
		stderr := err.(*exec.ExitError).Stderr
		if t == nil {
			log.Print(name, args)
			log.Printf("%s", stderr)
			log.Fatal(err)
		} else {
			t.Log(name, args)
			t.Logf("%s", stderr)
			t.Fatal(err)
		}
	}

	return output
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

// FindLocalBranchCommitAuthorsEqual tests if two `FindLocalBranchCommitAuthor`s are equal
func FindLocalBranchCommitAuthorsEqual(a *gitalypb.FindLocalBranchCommitAuthor, b *gitalypb.FindLocalBranchCommitAuthor) bool {
	return bytes.Equal(a.Name, b.Name) &&
		bytes.Equal(a.Email, b.Email) &&
		a.Date.Seconds == b.Date.Seconds
}

// FindLocalBranchResponsesEqual tests if two `FindLocalBranchResponse`s are equal
func FindLocalBranchResponsesEqual(a *gitalypb.FindLocalBranchResponse, b *gitalypb.FindLocalBranchResponse) bool {
	return a.CommitId == b.CommitId &&
		bytes.Equal(a.CommitSubject, b.CommitSubject) &&
		FindLocalBranchCommitAuthorsEqual(a.CommitAuthor, b.CommitAuthor) &&
		FindLocalBranchCommitAuthorsEqual(a.CommitCommitter, b.CommitCommitter)
}

// GetTemporaryGitalySocketFileName will return a unique, useable socket file name
func GetTemporaryGitalySocketFileName() string {
	if testDirectory == "" {
		log.Fatal("you must call testhelper.Configure() before GetTemporaryGitalySocketFileName()")
	}

	tmpfile, err := ioutil.TempFile(testDirectory, "gitaly.socket.")
	if err != nil {
		// No point in handling this error, panic
		panic(err)
	}

	name := tmpfile.Name()
	tmpfile.Close()
	os.Remove(name)

	return name
}

// GetLocalhostListener listens on the next available TCP port and returns
// the listener and the localhost address (host:port) string.
func GetLocalhostListener(t testing.TB) (net.Listener, string) {
	l, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)

	addr := fmt.Sprintf("localhost:%d", l.Addr().(*net.TCPAddr).Port)

	return l, addr
}

// ConfigureGit configures git for test purpose
func ConfigureGit() error {
	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		return fmt.Errorf("could not get caller info")
	}

	// Set both GOCACHE and GOPATH to the currently active settings to not
	// have them be overridden by changing our home directory. default it
	for _, envvar := range []string{"GOCACHE", "GOPATH"} {
		cmd := exec.Command("go", "env", envvar)

		output, err := cmd.Output()
		if err != nil {
			return err
		}

		err = os.Setenv(envvar, strings.TrimSpace(string(output)))
		if err != nil {
			return err
		}
	}

	testHome := filepath.Join(filepath.Dir(currentFile), "testdata/home")
	// overwrite HOME env variable so user global .gitconfig doesn't influence tests
	return os.Setenv("HOME", testHome)
}

// ConfigureRuby configures Ruby settings for test purposes at run time.
func ConfigureRuby() error {
	if dir := os.Getenv("GITALY_TEST_RUBY_DIR"); len(dir) > 0 {
		// Sometimes runtime.Caller is unreliable. This environment variable provides a bypass.
		config.Config.Ruby.Dir = dir
	} else {
		_, currentFile, _, ok := runtime.Caller(0)
		if !ok {
			return fmt.Errorf("could not get caller info")
		}
		config.Config.Ruby.Dir = filepath.Join(filepath.Dir(currentFile), "../../ruby")
	}

	if err := config.ConfigureRuby(); err != nil {
		log.Fatalf("validate ruby config: %v", err)
	}

	return nil
}

// GetGitEnvData reads and returns the content of testGitEnv
func GetGitEnvData() (string, error) {
	gitEnvBytes, err := ioutil.ReadFile(testGitEnv)

	if err != nil {
		return "", err
	}

	return string(gitEnvBytes), nil
}

// NewTestGrpcServer creates a GRPC Server for testing purposes
func NewTestGrpcServer(tb testing.TB, streamInterceptors []grpc.StreamServerInterceptor, unaryInterceptors []grpc.UnaryServerInterceptor) *grpc.Server {
	logger := NewTestLogger(tb)
	logrusEntry := log.NewEntry(logger).WithField("test", tb.Name())

	ctxTagger := grpc_ctxtags.WithFieldExtractorForInitialReq(fieldextractors.FieldExtractor)
	ctxStreamTagger := grpc_ctxtags.StreamServerInterceptor(ctxTagger)
	ctxUnaryTagger := grpc_ctxtags.UnaryServerInterceptor(ctxTagger)

	streamInterceptors = append([]grpc.StreamServerInterceptor{ctxStreamTagger, grpc_logrus.StreamServerInterceptor(logrusEntry)}, streamInterceptors...)
	unaryInterceptors = append([]grpc.UnaryServerInterceptor{ctxUnaryTagger, grpc_logrus.UnaryServerInterceptor(logrusEntry)}, unaryInterceptors...)

	return grpc.NewServer(
		grpc.StreamInterceptor(grpc_middleware.ChainStreamServer(streamInterceptors...)),
		grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(unaryInterceptors...)),
	)
}

// MustHaveNoChildProcess panics if it finds a running or finished child
// process. It waits for 2 seconds for processes to be cleaned up by other
// goroutines.
func MustHaveNoChildProcess() {
	waitDone := make(chan struct{})
	go func() {
		command.WaitAllDone()
		close(waitDone)
	}()

	select {
	case <-waitDone:
	case <-time.After(2 * time.Second):
	}

	mustFindNoFinishedChildProcess()
	mustFindNoRunningChildProcess()
}

func mustFindNoFinishedChildProcess() {
	// Wait4(pid int, wstatus *WaitStatus, options int, rusage *Rusage) (wpid int, err error)
	//
	// We use pid -1 to wait for any child. We don't care about wstatus or
	// rusage. Use WNOHANG to return immediately if there is no child waiting
	// to be reaped.
	wpid, err := syscall.Wait4(-1, nil, syscall.WNOHANG, nil)
	if err == nil && wpid > 0 {
		panic(fmt.Errorf("wait4 found child process %d", wpid))
	}
}

func mustFindNoRunningChildProcess() {
	pgrep := exec.Command("pgrep", "-P", fmt.Sprintf("%d", os.Getpid()))
	desc := fmt.Sprintf("%q", strings.Join(pgrep.Args, " "))

	out, err := pgrep.Output()
	if err == nil {
		pidsComma := strings.Replace(text.ChompBytes(out), "\n", ",", -1)
		psOut, _ := exec.Command("ps", "-o", "pid,args", "-p", pidsComma).Output()
		panic(fmt.Errorf("found running child processes %s:\n%s", pidsComma, psOut))
	}

	if status, ok := command.ExitStatus(err); ok && status == 1 {
		// Exit status 1 means no processes were found
		return
	}

	panic(fmt.Errorf("%s: %v", desc, err))
}

// ContextOpt returns a new context instance with the new additions to it.
type ContextOpt func(context.Context) (context.Context, func())

// ContextWithTimeout allows to set provided timeout to the context.
func ContextWithTimeout(duration time.Duration) ContextOpt {
	return func(ctx context.Context) (context.Context, func()) {
		return context.WithTimeout(ctx, duration)
	}
}

// ContextWithLogger allows to inject provided logger into the context.
func ContextWithLogger(logger *log.Entry) ContextOpt {
	return func(ctx context.Context) (context.Context, func()) {
		return ctxlogrus.ToContext(ctx, logger), func() {}
	}
}

// Context returns a cancellable context.
func Context(opts ...ContextOpt) (context.Context, func()) {
	ctx, cancel := context.WithCancel(context.Background())
	for _, ff := range featureflag.All {
		ctx = featureflag.IncomingCtxWithFeatureFlag(ctx, ff)
		ctx = featureflag.OutgoingCtxWithFeatureFlags(ctx, ff)
	}

	cancels := make([]func(), len(opts)+1)
	cancels[0] = cancel
	for i, opt := range opts {
		ctx, cancel = opt(ctx)
		cancels[i+1] = cancel
	}

	return ctx, func() {
		for i := len(cancels) - 1; i >= 0; i-- {
			cancels[i]()
		}
	}
}

// CreateRepo creates a temporary directory for a repo, without initializing it
func CreateRepo(t testing.TB, storagePath, relativePath string) *gitalypb.Repository {
	require.NoError(t, os.MkdirAll(filepath.Dir(storagePath), 0755), "making repo parent dir")
	return &gitalypb.Repository{
		StorageName:   "default",
		RelativePath:  relativePath,
		GlRepository:  GlRepository,
		GlProjectPath: GlProjectPath,
	}
}

// InitBareRepo creates a new bare repository
func InitBareRepo(t testing.TB) (*gitalypb.Repository, string, func()) {
	return initRepo(t, true)
}

// InitRepoWithWorktree creates a new repository with a worktree
func InitRepoWithWorktree(t testing.TB) (*gitalypb.Repository, string, func()) {
	return initRepo(t, false)
}

func initRepo(t testing.TB, bare bool) (*gitalypb.Repository, string, func()) {
	storagePath := GitlabTestStoragePath()
	relativePath := NewRepositoryName(t, bare)
	repoPath := filepath.Join(storagePath, relativePath)

	args := []string{"init"}
	if bare {
		args = append(args, "--bare")
	}

	MustRunCommand(t, nil, "git", append(args, repoPath)...)

	repo := CreateRepo(t, storagePath, relativePath)
	if !bare {
		repo.RelativePath = filepath.Join(repo.RelativePath, ".git")
	}

	return repo, repoPath, func() { require.NoError(t, os.RemoveAll(repoPath)) }
}

// NewTestRepoTo clones a new copy of test repository under a subdirectory in the storage root.
func NewTestRepoTo(t testing.TB, storageRoot, relativePath string) *gitalypb.Repository {
	repo, _, _ := cloneTestRepo(t, storageRoot, relativePath, true)
	return repo
}

// NewTestRepo creates a bare copy of the test repository..
func NewTestRepo(t testing.TB) (repo *gitalypb.Repository, repoPath string, cleanup func()) {
	return cloneTestRepo(t, GitlabTestStoragePath(), NewRepositoryName(t, true), true)
}

// NewTestRepoWithWorktree creates a copy of the test repository with a
// worktree. This is allows you to run normal 'non-bare' Git commands.
func NewTestRepoWithWorktree(t testing.TB) (repo *gitalypb.Repository, repoPath string, cleanup func()) {
	return cloneTestRepo(t, GitlabTestStoragePath(), NewRepositoryName(t, false), false)
}

// testRepositoryPath returns the absolute path of local 'gitlab-org/gitlab-test.git' clone.
// It is cloned under the path by the test preparing step of make.
func testRepositoryPath(t testing.TB) string {
	path := filepath.Join(GitlabTestStoragePath(), TestRelativePath)
	if !isValidRepoPath(path) {
		t.Fatalf("local clone of 'gitlab-org/gitlab-test.git' not found in %q, did you run `make prepare-tests`?", path)
	}

	return path
}

func cloneTestRepo(t testing.TB, storageRoot, relativePath string, bare bool) (repo *gitalypb.Repository, repoPath string, cleanup func()) {
	repoPath = filepath.Join(storageRoot, relativePath)

	repo = CreateRepo(t, storageRoot, relativePath)
	args := []string{"clone", "--no-hardlinks", "--dissociate"}
	if bare {
		args = append(args, "--bare")
	} else {
		// For non-bare repos the relative path is the .git folder inside the path
		repo.RelativePath = filepath.Join(relativePath, ".git")
	}

	MustRunCommand(t, nil, "git", append(args, testRepositoryPath(t), repoPath)...)

	return repo, repoPath, func() { require.NoError(t, os.RemoveAll(repoPath)) }
}

// AddWorktreeArgs returns git command arguments for adding a worktree at the
// specified repo
func AddWorktreeArgs(repoPath, worktreeName string) []string {
	return []string{"-C", repoPath, "worktree", "add", "--detach", worktreeName}
}

// AddWorktree creates a worktree in the repository path for tests
func AddWorktree(t testing.TB, repoPath string, worktreeName string) {
	MustRunCommand(t, nil, "git", AddWorktreeArgs(repoPath, worktreeName)...)
}

// GitalySSHParams contains parameters used to exec 'gitaly-ssh' binary.
type GitalySSHParams struct {
	Args    []string
	EnvVars []string
}

// ListenGitalySSHCalls creates a script that intercepts 'gitaly-ssh' binary calls.
// It substitutes execution path of 'gitaly-ssh' with a path to a script and returns a modified configuration to be used.
// The second return parameter provides the list of parameters used in each invocation of the 'gitaly-ssh'.
func ListenGitalySSHCalls(t *testing.T, conf config.Cfg) (config.Cfg, func() []GitalySSHParams, Cleanup) {
	t.Helper()

	if conf.BinDir == "" {
		assert.FailNow(t, "BinDir must be set")
		return conf, func() []GitalySSHParams { return nil }, func() {}
	}

	const envPrefix = "env-"
	const argsPrefix = "args-"

	tmpDir, clean := TempDir(t)
	script := fmt.Sprintf(`
		#!/bin/sh

		# To omit possible problem with parallel run and a race for the file creation with '>'
		# this option is used, please checkout https://mywiki.wooledge.org/NoClobber for more details.
		set -o noclobber

		ENV_IDX=$(ls %[1]q | grep %[2]s | wc -l)
		env > "%[1]s/%[2]s$ENV_IDX"

		ARGS_IDX=$(ls %[1]q | grep %[3]s | wc -l)
		echo $@ > "%[1]s/%[3]s$ARGS_IDX"

		%[4]q "$@" 1>&1 2>&2
		exit $?`,
		tmpDir, envPrefix, argsPrefix, filepath.Join(conf.BinDir, "gitaly-ssh"))

	require.NoError(t, ioutil.WriteFile(filepath.Join(tmpDir, "gitaly-ssh"), []byte(script), 0755))
	conf.BinDir = tmpDir

	getSSHParams := func() []GitalySSHParams {
		var gitalySSHParams []GitalySSHParams
		err := filepath.Walk(tmpDir, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}

			filename := filepath.Base(path)

			parseParams := func(prefix, delim string) error {
				if !strings.HasPrefix(filename, prefix) {
					return nil
				}

				idx, err := strconv.Atoi(strings.TrimSpace(strings.TrimPrefix(filename, prefix)))
				if err != nil {
					return err
				}

				if len(gitalySSHParams) < idx+1 {
					tmp := make([]GitalySSHParams, idx+1)
					copy(tmp, gitalySSHParams)
					gitalySSHParams = tmp
				}

				data, err := ioutil.ReadFile(path)
				if err != nil {
					return err
				}

				params := strings.Split(string(data), delim)

				switch prefix {
				case argsPrefix:
					gitalySSHParams[idx].Args = params
				case envPrefix:
					gitalySSHParams[idx].EnvVars = params
				}

				return nil
			}

			if err := parseParams(envPrefix, "\n"); err != nil {
				return err
			}

			if err := parseParams(argsPrefix, " "); err != nil {
				return err
			}

			return nil
		})
		assert.NoError(t, err)
		return gitalySSHParams
	}

	return conf, getSSHParams, clean
}

func buildCommand(cmd string) {
	if config.Config.BinDir == "" {
		log.Fatal("config.Config.BinDir must be set")
	}

	goBuildArgs := []string{
		"build",
		"-tags", "static,system_libgit2",
		"-o", filepath.Join(config.Config.BinDir, cmd),
		fmt.Sprintf("gitlab.com/gitlab-org/gitaly/cmd/%s", cmd),
	}
	MustRunCommand(nil, nil, "go", goBuildArgs...)
}

// ConfigureGitalyGit2Go configures the gitaly-git2go command for tests
func ConfigureGitalyGit2Go() {
	buildCommand("gitaly-git2go")
}

// ConfigureGitalyLfsSmudge configures the gitaly-lfs-smudge command for tests
func ConfigureGitalyLfsSmudge() {
	buildCommand("gitaly-lfs-smudge")
}

// ConfigureGitalySSH configures the gitaly-ssh command for tests
func ConfigureGitalySSH() {
	buildCommand("gitaly-ssh")
}

// GetRepositoryRefs gives a list of each repository ref as a string
func GetRepositoryRefs(t testing.TB, repoPath string) string {
	refs := MustRunCommand(t, nil, "git", "-C", repoPath, "for-each-ref")

	return string(refs)
}

// AssertPathNotExists asserts true if the path doesn't exist, false otherwise
func AssertPathNotExists(t testing.TB, path string) {
	_, err := os.Stat(path)
	assert.True(t, os.IsNotExist(err), "file should not exist: %s", path)
}

// newDiskHash generates a random directory path following the Rails app's
// approach in the hashed storage module, formatted as '[0-9a-f]{2}/[0-9a-f]{2}/[0-9a-f]{64}'.
// https://gitlab.com/gitlab-org/gitlab/-/blob/f5c7d8eb1dd4eee5106123e04dec26d277ff6a83/app/models/storage/hashed.rb#L38-43
func newDiskHash(t testing.TB) string {
	// rails app calculates a sha256 and uses its hex representation
	// as the directory path
	b, err := text.RandomHex(sha256.Size)
	require.NoError(t, err)
	return filepath.Join(b[0:2], b[2:4], b)
}

// NewRepositoryName returns a random repository hash
// in format '@hashed/[0-9a-f]{2}/[0-9a-f]{2}/[0-9a-f]{64}(.git)?'.
func NewRepositoryName(t testing.TB, bare bool) string {
	suffix := ""
	if bare {
		suffix = ".git"
	}

	return filepath.Join("@hashed", newDiskHash(t)+suffix)
}

// NewTestObjectPoolName returns a random pool repository name
// in format '@pools/[0-9a-z]{2}/[0-9a-z]{2}/[0-9a-z]{64}.git'.
func NewTestObjectPoolName(t testing.TB) string {
	return filepath.Join("@pools", newDiskHash(t)+".git")
}

// CreateLooseRef creates a ref that points to master
func CreateLooseRef(t testing.TB, repoPath, refName string) {
	relRefPath := fmt.Sprintf("refs/heads/%s", refName)
	MustRunCommand(t, nil, "git", "-C", repoPath, "update-ref", relRefPath, "master")
	require.FileExists(t, filepath.Join(repoPath, relRefPath), "ref must be in loose file")
}

// TempDir is a wrapper around ioutil.TempDir that provides a cleanup function.
func TempDir(t testing.TB) (string, func()) {
	if testDirectory == "" {
		log.Fatal("you must call testhelper.Configure() before TempDir()")
	}

	tmpDir, err := ioutil.TempDir(testDirectory, "")
	require.NoError(t, err)

	return tmpDir, func() { require.NoError(t, os.RemoveAll(tmpDir)) }
}

// GitObjectMustExist is a test assertion that fails unless the git repo in repoPath contains sha
func GitObjectMustExist(t testing.TB, repoPath, sha string) {
	gitObjectExists(t, repoPath, sha, true)
}

// GitObjectMustNotExist is a test assertion that fails unless the git repo in repoPath contains sha
func GitObjectMustNotExist(t testing.TB, repoPath, sha string) {
	gitObjectExists(t, repoPath, sha, false)
}

func gitObjectExists(t testing.TB, repoPath, sha string, exists bool) {
	cmd := exec.Command(command.GitPath(), "-C", repoPath, "cat-file", "-e", sha)
	cmd.Env = []string{
		"GIT_ALLOW_PROTOCOL=", // To prevent partial clone reaching remote repo over SSH
	}

	if exists {
		require.NoError(t, cmd.Run(), "checking for object should succeed")
		return
	}
	require.Error(t, cmd.Run(), "checking for object should fail")
}

// Cleanup functions should be called in a defer statement
// immediately after they are returned from a test helper
type Cleanup func()

// GetGitObjectDirSize gets the number of 1k blocks of a git object directory
func GetGitObjectDirSize(t testing.TB, repoPath string) int64 {
	return getGitDirSize(t, repoPath, "objects")
}

// GetGitPackfileDirSize gets the number of 1k blocks of a git object directory
func GetGitPackfileDirSize(t testing.TB, repoPath string) int64 {
	return getGitDirSize(t, repoPath, "objects", "pack")
}

func getGitDirSize(t testing.TB, repoPath string, subdirs ...string) int64 {
	cmd := exec.Command("du", "-s", "-k", filepath.Join(append([]string{repoPath}, subdirs...)...))
	output, err := cmd.Output()
	require.NoError(t, err)
	if len(output) < 2 {
		t.Error("invalid output of du -s -k")
	}

	outputSplit := strings.SplitN(string(output), "\t", 2)
	blocks, err := strconv.ParseInt(outputSplit[0], 10, 64)
	require.NoError(t, err)

	return blocks
}

func GrpcErrorHasMessage(grpcError error, msg string) bool {
	status, ok := status.FromError(grpcError)
	if !ok {
		return false
	}
	return status.Message() == msg
}

// dump the env vars that the custom hooks receives to a file
func WriteEnvToCustomHook(t testing.TB, repoPath, hookName string) (string, func()) {
	hookOutputTemp, err := ioutil.TempFile("", "")
	require.NoError(t, err)
	require.NoError(t, hookOutputTemp.Close())

	hookContent := fmt.Sprintf("#!/bin/sh\n/usr/bin/env > %s\n", hookOutputTemp.Name())

	cleanupCustomHook, err := WriteCustomHook(repoPath, hookName, []byte(hookContent))
	require.NoError(t, err)

	return hookOutputTemp.Name(), func() {
		cleanupCustomHook()
		os.Remove(hookOutputTemp.Name())
	}
}

// write a hook in the repo/path.git/custom_hooks directory
func WriteCustomHook(repoPath, name string, content []byte) (func(), error) {
	fullPath := filepath.Join(repoPath, "custom_hooks", name)
	return WriteExecutable(fullPath, content)
}

// WriteExecutable ensures that the parent directory exists, and writes an executable with provided content
func WriteExecutable(path string, content []byte) (func(), error) {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return func() {}, err
	}

	return func() {
		os.RemoveAll(dir)
	}, ioutil.WriteFile(path, content, 0755)
}

// WriteCheckNewObjectExistsHook writes a pre-receive hook which only succeeds
// if it can find the object in the quarantine directory. if
// GIT_OBJECT_DIRECTORY and GIT_ALTERNATE_OBJECT_DIRECTORIES were not passed
// through correctly to the hooks, it will fail
func WriteCheckNewObjectExistsHook(t *testing.T, repoPath string) func() {
	hook := fmt.Sprintf(`#!/usr/bin/env ruby
STDIN.each_line do |line|
  new_object = line.split(' ')[1]
  exit 1 unless new_object
  exit 1 unless	system(*%%W[%s cat-file -e #{new_object}])
end
`, command.GitPath())

	cleanup, err := WriteCustomHook(repoPath, "pre-receive", []byte(hook))
	require.NoError(t, err)

	return cleanup
}

func WriteBlobs(t testing.TB, testRepoPath string, n int) []string {
	var blobIDs []string
	for i := 0; i < n; i++ {
		var stdin bytes.Buffer
		stdin.Write([]byte(strconv.Itoa(time.Now().Nanosecond())))
		blobIDs = append(blobIDs, text.ChompBytes(MustRunCommand(t, &stdin, "git", "-C", testRepoPath, "hash-object", "-w", "--stdin")))
	}

	return blobIDs
}

// ModifyEnvironment will change an environment variable and return a func suitable
// for `defer` to change the value back.
func ModifyEnvironment(t testing.TB, key string, value string) func() {
	t.Helper()

	oldValue, hasOldValue := os.LookupEnv(key)
	require.NoError(t, os.Setenv(key, value))
	return func() {
		if hasOldValue {
			require.NoError(t, os.Setenv(key, oldValue))
		} else {
			require.NoError(t, os.Unsetenv(key))
		}
	}
}

// GenerateTestCerts creates a certificate that can be used to establish TLS protected TCP connection.
// It returns paths to the file with the certificate and its private key.
func GenerateTestCerts(t *testing.T) (string, string, Cleanup) {
	t.Helper()

	rootCA := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		NotBefore:             time.Now(),
		NotAfter:              time.Now().AddDate(0, 0, 1),
		BasicConstraintsValid: true,
		IsCA:                  true,
		IPAddresses:           []net.IP{net.ParseIP("0.0.0.0"), net.ParseIP("127.0.0.1"), net.ParseIP("::1"), net.ParseIP("::")},
		DNSNames:              []string{"localhost"},
		KeyUsage:              x509.KeyUsageCertSign,
	}

	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	caCert, err := x509.CreateCertificate(rand.Reader, rootCA, rootCA, &caKey.PublicKey, caKey)
	require.NoError(t, err)

	entityKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	entityX509 := &x509.Certificate{
		SerialNumber: big.NewInt(2),
	}

	entityCert, err := x509.CreateCertificate(rand.Reader, rootCA, entityX509, &entityKey.PublicKey, caKey)
	require.NoError(t, err)

	certFile, err := ioutil.TempFile("", "")
	require.NoError(t, err)
	defer certFile.Close()

	// create chained PEM file with CA and entity cert
	for _, cert := range [][]byte{entityCert, caCert} {
		require.NoError(t,
			pem.Encode(certFile, &pem.Block{
				Type:  "CERTIFICATE",
				Bytes: cert,
			}),
		)
	}

	keyFile, err := ioutil.TempFile("", "")
	require.NoError(t, err)
	defer keyFile.Close()

	entityKeyBytes, err := x509.MarshalECPrivateKey(entityKey)
	require.NoError(t, err)

	require.NoError(t,
		pem.Encode(keyFile, &pem.Block{
			Type:  "ECDSA PRIVATE KEY",
			Bytes: entityKeyBytes,
		}),
	)

	cleanup := func() {
		require.NoError(t, os.Remove(certFile.Name()))
		require.NoError(t, os.Remove(keyFile.Name()))
	}

	return certFile.Name(), keyFile.Name(), cleanup
}

func DefaultLocator() storage.Locator {
	return config.NewLocator(config.Cfg{Storages: []config.Storage{{Name: "default", Path: GitlabTestStoragePath()}}})
}
