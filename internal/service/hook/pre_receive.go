package hook

import (
	"bytes"
	"context"
	"crypto/sha1"
	"errors"
	"fmt"
	"io/ioutil"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"gitlab.com/gitlab-org/gitaly/internal/config"
	"gitlab.com/gitlab-org/gitaly/internal/git/hooks"
	"gitlab.com/gitlab-org/gitaly/internal/gitlabshell"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/metadata"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/streamio"
	"google.golang.org/grpc"
)

type hookRequest interface {
	GetEnvironmentVariables() []string
	GetRepository() *gitalypb.Repository
}

type prePostRequest interface {
	hookRequest
	GetGitPushOptions() []string
}

func hookRequestEnv(req hookRequest) ([]string, error) {
	gitlabshellEnv, err := gitlabshell.Env()
	if err != nil {
		return nil, err
	}
	return append(gitlabshellEnv, req.GetEnvironmentVariables()...), nil
}

func preReceiveEnv(req prePostRequest) ([]string, error) {
	env, err := hookRequestEnv(req)
	if err != nil {
		return nil, err
	}

	env = append(env, hooks.GitPushOptions(req.GetGitPushOptions())...)

	return env, nil
}

func gitlabShellHook(hookName string) string {
	return filepath.Join(config.Config.Ruby.Dir, "gitlab-shell", "hooks", hookName)
}

func (s *server) getPraefectConn(ctx context.Context, server *metadata.PraefectServer) (*grpc.ClientConn, error) {
	address, err := server.Address()
	if err != nil {
		return nil, err
	}

	s.mutex.RLock()
	conn, ok := s.praefectConnPool[address]
	s.mutex.RUnlock()

	if ok {
		return conn, nil
	}

	s.mutex.Lock()
	defer s.mutex.Unlock()

	conn, ok = s.praefectConnPool[address]
	if !ok {
		var err error
		conn, err = server.Dial(ctx)
		if err != nil {
			return nil, err
		}

		s.praefectConnPool[address] = conn
	}

	return conn, nil
}

func (s *server) voteOnTransaction(stream gitalypb.HookService_PreReceiveHookServer, hash []byte, env []string) error {
	tx, err := metadata.TransactionFromEnv(env)
	if err != nil {
		if errors.Is(err, metadata.ErrTransactionNotFound) {
			// No transaction being present is valid, e.g. in case
			// there is no Praefect server or the transactions
			// feature flag is not set.
			return nil
		}
		return fmt.Errorf("could not extract transaction: %w", err)
	}

	praefectServer, err := metadata.PraefectFromEnv(env)
	if err != nil {
		return fmt.Errorf("could not extract Praefect server: %w", err)
	}

	ctx, cancel := context.WithTimeout(stream.Context(), 10*time.Second)
	defer cancel()

	praefectConn, err := s.getPraefectConn(ctx, praefectServer)
	if err != nil {
		return err
	}

	praefectClient := gitalypb.NewRefTransactionClient(praefectConn)

	response, err := praefectClient.VoteTransaction(ctx, &gitalypb.VoteTransactionRequest{
		TransactionId:        tx.ID,
		Node:                 tx.Node,
		ReferenceUpdatesHash: hash,
	})
	if err != nil {
		return err
	}

	if response.State != gitalypb.VoteTransactionResponse_COMMIT {
		return errors.New("transaction was aborted")
	}

	return nil
}

func getRelativeObjectDirs(repoPath, gitObjectDir, gitAlternateObjectDirs string) (string, []string, error) {
	repoPathReal, err := filepath.EvalSymlinks(repoPath)
	if err != nil {
		return "", nil, err
	}

	gitObjDirRel, err := filepath.Rel(repoPathReal, gitObjectDir)
	if err != nil {
		return "", nil, err
	}

	var gitAltObjDirsRel []string

	for _, gitAltObjDirAbs := range strings.Split(gitAlternateObjectDirs, ":") {
		gitAltObjDirRel, err := filepath.Rel(repoPathReal, gitAltObjDirAbs)
		if err != nil {
			return "", nil, err
		}

		gitAltObjDirsRel = append(gitAltObjDirsRel, gitAltObjDirRel)
	}

	return gitObjDirRel, gitAltObjDirsRel, nil
}

func (s *server) PreReceiveHook(stream gitalypb.HookService_PreReceiveHookServer) error {
	firstRequest, err := stream.Recv()
	if err != nil {
		return helper.ErrInternalf("receiving first request: %w", err)
	}

	if err := validatePreReceiveHookRequest(firstRequest); err != nil {
		return helper.ErrInvalidArgument(err)
	}
	reqEnvVars := firstRequest.GetEnvironmentVariables()
	repository := firstRequest.GetRepository()

	if !useGoPreReceiveHook(reqEnvVars) {
		return s.preReceiveHookRuby(firstRequest, stream)
	}

	stdout := streamio.NewWriter(func(p []byte) error { return stream.Send(&gitalypb.PreReceiveHookResponse{Stdout: p}) })
	stderr := streamio.NewWriter(func(p []byte) error { return stream.Send(&gitalypb.PreReceiveHookResponse{Stderr: p}) })

	if gitObjDir, gitAltObjDirs := getEnvVar("GIT_OBJECT_DIRECTORY", reqEnvVars), getEnvVar("GIT_ALTERNATE_OBJECT_DIRECTORIES", reqEnvVars); gitObjDir != "" && gitAltObjDirs != "" {
		repoPath, err := helper.GetRepoPath(repository)
		if err != nil {
			return helper.ErrInternalf("getting repo path: %v", err)
		}

		gitObjectDirRel, gitAltObjectDirRel, err := getRelativeObjectDirs(repoPath, gitObjDir, gitAltObjDirs)
		if err != nil {
			return helper.ErrInternalf("getting relative git object directories: %v", err)
		}

		repository.GitObjectDirectory = gitObjectDirRel
		repository.GitAlternateObjectDirectories = gitAltObjectDirRel
	}

	stdin := streamio.NewReader(func() ([]byte, error) {
		req, err := stream.Recv()
		return req.GetStdin(), err
	})

	changes, err := ioutil.ReadAll(stdin)
	if err != nil {
		return helper.ErrInternalf("reading stdin from request: %w", err)
	}

	glID, glRepo, glProtocol := getEnvVar("GL_ID", reqEnvVars), getEnvVar("GL_REPOSITORY", reqEnvVars), getEnvVar("GL_PROTOCOL", reqEnvVars)

	allowed, message, err := s.gitlabAPI.Allowed(repository, glRepo, glID, glProtocol, string(changes))
	if err != nil {
		return preReceiveHookResponse(stream, int32(1), fmt.Sprintf("GitLab: %v", err))
	}

	if !allowed {
		return preReceiveHookResponse(stream, int32(1), message)
	}

	// custom hooks execution
	repoPath, err := helper.GetRepoPath(repository)
	if err != nil {
		return err
	}
	executor, err := newCustomHooksExecutor(repoPath, s.hooksConfig.CustomHooksDir, "pre-receive")
	if err != nil {
		return helper.ErrInternalf("creating custom hooks executor: %v", err)
	}

	env, err := preReceiveEnv(firstRequest)
	if err != nil {
		return helper.ErrInternalf("getting pre receive env: %v", err)
	}

	if err = executor(
		stream.Context(),
		nil,
		env,
		bytes.NewReader(changes),
		stdout,
		stderr,
	); err != nil {
		var exitError *exec.ExitError
		if errors.As(err, &exitError) {
			return preReceiveHookResponse(stream, int32(exitError.ExitCode()), "")
		}

		return helper.ErrInternalf("executing custom hooks: %v", err)
	}

	// reference counter
	ok, err := s.gitlabAPI.PreReceive(glRepo)
	if err != nil {
		return helper.ErrInternalf("calling pre_receive endpoint: %v", err)
	}

	if !ok {
		return preReceiveHookResponse(stream, 1, "")
	}

	hash := sha1.Sum(changes)
	if err := s.voteOnTransaction(stream, hash[:], env); err != nil {
		return helper.ErrInternalf("error voting on transaction: %v", err)
	}

	return preReceiveHookResponse(stream, 0, "")
}

func validatePreReceiveHookRequest(in *gitalypb.PreReceiveHookRequest) error {
	if in.GetRepository() == nil {
		return errors.New("repository is empty")
	}

	return nil
}

func useGoPreReceiveHook(env []string) bool {
	return getEnvVar(featureflag.GoPreReceiveHookEnvVar, env) == "true"
}

func preReceiveHookResponse(stream gitalypb.HookService_PreReceiveHookServer, code int32, stderr string) error {
	if err := stream.Send(&gitalypb.PreReceiveHookResponse{
		ExitStatus: &gitalypb.ExitStatus{Value: code},
		Stderr:     []byte(stderr),
	}); err != nil {
		return helper.ErrInternalf("sending response: %v", err)
	}

	return nil
}

func (s *server) preReceiveHookRuby(firstRequest *gitalypb.PreReceiveHookRequest, stream gitalypb.HookService_PreReceiveHookServer) error {
	referenceUpdatesHasher := sha1.New()

	stdin := streamio.NewReader(func() ([]byte, error) {
		req, err := stream.Recv()
		if err != nil {
			return nil, err
		}

		stdin := req.GetStdin()
		if _, err := referenceUpdatesHasher.Write(stdin); err != nil {
			return stdin, err
		}

		return stdin, nil
	})
	stdout := streamio.NewWriter(func(p []byte) error { return stream.Send(&gitalypb.PreReceiveHookResponse{Stdout: p}) })
	stderr := streamio.NewWriter(func(p []byte) error { return stream.Send(&gitalypb.PreReceiveHookResponse{Stderr: p}) })

	repoPath, err := helper.GetRepoPath(firstRequest.GetRepository())
	if err != nil {
		return helper.ErrInternal(err)
	}

	c := exec.Command(gitlabShellHook("pre-receive"))
	c.Dir = repoPath

	env, err := preReceiveEnv(firstRequest)
	if err != nil {
		return helper.ErrInternal(err)
	}

	status, err := streamCommandResponse(
		stream.Context(),
		stdin,
		stdout, stderr,
		c,
		env,
	)
	if err != nil {
		return helper.ErrInternal(err)
	}

	if err := s.voteOnTransaction(stream, referenceUpdatesHasher.Sum(nil), env); err != nil {
		return helper.ErrInternalf("error voting on transaction: %w", err)
	}

	if err := stream.SendMsg(&gitalypb.PreReceiveHookResponse{
		ExitStatus: &gitalypb.ExitStatus{Value: status},
	}); err != nil {
		return helper.ErrInternal(err)
	}

	return nil
}

func getEnvVar(key string, vars []string) string {
	for _, varPair := range vars {
		kv := strings.SplitN(varPair, "=", 2)
		if kv[0] == key {
			return kv[1]
		}
	}

	return ""
}
