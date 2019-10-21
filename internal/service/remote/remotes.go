package remote

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"strings"

	"gitlab.com/gitlab-org/gitaly/internal/git/remote"
	"gitlab.com/gitlab-org/gitaly/internal/rubyserver"

	"github.com/prometheus/client_golang/prometheus"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/helper/chunk"
	"gitlab.com/gitlab-org/gitaly/internal/metadata/featureflag"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var addRemoteRequests = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "gitaly_add_remote_total",
		Help: "Counter of go vs ruby implementation of AddRemote",
	},
	[]string{"implementation"},
)

func init() {
	prometheus.MustRegister(addRemoteRequests)
}

// AddRemote adds a remote to the repository
func (s *server) AddRemote(ctx context.Context, req *gitalypb.AddRemoteRequest) (*gitalypb.AddRemoteResponse, error) {
	if err := validateAddRemoteRequest(req); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "AddRemote: %v", err)
	}

	if featureflag.IsEnabled(ctx, featureflag.AddRemoteGo) {
		addRemoteRequests.WithLabelValues("go").Inc()

		return addRemote(ctx, req)
	}

	addRemoteRequests.WithLabelValues("ruby").Inc()

	client, err := s.ruby.RemoteServiceClient(ctx)
	if err != nil {
		return nil, err
	}

	clientCtx, err := rubyserver.SetHeaders(ctx, req.GetRepository())
	if err != nil {
		return nil, err
	}

	return client.AddRemote(clientCtx, req)
}

func addRemote(ctx context.Context, req *gitalypb.AddRemoteRequest) (*gitalypb.AddRemoteResponse, error) {
	err := remote.Add(ctx, req.GetRepository(), req.Name, req.Url, req.GetMirrorRefmaps())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "AddRemote: %v", err)
	}

	return &gitalypb.AddRemoteResponse{}, nil
}

func validateAddRemoteRequest(req *gitalypb.AddRemoteRequest) error {
	if strings.TrimSpace(req.GetName()) == "" {
		return fmt.Errorf("empty remote name")
	}
	if req.GetUrl() == "" {
		return fmt.Errorf("empty remote url")
	}

	return nil
}

// RemoveRemote removes the given remote
func (s *server) RemoveRemote(ctx context.Context, req *gitalypb.RemoveRemoteRequest) (*gitalypb.RemoveRemoteResponse, error) {
	if err := validateRemoveRemoteRequest(req); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "RemoveRemote: %v", err)
	}

	hasRemote, err := remote.Exists(ctx, req.GetRepository(), req.Name)
	if err != nil {
		return nil, err
	}
	if !hasRemote {
		return &gitalypb.RemoveRemoteResponse{Result: false}, nil
	}

	if err := remote.Remove(ctx, req.GetRepository(), req.Name); err != nil {
		return nil, err
	}

	return &gitalypb.RemoveRemoteResponse{Result: true}, nil
}

func (s *server) FindRemoteRepository(ctx context.Context, req *gitalypb.FindRemoteRepositoryRequest) (*gitalypb.FindRemoteRepositoryResponse, error) {
	if req.GetRemote() == "" {
		return nil, status.Error(codes.InvalidArgument, "FindRemoteRepository: empty remote can't be checked.")
	}

	cmd, err := git.CommandWithoutRepo(ctx, "ls-remote", req.GetRemote(), "HEAD")

	if err != nil {
		return nil, status.Errorf(codes.Internal, "error executing git command: %s", err)
	}

	output, err := ioutil.ReadAll(cmd)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "unable to read stdout: %s", err)
	}
	if err := cmd.Wait(); err != nil {
		return &gitalypb.FindRemoteRepositoryResponse{Exists: false}, nil
	}

	// The output of a successful command is structured like
	// Regexp would've read better, but this is faster
	// 58fbff2e0d3b620f591a748c158799ead87b51cd	HEAD
	fields := bytes.Fields(output)
	match := len(fields) == 2 && len(fields[0]) == 40 && string(fields[1]) == "HEAD"

	return &gitalypb.FindRemoteRepositoryResponse{Exists: match}, nil
}

func validateRemoveRemoteRequest(req *gitalypb.RemoveRemoteRequest) error {
	if req.GetName() == "" {
		return fmt.Errorf("empty remote name")
	}

	return nil
}

func (s *server) ListRemotes(req *gitalypb.ListRemotesRequest, stream gitalypb.RemoteService_ListRemotesServer) error {
	repo := req.GetRepository()

	ctx := stream.Context()
	cmd, err := git.Command(ctx, repo, "remote", "-v")
	if err != nil {
		return err
	}

	scanner := bufio.NewScanner(cmd)
	remotesMap := make(map[string]*gitalypb.ListRemotesResponse_Remote)

	for scanner.Scan() {
		text := scanner.Text()
		splitLine := strings.Fields(text)
		if len(splitLine) != 3 {
			continue
		}

		remote := &gitalypb.ListRemotesResponse_Remote{Name: splitLine[0]}
		if splitLine[2] == "(fetch)" {
			remote.FetchUrl = splitLine[1]
		} else if splitLine[2] == "(push)" {
			remote.PushUrl = splitLine[1]
		}

		oldRemote := remotesMap[splitLine[0]]
		remotesMap[splitLine[0]] = mergeGitalyRemote(oldRemote, remote)
	}

	sender := chunk.New(&listRemotesSender{stream: stream})
	for _, remote := range remotesMap {
		if err := sender.Send(remote); err != nil {
			return err
		}
	}

	return sender.Flush()
}

func mergeGitalyRemote(oldRemote *gitalypb.ListRemotesResponse_Remote, newRemote *gitalypb.ListRemotesResponse_Remote) *gitalypb.ListRemotesResponse_Remote {
	if oldRemote == nil {
		return &gitalypb.ListRemotesResponse_Remote{Name: newRemote.Name, FetchUrl: newRemote.FetchUrl, PushUrl: newRemote.PushUrl}
	}

	newRemoteInstance := &gitalypb.ListRemotesResponse_Remote{Name: oldRemote.Name, FetchUrl: oldRemote.FetchUrl, PushUrl: oldRemote.PushUrl}
	if newRemote.Name != "" {
		newRemoteInstance.Name = newRemote.Name
	}

	if newRemote.PushUrl != "" {
		newRemoteInstance.PushUrl = newRemote.PushUrl
	}

	if newRemote.FetchUrl != "" {
		newRemoteInstance.FetchUrl = newRemote.PushUrl
	}

	return newRemoteInstance
}

type listRemotesSender struct {
	stream  gitalypb.RemoteService_ListRemotesServer
	remotes []*gitalypb.ListRemotesResponse_Remote
}

func (l *listRemotesSender) Append(it chunk.Item) {
	l.remotes = append(l.remotes, it.(*gitalypb.ListRemotesResponse_Remote))
}

func (l *listRemotesSender) Send() error {
	return l.stream.Send(&gitalypb.ListRemotesResponse{Remotes: l.remotes})
}

func (l *listRemotesSender) Reset() { l.remotes = nil }
