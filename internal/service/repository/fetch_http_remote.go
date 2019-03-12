package repository

import (
	"context"
	"errors"
	"fmt"
	"net/url"

	grpc_logrus "github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus"
	"gitlab.com/gitlab-org/gitaly-proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/rubyserver"
)

func (s *server) FetchHTTPRemote(ctx context.Context, req *gitalypb.FetchHTTPRemoteRequest) (*gitalypb.FetchHTTPRemoteResponse, error) {
	if err := validateFetchHTTPRemoteRequest(req); err != nil {
		return nil, helper.ErrInvalidArgument(err)
	}

	authorizationKey := fmt.Sprintf("http.%s.extraHeader", req.GetRemote().GetUrl())
	authorizationValue := fmt.Sprintf("Authorization: %s", req.GetRemote().GetHttpAuthorizationHeader())

	if err := s.setAuthorization(ctx, req.GetRepository(), authorizationKey, authorizationValue); err != nil {
		return nil, helper.ErrInternal(err)
	}

	defer func() {
		if err := s.removeAuthorization(ctx, req.GetRepository(), authorizationKey); err != nil {
			grpc_logrus.Extract(ctx).WithError(err).Error("error removing authorization config")
		}
	}()

	repository := req.GetRepository()
	remoteName := req.GetRemote().GetName()
	remoteURL := req.GetRemote().GetUrl()

	if err := s.addRemote(ctx, repository, remoteName, remoteURL); err != nil {
		return nil, helper.ErrInternal(err)
	}

	if err := s.fetchRemote(ctx, repository, remoteName, req.GetTimeout()); err != nil {
		return nil, helper.ErrInternal(err)
	}

	return &gitalypb.FetchHTTPRemoteResponse{}, nil
}

func validateFetchHTTPRemoteRequest(req *gitalypb.FetchHTTPRemoteRequest) error {
	if req.GetRepository() == nil {
		return errors.New("repository is empty")
	}

	if req.GetRemote().GetUrl() == "" {
		return errors.New("missing remote url")
	}

	u, err := url.ParseRequestURI(req.GetRemote().GetUrl())
	if err != nil {
		return errors.New("invalid remote url")
	}

	if u.Scheme != "http" && u.Scheme != "https" {
		return errors.New("invalid remote url")
	}

	if req.GetRemote().GetName() == "" {
		return errors.New("missing remote name")
	}

	return nil
}

func (s *server) addRemote(ctx context.Context, repository *gitalypb.Repository, name, url string) error {
	client, err := s.RemoteServiceClient(ctx)
	if err != nil {
		return err
	}

	clientCtx, err := rubyserver.SetHeaders(ctx, repository)
	if err != nil {
		return err
	}

	if _, err = client.AddRemote(clientCtx, &gitalypb.AddRemoteRequest{
		Repository:    repository,
		Name:          name,
		Url:           url,
		MirrorRefmaps: []string{"all_refs"},
	}); err != nil {
		return err
	}

	return nil
}

func (s *server) fetchRemote(ctx context.Context, repository *gitalypb.Repository, remoteName string, timeout int32) error {
	fetchRemoteRequest := &gitalypb.FetchRemoteRequest{
		Repository: repository,
		Remote:     remoteName,
		Force:      false,
		Timeout:    timeout,
	}

	_, err := s.FetchRemote(ctx, fetchRemoteRequest)
	return err
}

func (s *server) setAuthorization(ctx context.Context, repository *gitalypb.Repository, key, value string) error {
	_, err := s.SetConfig(ctx, &gitalypb.SetConfigRequest{
		Repository: repository,
		Entries: []*gitalypb.SetConfigRequest_Entry{
			&gitalypb.SetConfigRequest_Entry{
				Key: key,
				Value: &gitalypb.SetConfigRequest_Entry_ValueStr{
					ValueStr: value,
				},
			},
		},
	})
	return err
}

func (s *server) removeAuthorization(ctx context.Context, repository *gitalypb.Repository, key string) error {
	_, err := s.DeleteConfig(ctx, &gitalypb.DeleteConfigRequest{
		Repository: repository,
		Keys:       []string{key},
	})
	return err
}
