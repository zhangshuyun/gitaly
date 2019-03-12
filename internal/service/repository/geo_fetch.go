package repository

import (
	"context"
	"errors"
	"fmt"

	grpc_logrus "github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus"
	"gitlab.com/gitlab-org/gitaly-proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/rubyserver"
)

func (s *server) GeoFetch(ctx context.Context, req *gitalypb.GeoFetchRequest) (*gitalypb.GeoFetchResponse, error) {
	if err := validateGeoFetchRequest(req); err != nil {
		return nil, helper.ErrInvalidArgument(err)
	}

	authorizationKey := fmt.Sprintf("http.%s.extraHeader", req.GetGeoRemote().GetUrl())
	authorizationValue := fmt.Sprintf("Authorization: %s", req.GetGeoRemote().GetHttpAuthorizationHeader())

	if err := s.setAuthorization(ctx, req.GetRepository(), authorizationKey, authorizationValue); err != nil {
		return nil, helper.ErrInternal(err)
	}

	defer func() {
		if err := s.removeAuthorization(ctx, req.GetRepository(), authorizationKey); err != nil {
			grpc_logrus.Extract(ctx).WithError(err).Error("error removing authorization config")
		}
	}()

	if err := s.addRemote(ctx, req.GetRepository(), req.GetGeoRemote().GetName(), req.GetGeoRemote().GetUrl()); err != nil {
		return nil, helper.ErrInternal(err)
	}

	if err := s.fetchRemote(ctx, req.GetRepository(), req.GetGeoRemote().GetName()); err != nil {
		return nil, helper.ErrInternal(err)
	}

	return &gitalypb.GeoFetchResponse{}, nil
}

func validateGeoFetchRequest(req *gitalypb.GeoFetchRequest) error {
	if req.GetRepository() == nil {
		return errors.New("repository is empty")
	}

	if req.GetGeoRemote().GetUrl() == "" {
		return errors.New("missing remote url")
	}

	if req.GetGeoRemote().GetName() == "" {
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

func (s *server) fetchRemote(ctx context.Context, repository *gitalypb.Repository, remoteName string) error {
	fetchRemoteRequest := &gitalypb.FetchRemoteRequest{
		Repository: repository,
		Remote:     remoteName,
		Force:      false,
		Timeout:    1000,
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
