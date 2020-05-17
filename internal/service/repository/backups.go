package repository

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"strconv"

	"gitlab.com/gitlab-org/gitaly/internal/helper"
	"gitlab.com/gitlab-org/gitaly/internal/service/repository/bundle"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
)

func (s *server) CreateBackup(ctx context.Context, in *gitalypb.CreateBackupRequest) (*gitalypb.CreateBackupResponse, error) {
	backupData, bundleFileWriter, err := bundle.Create(ctx, in.GetRepository())
	if err != nil {
		return nil, helper.ErrInternalf("creating gitaly bundle: %w", err)
	}
	defer bundleFileWriter.Close()

	if backupData == nil {
		return &gitalypb.CreateBackupResponse{}, nil
	}

	req, err := http.NewRequest(http.MethodPost, in.GetPostUrl(), bytes.NewReader(backupData))
	if err != nil {
		return nil, helper.ErrInternalf("creating post request: %w", err)
	}

	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", in.GetToken()))
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("Content-Length", strconv.Itoa(len(backupData)))

	var client http.Client

	resp, err := client.Do(req)
	if err != nil {
		return nil, helper.ErrInternalf("sending post request: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		bundleFileWriter.Close()
		return nil, helper.ErrInternalf("failed to send data to endpoint with code: %d", resp.StatusCode)
	}

	if err := bundleFileWriter.Commit(); err != nil {
		return nil, helper.ErrInternalf("writing bundle ref file: %w", err)
	}

	return &gitalypb.CreateBackupResponse{}, nil
}

func (s *server) RestoreFromBackup(ctx context.Context, in *gitalypb.RestoreFromBackupRequest) (*gitalypb.RestoreFromBackupResponse, error) {
	req, err := http.NewRequest(http.MethodGet, in.GetUrl(), nil)
	if err != nil {
		return nil, helper.ErrInternalf("http get %v: %w", in.GetUrl(), err)
	}
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", in.GetToken()))

	var c http.Client

	resp, err := c.Do(req)
	if err != nil {
		return nil, helper.ErrInternalf("http get %v: %w", in.GetUrl(), err)
	}

	defer resp.Body.Close()

	if err = bundle.Unpack(ctx, in.GetRepository(), resp.Body); err != nil {
		return nil, helper.ErrInternalf("restoring backup: %w", err)
	}

	return &gitalypb.RestoreFromBackupResponse{}, nil
}
