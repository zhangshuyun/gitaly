package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"path/filepath"

	"github.com/git-lfs/git-lfs/lfs"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitaly/config/prometheus"
	"gitlab.com/gitlab-org/gitaly/v14/internal/gitlab"
	gitalylog "gitlab.com/gitlab-org/gitaly/v14/internal/log"
	"gitlab.com/gitlab-org/labkit/log"
	"gitlab.com/gitlab-org/labkit/tracing"
)

type configProvider interface {
	Get(key string) string
}

func initLogging(p configProvider) (io.Closer, error) {
	path := p.Get(gitalylog.GitalyLogDirEnvKey)
	if path == "" {
		return nil, nil
	}

	filepath := filepath.Join(path, "gitaly_lfs_smudge.log")

	return log.Initialize(
		log.WithFormatter("json"),
		log.WithLogLevel("info"),
		log.WithOutputName(filepath),
	)
}

func smudge(to io.Writer, from io.Reader, cfgProvider configProvider) error {
	output, err := handleSmudge(to, from, cfgProvider)
	if err != nil {
		log.WithError(err).Error(err)
		return err
	}

	_, copyErr := io.Copy(to, output)
	if copyErr != nil {
		log.WithError(err).Error(copyErr)
		return copyErr
	}

	return nil
}

func handleSmudge(to io.Writer, from io.Reader, config configProvider) (io.Reader, error) {
	// Since the environment is sanitized at the moment, we're only
	// using this to extract the correlation ID. The finished() call
	// to clean up the tracing will be a NOP here.
	ctx, finished := tracing.ExtractFromEnv(context.Background())
	defer finished()

	logger := log.ContextLogger(ctx)

	ptr, contents, err := lfs.DecodeFrom(from)
	if err != nil {
		// This isn't a valid LFS pointer. Just copy the existing pointer data.
		return contents, nil
	}

	logger.WithField("oid", ptr.Oid).Debug("decoded LFS OID")

	glCfg, tlsCfg, glRepository, err := loadConfig(config)
	if err != nil {
		return contents, err
	}

	logger.WithField("gitlab_config", glCfg).
		WithField("gitaly_tls_config", tlsCfg).
		Debug("loaded GitLab API config")

	client, err := gitlab.NewHTTPClient(glCfg, tlsCfg, prometheus.Config{})
	if err != nil {
		return contents, err
	}

	qs := url.Values{}
	qs.Set("oid", ptr.Oid)
	qs.Set("gl_repository", glRepository)
	u := url.URL{Path: "/lfs", RawQuery: qs.Encode()}

	response, err := client.Get(ctx, u.String())
	if err != nil {
		return contents, fmt.Errorf("error loading LFS object: %v", err)
	}

	if response.StatusCode == 200 {
		return response.Body, nil
	}

	return contents, nil
}

func loadConfig(cfgProvider configProvider) (config.Gitlab, config.TLS, string, error) {
	var cfg config.Gitlab
	var tlsCfg config.TLS

	glRepository := cfgProvider.Get("GL_REPOSITORY")
	if glRepository == "" {
		return cfg, tlsCfg, "", fmt.Errorf("error loading project: GL_REPOSITORY is not defined")
	}

	u := cfgProvider.Get("GL_INTERNAL_CONFIG")
	if u == "" {
		return cfg, tlsCfg, glRepository, fmt.Errorf("unable to retrieve GL_INTERNAL_CONFIG")
	}

	if err := json.Unmarshal([]byte(u), &cfg); err != nil {
		return cfg, tlsCfg, glRepository, fmt.Errorf("unable to unmarshal GL_INTERNAL_CONFIG: %v", err)
	}

	u = cfgProvider.Get("GITALY_TLS")
	if u == "" {
		return cfg, tlsCfg, glRepository, errors.New("unable to retrieve GITALY_TLS")
	}

	if err := json.Unmarshal([]byte(u), &tlsCfg); err != nil {
		return cfg, tlsCfg, glRepository, fmt.Errorf("unable to unmarshal GITALY_TLS: %w", err)
	}

	return cfg, tlsCfg, glRepository, nil
}
