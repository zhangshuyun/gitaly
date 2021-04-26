package gitlab

import (
	"fmt"
	"io/ioutil"
	"net/url"

	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/version"
	"gitlab.com/gitlab-org/gitlab-shell/client"
)

// NewHTTPClient creates an HTTP client to talk to the Rails internal API
func NewHTTPClient(gitlabCfg config.Gitlab, tlsCfg config.TLS) (*client.GitlabNetClient, error) {
	url, err := url.PathUnescape(gitlabCfg.URL)
	if err != nil {
		return nil, err
	}

	var opts []client.HTTPClientOpt
	if tlsCfg.CertPath != "" && tlsCfg.KeyPath != "" {
		opts = append(opts, client.WithClientCert(tlsCfg.CertPath, tlsCfg.KeyPath))
	}

	httpClient, err := client.NewHTTPClientWithOpts(
		url,
		gitlabCfg.RelativeURLRoot,
		gitlabCfg.HTTPSettings.CAFile,
		gitlabCfg.HTTPSettings.CAPath,
		gitlabCfg.HTTPSettings.SelfSigned,
		uint64(gitlabCfg.HTTPSettings.ReadTimeout),
		opts,
	)
	if err != nil {
		return nil, fmt.Errorf("building new HTTP client for GitLab API: %w", err)
	}

	if httpClient == nil {
		return nil, fmt.Errorf("%s is not a valid url", gitlabCfg.URL)
	}

	secret, err := ioutil.ReadFile(gitlabCfg.SecretFile)
	if err != nil {
		return nil, fmt.Errorf("reading secret file: %w", err)
	}

	gitlabnetClient, err := client.NewGitlabNetClient(gitlabCfg.HTTPSettings.User, gitlabCfg.HTTPSettings.Password, string(secret), httpClient)
	if err != nil {
		return nil, fmt.Errorf("instantiating gitlab net client: %w", err)
	}

	gitlabnetClient.SetUserAgent("gitaly/" + version.GetVersion())

	return gitlabnetClient, nil
}
