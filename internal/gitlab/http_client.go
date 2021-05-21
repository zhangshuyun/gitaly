package gitlab

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"mime"
	"net/http"
	"net/url"
	"regexp"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	promcfg "gitlab.com/gitlab-org/gitaly/internal/gitaly/config/prometheus"
	"gitlab.com/gitlab-org/gitaly/internal/prometheus/metrics"
	"gitlab.com/gitlab-org/gitaly/internal/version"
	"gitlab.com/gitlab-org/gitlab-shell/client"
)

var glIDRegex = regexp.MustCompile(`\A[0-9]+\z`)

// HTTPClient is an HTTP client used to talk to the internal GitLab Rails API.
type HTTPClient struct {
	*client.GitlabNetClient
	latencyMetric metrics.HistogramVec
}

// NewHTTPClient creates an HTTP client to talk to the Rails internal API
func NewHTTPClient(
	gitlabCfg config.Gitlab,
	tlsCfg config.TLS,
	promCfg promcfg.Config,
) (*HTTPClient, error) {
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

	return &HTTPClient{
		GitlabNetClient: gitlabnetClient,
		latencyMetric: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "gitaly_gitlab_api_latency_seconds",
				Help:    "Latency between posting to GitLab's `/internal/` APIs and receiving a response",
				Buckets: promCfg.GRPCLatencyBuckets,
			},
			[]string{"endpoint"},
		),
	}, nil
}

// Describe describes Prometheus metrics exposed by the HTTPClient.
func (c *HTTPClient) Describe(descs chan<- *prometheus.Desc) {
	prometheus.DescribeByCollect(c, descs)
}

// Collect collects Prometheus metrics exposed by the HTTPClient.
func (c *HTTPClient) Collect(metrics chan<- prometheus.Metric) {
	c.latencyMetric.Collect(metrics)
}

// allowedRequest is a request for the internal gitlab api /allowed endpoint
type allowedRequest struct {
	Action       string `json:"action,omitempty"`
	GLRepository string `json:"gl_repository,omitempty"`
	Project      string `json:"project,omitempty"`
	Changes      string `json:"changes,omitempty"`
	Protocol     string `json:"protocol,omitempty"`
	Env          string `json:"env,omitempty"`
	Username     string `json:"username,omitempty"`
	KeyID        string `json:"key_id,omitempty"`
	UserID       string `json:"user_id,omitempty"`
}

func (a *allowedRequest) parseAndSetGLID(glID string) error {
	var value string

	switch {
	case strings.HasPrefix(glID, "username-"):
		a.Username = strings.TrimPrefix(glID, "username-")
		return nil
	case strings.HasPrefix(glID, "key-"):
		a.KeyID = strings.TrimPrefix(glID, "key-")
		value = a.KeyID
	case strings.HasPrefix(glID, "user-"):
		a.UserID = strings.TrimPrefix(glID, "user-")
		value = a.UserID
	}

	if !glIDRegex.MatchString(value) {
		return fmt.Errorf("gl_id='%s' is invalid", glID)
	}

	return nil
}

// allowedResponse is a response for the internal gitlab api's /allowed endpoint with a subset
// of fields
type allowedResponse struct {
	Status  bool   `json:"status"`
	Message string `json:"message"`
}

// Allowed checks if a ref change for a given repository is allowed through the gitlab internal api /allowed endpoint
func (c *HTTPClient) Allowed(ctx context.Context, params AllowedParams) (bool, string, error) {
	defer prometheus.NewTimer(c.latencyMetric.WithLabelValues("allowed")).ObserveDuration()

	gitObjDirVars, err := marshallGitObjectDirs(params.GitObjectDirectory, params.GitAlternateObjectDirectories)
	if err != nil {
		return false, "", fmt.Errorf("when getting git object directories json encoded string: %w", err)
	}

	req := allowedRequest{
		Action:       "git-receive-pack",
		GLRepository: params.GLRepository,
		Changes:      params.Changes,
		Protocol:     params.GLProtocol,
		Project:      strings.Replace(params.RepoPath, "'", "", -1),
		Env:          gitObjDirVars,
	}

	if err := req.parseAndSetGLID(params.GLID); err != nil {
		return false, "", fmt.Errorf("setting gl_id: %w", err)
	}

	resp, err := c.Post(ctx, "/allowed", &req)
	if err != nil {
		return false, "", err
	}

	defer func() {
		io.Copy(ioutil.Discard, resp.Body)
		resp.Body.Close()
	}()

	var response allowedResponse

	switch resp.StatusCode {
	case http.StatusOK,
		http.StatusMultipleChoices:

		mtype, _, err := mime.ParseMediaType(resp.Header.Get("Content-Type"))
		if err != nil {
			return false, "", fmt.Errorf("/allowed endpoint respond with unsupported content type: %w", err)
		}

		if mtype != "application/json" {
			return false, "", fmt.Errorf("/allowed endpoint respond with unsupported content type: %s", mtype)
		}

		if err = json.NewDecoder(resp.Body).Decode(&response); err != nil {
			return false, "", fmt.Errorf("decoding response from /allowed endpoint: %w", err)
		}
	default:
		return false, "", fmt.Errorf("gitlab api is not accessible: %d", resp.StatusCode)
	}

	return response.Status, response.Message, nil
}

type preReceiveResponse struct {
	ReferenceCounterIncreased bool `json:"reference_counter_increased"`
}

// PreReceive increases the reference counter for a push for a given gl_repository through the gitlab internal API /pre_receive endpoint
func (c *HTTPClient) PreReceive(ctx context.Context, glRepository string) (bool, error) {
	defer prometheus.NewTimer(c.latencyMetric.WithLabelValues("pre-receive")).ObserveDuration()

	resp, err := c.Post(ctx, "/pre_receive", map[string]string{"gl_repository": glRepository})
	if err != nil {
		return false, fmt.Errorf("http post to gitlab api /pre_receive endpoint: %w", err)
	}

	defer func() {
		io.Copy(ioutil.Discard, resp.Body)
		resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK {
		return false, fmt.Errorf("pre-receive call failed with status: %d", resp.StatusCode)
	}

	mtype, _, err := mime.ParseMediaType(resp.Header.Get("Content-Type"))
	if err != nil {
		return false, fmt.Errorf("/pre_receive endpoint respond with unsupported content type: %w", err)
	}

	if mtype != "application/json" {
		return false, fmt.Errorf("/pre_receive endpoint respond with unsupported content type: %s", mtype)
	}

	var result preReceiveResponse

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return false, fmt.Errorf("decoding response from /pre_receive endpoint: %w", err)
	}

	return result.ReferenceCounterIncreased, nil
}

// postReceiveResponse is the response the GitLab internal api provides on a successful /post_receive call
type postReceiveResponse struct {
	ReferenceCounterDecreased bool                 `json:"reference_counter_decreased"`
	Messages                  []PostReceiveMessage `json:"messages"`
}

// PostReceive decreases the reference counter for a push for a given gl_repository through the gitlab internal API /post_receive endpoint
func (c *HTTPClient) PostReceive(ctx context.Context, glRepository, glID, changes string, pushOptions ...string) (bool, []PostReceiveMessage, error) {
	defer prometheus.NewTimer(c.latencyMetric.WithLabelValues("post-receive")).ObserveDuration()

	resp, err := c.Post(ctx, "/post_receive", map[string]interface{}{"gl_repository": glRepository, "identifier": glID, "changes": changes, "push_options": pushOptions})
	if err != nil {
		return false, nil, fmt.Errorf("http post to gitlab api /post_receive endpoint: %w", err)
	}

	defer func() {
		io.Copy(ioutil.Discard, resp.Body)
		resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK {
		return false, nil, fmt.Errorf("post-receive call failed with status: %d", resp.StatusCode)
	}

	mtype, _, err := mime.ParseMediaType(resp.Header.Get("Content-Type"))
	if err != nil {
		return false, nil, fmt.Errorf("/post_receive endpoint respond with invalid content type: %w", err)
	}

	if mtype != "application/json" {
		return false, nil, fmt.Errorf("/post_receive endpoint respond with unsupported content type: %s", mtype)
	}

	var result postReceiveResponse

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return false, nil, fmt.Errorf("decoding response from /post_receive endpoint: %w", err)
	}

	return result.ReferenceCounterDecreased, result.Messages, nil
}

// Check performs an HTTP request to the internal/check API endpoint to verify
// the connection and tokens. It returns basic information of the installed
// GitLab
func (c *HTTPClient) Check(ctx context.Context) (*CheckInfo, error) {
	defer prometheus.NewTimer(c.latencyMetric.WithLabelValues("check")).ObserveDuration()

	resp, err := c.Get(ctx, "/check")
	if err != nil {
		return nil, fmt.Errorf("HTTP GET to GitLab endpoint /check failed: %w", err)
	}

	defer func() {
		io.Copy(ioutil.Discard, resp.Body)
		resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("Check HTTP request failed with status: %d", resp.StatusCode)
	}

	var info CheckInfo
	if err := json.NewDecoder(resp.Body).Decode(&info); err != nil {
		return nil, fmt.Errorf("failed to decode response from /check endpoint: %w", err)
	}

	return &info, nil
}

// marshallGitObjectDirs generates a json encoded string containing GIT_OBJECT_DIRECTORY_RELATIVE, and GIT_ALTERNATE_OBJECT_DIRECTORIES_RELATIVE
func marshallGitObjectDirs(gitObjectDirRel string, gitAltObjectDirsRel []string) (string, error) {
	envString, err := json.Marshal(map[string]interface{}{
		"GIT_OBJECT_DIRECTORY_RELATIVE":             gitObjectDirRel,
		"GIT_ALTERNATE_OBJECT_DIRECTORIES_RELATIVE": gitAltObjectDirsRel,
	})

	if err != nil {
		return "", err
	}

	return string(envString), nil
}
