package hook

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

	"gitlab.com/gitlab-org/gitaly/internal/gitaly/config"
	"gitlab.com/gitlab-org/gitaly/internal/version"
	"gitlab.com/gitlab-org/gitlab-shell/client"
)

// AllowedResponse is a response for the internal gitlab api's /allowed endpoint with a subset
// of fields
type AllowedResponse struct {
	Status  bool   `json:"status"`
	Message string `json:"message"`
}

// AllowedRequest is a request for the internal gitlab api /allowed endpoint
type AllowedRequest struct {
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

// AllowedParams compose set of parameters required to call 'GitlabAPI.Allowed' method.
type AllowedParams struct {
	// RepoPath is an absolute path to the repository.
	RepoPath string
	// GitObjectDirectory is a path to git object directory.
	GitObjectDirectory string
	// GitAlternateObjectDirectories are the paths to alternate object directories.
	GitAlternateObjectDirectories []string
	// GLRepository is a name of the repository.
	GLRepository string
	// GLID is an identifier of the repository.
	GLID string
	// GLProtocol is a protocol used for operation.
	GLProtocol string
	// Changes is a set of changes to be applied.
	Changes string
}

// GitlabAPI is an interface for accessing the gitlab internal API
type GitlabAPI interface {
	// Allowed queries the gitlab internal api /allowed endpoint to determine if a ref change for a given repository and user is allowed
	Allowed(ctx context.Context, params AllowedParams) (bool, string, error)
	// Check verifies that GitLab can be reached, and authenticated to
	Check(ctx context.Context) (*CheckInfo, error)
	// PreReceive queries the gitlab internal api /pre_receive to increase the reference counter
	PreReceive(ctx context.Context, glRepository string) (bool, error)
	// PostReceive queries the gitlab internal api /post_receive to decrease the reference counter
	PostReceive(ctx context.Context, glRepository, glID, changes string, pushOptions ...string) (bool, []PostReceiveMessage, error)
}

// gitlabAPI is a wrapper around client.GitlabNetClient with API methods for gitlab git receive hooks
type gitlabAPI struct {
	client *client.GitlabNetClient
}

// NewGitlabNetClient creates an HTTP client to talk to the Rails internal API
func NewGitlabNetClient(gitlabCfg config.Gitlab, tlsCfg config.TLS) (*client.GitlabNetClient, error) {
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

// NewGitlabAPI creates a GitLabAPI to talk to the Rails internal API
func NewGitlabAPI(gitlabCfg config.Gitlab, tlsCfg config.TLS) (GitlabAPI, error) {
	client, err := NewGitlabNetClient(gitlabCfg, tlsCfg)
	if err != nil {
		return nil, err
	}
	client.SetUserAgent(version.GetVersion())

	return &gitlabAPI{client: client}, nil
}

// Allowed checks if a ref change for a given repository is allowed through the gitlab internal api /allowed endpoint
func (a *gitlabAPI) Allowed(ctx context.Context, params AllowedParams) (bool, string, error) {
	gitObjDirVars, err := marshallGitObjectDirs(params.GitObjectDirectory, params.GitAlternateObjectDirectories)
	if err != nil {
		return false, "", fmt.Errorf("when getting git object directories json encoded string: %w", err)
	}

	req := AllowedRequest{
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

	resp, err := a.client.Post(ctx, "/allowed", &req)
	if err != nil {
		return false, "", err
	}

	defer func() {
		io.Copy(ioutil.Discard, resp.Body)
		resp.Body.Close()
	}()

	var response AllowedResponse

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
func (a *gitlabAPI) PreReceive(ctx context.Context, glRepository string) (bool, error) {
	resp, err := a.client.Post(ctx, "/pre_receive", map[string]string{"gl_repository": glRepository})
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

// PostReceiveResponse is the response the GitLab internal api provides on a successful /post_receive call
type PostReceiveResponse struct {
	ReferenceCounterDecreased bool                 `json:"reference_counter_decreased"`
	Messages                  []PostReceiveMessage `json:"messages"`
}

// PostReceiveMessage encapsulates a message from the /post_receive endpoint that gets printed to stdout
type PostReceiveMessage struct {
	Message string `json:"message"`
	Type    string `json:"type"`
}

// PostReceive decreases the reference counter for a push for a given gl_repository through the gitlab internal API /post_receive endpoint
func (a *gitlabAPI) PostReceive(ctx context.Context, glRepository, glID, changes string, pushOptions ...string) (bool, []PostReceiveMessage, error) {
	resp, err := a.client.Post(ctx, "/post_receive", map[string]interface{}{"gl_repository": glRepository, "identifier": glID, "changes": changes, "push_options": pushOptions})
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

	var result PostReceiveResponse

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return false, nil, fmt.Errorf("decoding response from /post_receive endpoint: %w", err)
	}

	return result.ReferenceCounterDecreased, result.Messages, nil
}

var glIDRegex = regexp.MustCompile(`\A[0-9]+\z`)

func (a *AllowedRequest) parseAndSetGLID(glID string) error {
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

// mockAPI is a noop gitlab API client
type mockAPI struct{}

func (m *mockAPI) Allowed(ctx context.Context, params AllowedParams) (bool, string, error) {
	return true, "", nil
}

func (m *mockAPI) Check(ctx context.Context) (*CheckInfo, error) {
	return &CheckInfo{
		Version:        "v13.5.0",
		Revision:       "deadbeef",
		APIVersion:     "v4",
		RedisReachable: true,
	}, nil
}

func (m *mockAPI) PreReceive(ctx context.Context, glRepository string) (bool, error) {
	return true, nil
}

func (m *mockAPI) PostReceive(ctx context.Context, glRepository, glID, changes string, gitPushOptions ...string) (bool, []PostReceiveMessage, error) {
	return true, nil, nil
}

// GitlabAPIStub is a global mock that can be used in testing
var GitlabAPIStub = &mockAPI{}
