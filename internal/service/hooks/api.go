package hook

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
)

type internalClient struct {
	client             *http.Client
	gitlabURL          string
	username, password string
	secret             string
}

type gitlabAccessStatus struct {
	StatusCode int    `json:"status_code"`
	Status     bool   `json:"status"`
	Message    string `json:"message"`
}

func newInternalClient(gitlabURL, secretPath string, selfSigned bool, caFile, caDir string) (*internalClient, error) {
	tlsConfig := &tls.Config{
		InsecureSkipVerify: selfSigned,
	}

	if caFile != "" {
		os.Setenv("SSL_CERT_FILE", caFile)
	}

	if caDir != "" {
		os.Setenv("SSL_CERT_DIR", caDir)
	}

	var secret string
	if secretPath != "" {
		secretBytes, err := ioutil.ReadFile(secretPath)
		if err != nil {
			return nil, err
		}
		secret = string(secretBytes)
	}

	transport := &http.Transport{TLSClientConfig: tlsConfig}

	if strings.HasPrefix(gitlabURL, "http://unix") {
		transport = &http.Transport{
			DialContext: func(_ context.Context, _, _ string) (net.Conn, error) {
				socket := strings.TrimPrefix(gitlabURL, "http://unix")
				return net.Dial("unix", socket)
			},
		}
	}
	return &internalClient{
		client:    &http.Client{Transport: transport},
		gitlabURL: gitlabURL,
		secret:    secret,
	}, nil
}

const internalURL = "http://unix/api/v4/internal"

func parseGitObjVars(repoPath string, env []string) (string, error) {
	envMap := map[string]interface{}{}

	for _, v := range env {
		kv := strings.SplitN(v, "=", 2)
		k := kv[0]
		v := kv[1]

		if k == "GIT_OBJECT_DIRECTORY" {
			gitObjDirRel, err := filepath.Rel(repoPath, v)
			if err != nil {
				return "", err
			}
			envMap["GIT_OBJECT_DIRECTORY_RELATIVE"] = gitObjDirRel
			continue
		}

		if k == "GIT_ALTERNATE_OBJECT_DIRECTORIES" {
			var gitAltObjRelDirs []string

			for _, gitAltObjDir := range strings.Split(v, ":") {
				gitAltObjDirRel, err := filepath.Rel(repoPath, gitAltObjDir)
				if err != nil {
					return "", err
				}
				gitAltObjRelDirs = append(gitAltObjRelDirs, gitAltObjDirRel)
			}

			envMap["GIT_ALTERNATE_OBJECT_DIRECTORIES_RELATIVE"] = gitAltObjRelDirs
			continue
		}
	}

	output, err := json.Marshal(envMap)

	return string(output), err
}

func getVar(vars []string, s string) string {
	for _, v := range vars {
		if strings.HasPrefix(v, s+"=") {
			return strings.TrimPrefix(v, s+"=")
		}
	}

	return ""
}

func (i *internalClient) checkAccess(repoPath, changes string, env []string) (bool, string, error) {
	form := url.Values{}
	form.Set("action", "git-receive-pack")
	form.Set("changes", changes)
	form.Set("gl_repository", getVar(env, "GL_REPOSITORY"))
	form.Set("project", strings.Replace(repoPath, "'", "", -1))
	form.Set("protocol", getVar(env, "GL_PROTOCOL"))
	form.Set("secret_token", i.secret)

	glIDKey, glIDValue, err := parseGLID(getVar(env, "GL_ID"))
	if err != nil {
		return false, "", err
	}
	form.Set(glIDKey, glIDValue)

	envVars, err := parseGitObjVars(repoPath, env)
	form.Set("env", envVars)

	r, err := http.NewRequest(http.MethodPost, internalURL+"/allowed", strings.NewReader(form.Encode()))
	if err != nil {
		return false, "", err
	}

	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := i.post(r)
	if err != nil {
		return false, "", err
	}

	var allowedResponse gitlabAccessStatus

	if err = json.NewDecoder(resp.Body).Decode(&allowedResponse); err != nil {
		return false, "", err
	}

	switch resp.StatusCode {
	case http.StatusOK, http.StatusMultipleChoices, http.StatusUnauthorized, http.StatusNotFound, http.StatusServiceUnavailable:
		if resp.Header.Get("Content-Type") == "application/json" {
			return allowedResponse.Status, allowedResponse.Message, nil
		}
	}

	return false, "API Inaccessible", nil
}

func (i *internalClient) post(r *http.Request) (*http.Response, error) {
	if i.username != "" && i.password != "" {
		r.SetBasicAuth(i.username, i.password)
	}
	return i.client.Do(r)
}

func parseGLID(glID string) (glIDKey string, glIDValue string, err error) {
	switch {
	case strings.HasPrefix(glID, "key-"):
		glIDKey = "key_id"
		glIDValue = strings.Replace(glID, "key-", "", 1)
	case strings.HasPrefix(glID, "user-"):
		glIDKey = "user_id"
		glIDValue = strings.Replace(glID, "user-", "", 1)
	case strings.HasPrefix(glID, "username-"):
		glIDKey = "username"
		glIDValue = strings.Replace(glID, "username-", "", 1)
	default:
		err = fmt.Errorf("gl_id='%s' is invalid")
	}

	return
}
