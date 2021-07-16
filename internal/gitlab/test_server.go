package gitlab

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
)

var changeLineRegex = regexp.MustCompile("^[a-f0-9]{40} [a-f0-9]{40} refs/[^ ]+$")

const secretHeaderName = "Gitlab-Shared-Secret"

// WriteShellSecretFile writes a .gitlab_shell_secret file in the specified directory
func WriteShellSecretFile(t testing.TB, dir, secretToken string) string {
	t.Helper()

	require.NoError(t, os.MkdirAll(dir, os.ModeDir))
	filePath := filepath.Join(dir, ".gitlab_shell_secret")
	require.NoError(t, ioutil.WriteFile(filePath, []byte(secretToken), 0644))
	return filePath
}

// TestServerOptions is a config for a mock gitlab server containing expected values
type TestServerOptions struct {
	User, Password, SecretToken string
	GLID                        string
	GLRepository                string
	Changes                     string
	PostReceiveMessages         []string
	PostReceiveAlerts           []string
	PostReceiveCounterDecreased bool
	UnixSocket                  bool
	LfsStatusCode               int
	LfsOid                      string
	LfsBody                     string
	Protocol                    string
	GitPushOptions              []string
	GitObjectDir                string
	GitAlternateObjectDirs      []string
	RepoPath                    string
	RelativeURLRoot             string
	GlRepository                string
	ClientCACertPath            string // used to verify client certs are valid
	ServerCertPath              string
	ServerKeyPath               string
}

// NewTestServer returns a mock gitlab server that responds to the hook api endpoints
func NewTestServer(t testing.TB, options TestServerOptions) (url string, cleanup func()) {
	t.Helper()

	mux := http.NewServeMux()
	prefix := strings.TrimRight(options.RelativeURLRoot, "/") + "/api/v4/internal"
	mux.Handle(prefix+"/allowed", http.HandlerFunc(handleAllowed(t, options)))
	mux.Handle(prefix+"/pre_receive", http.HandlerFunc(handlePreReceive(t, options)))
	mux.Handle(prefix+"/post_receive", http.HandlerFunc(handlePostReceive(options)))
	mux.Handle(prefix+"/check", http.HandlerFunc(handleCheck(t, options)))
	mux.Handle(prefix+"/lfs", http.HandlerFunc(handleLfs(t, options)))

	var tlsCfg *tls.Config
	if options.ClientCACertPath != "" {
		caCertPEM, err := ioutil.ReadFile(options.ClientCACertPath)
		require.NoError(t, err)

		certPool := x509.NewCertPool()
		require.True(t, certPool.AppendCertsFromPEM(caCertPEM))

		serverCert, err := tls.LoadX509KeyPair(options.ServerCertPath, options.ServerKeyPath)
		require.NoError(t, err)

		tlsCfg = &tls.Config{
			ClientCAs:    certPool,
			ClientAuth:   tls.RequireAndVerifyClientCert,
			Certificates: []tls.Certificate{serverCert},
			MinVersion:   tls.VersionTLS12,
		}
	}

	if options.UnixSocket {
		return startSocketHTTPServer(t, mux, tlsCfg)
	}

	var server *httptest.Server
	if tlsCfg == nil {
		server = httptest.NewServer(mux)
	} else {
		server = httptest.NewUnstartedServer(mux)
		server.TLS = tlsCfg
		server.StartTLS()
	}
	return server.URL, server.Close
}

func preReceiveFormToMap(u url.Values) map[string]string {
	return map[string]string{
		"action":        u.Get("action"),
		"gl_repository": u.Get("gl_repository"),
		"project":       u.Get("project"),
		"changes":       u.Get("changes"),
		"protocol":      u.Get("protocol"),
		"env":           u.Get("env"),
		"username":      u.Get("username"),
		"key_id":        u.Get("key_id"),
		"user_id":       u.Get("user_id"),
	}
}

type postReceiveForm struct {
	GLRepository   string   `json:"gl_repository"`
	SecretToken    string   `json:"secret_token"`
	Changes        string   `json:"changes"`
	Identifier     string   `json:"identifier"`
	GitPushOptions []string `json:"push_options"`
}

func parsePostReceiveForm(u url.Values) postReceiveForm {
	return postReceiveForm{
		GLRepository:   u.Get("gl_repository"),
		SecretToken:    u.Get("secret_token"),
		Changes:        u.Get("changes"),
		Identifier:     u.Get("identifier"),
		GitPushOptions: u["push_options[]"],
	}
}

func handleAllowed(t testing.TB, options TestServerOptions) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			http.Error(w, "could not parse form", http.StatusBadRequest)
			return
		}

		if r.Method != http.MethodPost {
			http.Error(w, "method not POST", http.StatusMethodNotAllowed)
			return
		}

		params := make(map[string]string)

		switch r.Header.Get("Content-Type") {
		case "application/x-www-form-urlencoded":
			params = preReceiveFormToMap(r.Form)
		case "application/json":
			if err := json.NewDecoder(r.Body).Decode(&params); err != nil {
				http.Error(w, "could not unmarshal json body", http.StatusBadRequest)
				return
			}
		}

		user, password, _ := r.BasicAuth()
		if user != options.User || password != options.Password {
			http.Error(w, "user or password incorrect", http.StatusUnauthorized)
			return
		}

		if options.GLID != "" {
			glidSplit := strings.SplitN(options.GLID, "-", 2)
			if len(glidSplit) != 2 {
				http.Error(w, "gl_id invalid", http.StatusUnauthorized)
				return
			}

			glKey, glVal := glidSplit[0], glidSplit[1]

			var glIDMatches bool
			switch glKey {
			case "user":
				glIDMatches = glVal == params["user_id"]
			case "key":
				glIDMatches = glVal == params["key_id"]
			case "username":
				glIDMatches = glVal == params["username"]
			default:
				http.Error(w, "gl_id invalid", http.StatusUnauthorized)
				return
			}

			if !glIDMatches {
				http.Error(w, "gl_id invalid", http.StatusUnauthorized)
				return
			}
		}

		if params["gl_repository"] == "" {
			http.Error(w, "gl_repository is empty", http.StatusUnauthorized)
			return
		}

		if options.GLRepository != "" {
			if params["gl_repository"] != options.GLRepository {
				http.Error(w, "gl_repository is invalid", http.StatusUnauthorized)
				return
			}
		}

		if params["protocol"] == "" {
			http.Error(w, "protocol is empty", http.StatusUnauthorized)
			return
		}

		if options.Protocol != "" {
			if params["protocol"] != options.Protocol {
				http.Error(w, "protocol is invalid", http.StatusUnauthorized)
				return
			}
		}

		if options.Changes != "" {
			if params["changes"] != options.Changes {
				http.Error(w, "changes is invalid", http.StatusUnauthorized)
				return
			}
		} else {
			changeLines := strings.Split(strings.TrimSuffix(params["changes"], "\n"), "\n")
			for _, line := range changeLines {
				if !changeLineRegex.MatchString(line) {
					http.Error(w, "changes is invalid", http.StatusUnauthorized)
					return
				}
			}
		}

		env := params["env"]
		if len(env) == 0 {
			http.Error(w, "env is empty", http.StatusUnauthorized)
			return
		}

		var gitVars struct {
			GitAlternateObjectDirsRel []string `json:"GIT_ALTERNATE_OBJECT_DIRECTORIES_RELATIVE"`
			GitObjectDirRel           string   `json:"GIT_OBJECT_DIRECTORY_RELATIVE"`
		}

		w.Header().Set("Content-Type", "application/json")

		if err := json.Unmarshal([]byte(env), &gitVars); err != nil {
			http.Error(w, "could not unmarshal env", http.StatusUnauthorized)
			return
		}

		if options.GitObjectDir != "" {
			relObjectDir, err := filepath.Rel(options.RepoPath, options.GitObjectDir)
			if err != nil {
				http.Error(w, "git object dirs is invalid", http.StatusUnauthorized)
				return
			}
			if relObjectDir != gitVars.GitObjectDirRel {
				_, err := w.Write([]byte(`{"status":false}`))
				require.NoError(t, err)
				return
			}
		}

		if len(options.GitAlternateObjectDirs) > 0 {
			if len(gitVars.GitAlternateObjectDirsRel) != len(options.GitAlternateObjectDirs) {
				http.Error(w, "git alternate object dirs is invalid", http.StatusUnauthorized)
				return
			}

			for i, gitAlterateObjectDir := range options.GitAlternateObjectDirs {
				relAltObjectDir, err := filepath.Rel(options.RepoPath, gitAlterateObjectDir)
				if err != nil {
					http.Error(w, "git alternate object dirs is invalid", http.StatusUnauthorized)
					return
				}

				if relAltObjectDir != gitVars.GitAlternateObjectDirsRel[i] {
					_, err := w.Write([]byte(`{"status":false}`))
					require.NoError(t, err)
					return
				}
			}
		}

		var authenticated bool
		if r.Form.Get("secret_token") == options.SecretToken {
			authenticated = true
		}

		secretHeader, err := base64.StdEncoding.DecodeString(r.Header.Get(secretHeaderName))
		if err == nil {
			if string(secretHeader) == options.SecretToken {
				authenticated = true
			}
		}

		if authenticated {
			_, err := w.Write([]byte(`{"status":true}`))
			require.NoError(t, err)
			return
		}
		w.WriteHeader(http.StatusUnauthorized)

		_, err = w.Write([]byte(`{"message":"401 Unauthorized\n"}`))
		require.NoError(t, err)
	}
}

func handlePreReceive(t testing.TB, options TestServerOptions) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			http.Error(w, "could not parse form", http.StatusBadRequest)
			return
		}

		params := make(map[string]string)

		switch r.Header.Get("Content-Type") {
		case "application/x-www-form-urlencoded":
			b, err := json.Marshal(r.Form)
			if err != nil {
				http.Error(w, "could not marshal form", http.StatusBadRequest)
				return
			}

			var reqForm struct {
				GLRepository []string `json:"gl_repository"`
			}

			if err = json.Unmarshal(b, &reqForm); err != nil {
				http.Error(w, "could not unmarshal form", http.StatusBadRequest)
				return
			}

			if len(reqForm.GLRepository) == 0 {
				http.Error(w, "gl_repository is missing", http.StatusBadRequest)
				return
			}

			params["gl_repository"] = reqForm.GLRepository[0]
		case "application/json":
			if err := json.NewDecoder(r.Body).Decode(&params); err != nil {
				http.Error(w, "error when unmarshalling json body", http.StatusBadRequest)
				return
			}
		}

		if r.Method != http.MethodPost {
			http.Error(w, "method not POST", http.StatusMethodNotAllowed)
			return
		}

		if params["gl_repository"] == "" {
			http.Error(w, "gl_repository is empty", http.StatusUnauthorized)
			return
		}

		if options.GLRepository != "" {
			if params["gl_repository"] != options.GLRepository {
				http.Error(w, "gl_repository is invalid", http.StatusUnauthorized)
				return
			}
		}

		var authenticated bool
		if r.Form.Get("secret_token") == options.SecretToken {
			authenticated = true
		}

		secretHeader, err := base64.StdEncoding.DecodeString(r.Header.Get(secretHeaderName))
		if err == nil {
			if string(secretHeader) == options.SecretToken {
				authenticated = true
			}
		}

		if !authenticated {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)

		_, err = w.Write([]byte(`{"reference_counter_increased": true}`))
		require.NoError(t, err)
	}
}

func handlePostReceive(options TestServerOptions) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			http.Error(w, "couldn't parse form", http.StatusBadRequest)
			return
		}

		if r.Method != http.MethodPost {
			http.Error(w, "method not POST", http.StatusMethodNotAllowed)
			return
		}

		var params postReceiveForm

		switch r.Header.Get("Content-Type") {
		case "application/x-www-form-urlencoded":
			params = parsePostReceiveForm(r.Form)
		case "application/json":
			if err := json.NewDecoder(r.Body).Decode(&params); err != nil {
				http.Error(w, "could not parse json body", http.StatusBadRequest)
				return
			}
		}

		if params.GLRepository == "" {
			http.Error(w, "gl_repository is empty", http.StatusUnauthorized)
			return
		}

		if options.GLRepository != "" {
			if params.GLRepository != options.GLRepository {
				http.Error(w, "gl_repository is invalid", http.StatusUnauthorized)
				return
			}
		}

		if params.SecretToken != options.SecretToken {
			decodedSecret, err := base64.StdEncoding.DecodeString(r.Header.Get("Gitlab-Shared-Secret"))
			if err != nil {
				http.Error(w, "secret_token is invalid", http.StatusUnauthorized)
				return
			}

			if string(decodedSecret) != options.SecretToken {
				http.Error(w, "secret_token is invalid", http.StatusUnauthorized)
				return
			}
		}

		if params.Identifier == "" {
			http.Error(w, "identifier is empty", http.StatusUnauthorized)
			return
		}

		if options.GLRepository != "" {
			if params.Identifier != options.GLID {
				http.Error(w, "identifier is invalid", http.StatusUnauthorized)
				return
			}
		}

		if params.Changes == "" {
			http.Error(w, "changes is empty", http.StatusUnauthorized)
			return
		}

		if options.Changes != "" {
			if params.Changes != options.Changes {
				http.Error(w, "changes is invalid", http.StatusUnauthorized)
				return
			}
		}

		if len(options.GitPushOptions) != len(params.GitPushOptions) {
			http.Error(w, "invalid push options", http.StatusUnauthorized)
			return
		}

		for i, opt := range options.GitPushOptions {
			if opt != params.GitPushOptions[i] {
				http.Error(w, "invalid push options", http.StatusUnauthorized)
				return
			}
		}

		response := postReceiveResponse{
			ReferenceCounterDecreased: options.PostReceiveCounterDecreased,
		}

		for _, basicMessage := range options.PostReceiveMessages {
			response.Messages = append(response.Messages, PostReceiveMessage{
				Message: basicMessage,
				Type:    "basic",
			})
		}

		for _, alertMessage := range options.PostReceiveAlerts {
			response.Messages = append(response.Messages, PostReceiveMessage{
				Message: alertMessage,
				Type:    "alert",
			})
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		if err := json.NewEncoder(w).Encode(&response); err != nil {
			panic(err)
		}
	}
}

func handleCheck(t testing.TB, options TestServerOptions) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		u, p, ok := r.BasicAuth()
		if !ok || u != options.User || p != options.Password {
			w.WriteHeader(http.StatusUnauthorized)
			require.NoError(t, json.NewEncoder(w).Encode(struct {
				Message string `json:"message"`
			}{Message: "authorization failed"}))
			return
		}

		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, `{"redis": true}`)
	}
}

func handleLfs(t testing.TB, options TestServerOptions) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			http.Error(w, "couldn't parse form", http.StatusBadRequest)
			return
		}

		if r.Method != http.MethodGet {
			http.Error(w, "method not GET", http.StatusMethodNotAllowed)
			return
		}

		if options.LfsOid != "" {
			if r.FormValue("oid") != options.LfsOid {
				http.Error(w, "oid parameter does not match", http.StatusBadRequest)
				return
			}
		}

		if options.GlRepository != "" {
			if r.FormValue("gl_repository") != options.GlRepository {
				http.Error(w, "gl_repository parameter does not match", http.StatusBadRequest)
				return
			}
		}

		w.Header().Set("Content-Type", "application/octet-stream")

		if options.LfsStatusCode != 0 {
			w.WriteHeader(options.LfsStatusCode)
		}

		if options.LfsBody != "" {
			_, err := w.Write([]byte(options.LfsBody))
			require.NoError(t, err)
		}
	}
}

func startSocketHTTPServer(t testing.TB, mux *http.ServeMux, tlsCfg *tls.Config) (string, func()) {
	tempDir := testhelper.TempDir(t)

	filename := filepath.Join(tempDir, "http-test-server")
	socketListener, err := net.Listen("unix", filename)
	require.NoError(t, err)

	server := http.Server{
		Handler:   mux,
		TLSConfig: tlsCfg,
	}

	go server.Serve(socketListener)

	url := "http+unix://" + filename
	cleanup := func() {
		require.NoError(t, server.Close())
	}

	return url, cleanup
}

// SetupAndStartGitlabServer creates a new GitlabTestServer, starts it and sets
// up the gitlab-shell secret.
func SetupAndStartGitlabServer(t testing.TB, shellDir string, c *TestServerOptions) (string, func()) {
	url, cleanup := NewTestServer(t, *c)

	WriteShellSecretFile(t, shellDir, c.SecretToken)

	return url, cleanup
}
