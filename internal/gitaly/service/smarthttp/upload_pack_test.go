package smarthttp

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/git"
	"gitlab.com/gitlab-org/gitaly/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/internal/git/pktline"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/streamio"
	"google.golang.org/grpc/codes"
)

const (
	clientCapabilities = `multi_ack_detailed no-done side-band-64k thin-pack include-tag ofs-delta deepen-since deepen-not filter agent=git/2.18.0`
)

func TestSuccessfulUploadPackRequest(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()
	cfg, repo, _ := testcfg.BuildWithRepo(t)

	testhelper.ConfigureGitalyHooksBin(t, cfg)

	negotiationMetrics := prometheus.NewCounterVec(prometheus.CounterOpts{}, []string{"feature"})

	serverSocketPath := runSmartHTTPServer(t, cfg, WithPackfileNegotiationMetrics(negotiationMetrics))

	storagePath := cfg.Storages[0].Path
	remoteRepoRelativePath := "gitlab-test-remote"
	localRepoRelativePath := "gitlab-test-local"
	remoteRepoPath := filepath.Join(storagePath, remoteRepoRelativePath)
	localRepoPath := filepath.Join(storagePath, localRepoRelativePath)
	testRepoPath := filepath.Join(storagePath, repo.RelativePath)
	// Make a non-bare clone of the test repo to act as a remote one
	gittest.Exec(t, cfg, "clone", testRepoPath, remoteRepoPath)
	// Make a bare clone of the test repo to act as a local one and to leave the original repo intact for other tests
	gittest.Exec(t, cfg, "clone", "--bare", testRepoPath, localRepoPath)
	defer os.RemoveAll(localRepoPath)
	defer os.RemoveAll(remoteRepoPath)

	commitMsg := fmt.Sprintf("Testing UploadPack RPC around %d", time.Now().Unix())
	committerName := "Scrooge McDuck"
	committerEmail := "scrooge@mcduck.com"

	// The latest commit ID on the local repo
	oldHead := bytes.TrimSpace(gittest.Exec(t, cfg, "-C", remoteRepoPath, "rev-parse", "master"))

	gittest.Exec(t, cfg, "-C", remoteRepoPath,
		"-c", fmt.Sprintf("user.name=%s", committerName),
		"-c", fmt.Sprintf("user.email=%s", committerEmail),
		"commit", "--allow-empty", "-m", commitMsg)

	// The commit ID we want to pull from the remote repo
	newHead := bytes.TrimSpace(gittest.Exec(t, cfg, "-C", remoteRepoPath, "rev-parse", "master"))

	// UploadPack request is a "want" packet line followed by a packet flush, then many "have" packets followed by a packet flush.
	// This is explained a bit in https://git-scm.com/book/en/v2/Git-Internals-Transfer-Protocols#_downloading_data
	requestBuffer := &bytes.Buffer{}
	gittest.WritePktlineString(t, requestBuffer, fmt.Sprintf("want %s %s\n", newHead, clientCapabilities))
	gittest.WritePktlineFlush(t, requestBuffer)
	gittest.WritePktlineString(t, requestBuffer, fmt.Sprintf("have %s\n", oldHead))
	gittest.WritePktlineFlush(t, requestBuffer)

	req := &gitalypb.PostUploadPackRequest{
		Repository: &gitalypb.Repository{
			StorageName:  cfg.Storages[0].Name,
			RelativePath: filepath.Join(remoteRepoRelativePath, ".git"),
		},
	}
	responseBuffer, err := makePostUploadPackRequest(ctx, t, serverSocketPath, cfg.Auth.Token, req, requestBuffer)
	require.NoError(t, err)

	// There's no git command we can pass it this response and do the work for us (extracting pack file, ...),
	// so we have to do it ourselves.
	pack, version, entries := extractPackDataFromResponse(t, responseBuffer)
	require.NotNil(t, pack, "Expected to find a pack file in response, found none")

	gittest.ExecStream(t, cfg, bytes.NewReader(pack), "-C", localRepoPath, "unpack-objects", fmt.Sprintf("--pack_header=%d,%d", version, entries))

	// The fact that this command succeeds means that we got the commit correctly, no further checks should be needed.
	gittest.Exec(t, cfg, "-C", localRepoPath, "show", string(newHead))

	metric, err := negotiationMetrics.GetMetricWithLabelValues("have")
	require.NoError(t, err)
	require.Equal(t, 1.0, promtest.ToFloat64(metric))
}

func TestUploadPackRequestWithGitConfigOptions(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()
	cfg, repo, _ := testcfg.BuildWithRepo(t)

	testhelper.ConfigureGitalyHooksBin(t, cfg)

	serverSocketPath := runSmartHTTPServer(t, cfg)

	storagePath := cfg.Storages[0].Path
	ourRepoRelativePath := "gitlab-test-remote"
	ourRepoPath := filepath.Join(storagePath, ourRepoRelativePath)
	testRepoPath := filepath.Join(storagePath, repo.RelativePath)

	// Make a clone of the test repo to modify
	gittest.Exec(t, cfg, "clone", "--bare", testRepoPath, ourRepoPath)
	defer os.RemoveAll(ourRepoPath)

	// Remove remote-tracking branches that get in the way for this test
	gittest.Exec(t, cfg, "-C", ourRepoPath, "remote", "remove", "origin")

	// Turn the csv branch into a hidden ref
	want := string(bytes.TrimSpace(gittest.Exec(t, cfg, "-C", ourRepoPath, "rev-parse", "refs/heads/csv")))
	gittest.Exec(t, cfg, "-C", ourRepoPath, "update-ref", "refs/hidden/csv", want)
	gittest.Exec(t, cfg, "-C", ourRepoPath, "update-ref", "-d", "refs/heads/csv")

	have := string(bytes.TrimSpace(gittest.Exec(t, cfg, "-C", ourRepoPath, "rev-parse", want+"~1")))

	requestBody := &bytes.Buffer{}
	requestBodyCopy := &bytes.Buffer{}
	tee := io.MultiWriter(requestBody, requestBodyCopy)

	gittest.WritePktlineString(t, tee, fmt.Sprintf("want %s %s\n", want, clientCapabilities))
	gittest.WritePktlineFlush(t, tee)
	gittest.WritePktlineString(t, tee, fmt.Sprintf("have %s\n", have))
	gittest.WritePktlineFlush(t, tee)

	rpcRequest := &gitalypb.PostUploadPackRequest{
		Repository: &gitalypb.Repository{
			StorageName:  cfg.Storages[0].Name,
			RelativePath: ourRepoRelativePath,
		},
	}

	// The ref is successfully requested as it is not hidden
	response, err := makePostUploadPackRequest(ctx, t, serverSocketPath, cfg.Auth.Token, rpcRequest, requestBody)
	require.NoError(t, err)
	_, _, count := extractPackDataFromResponse(t, response)
	assert.Equal(t, 5, count, "pack should have 5 entries")

	// Now the ref is hidden, no packfile will be received. The git process
	// dies with an error message: `git upload-pack: not our ref ...` but the
	// client just sees a grpc unavailable error
	// we need to set uploadpack.allowAnySHA1InWant=false, because if it's true then we won't encounter an error from setting
	// uploadpack.hideRefs=refs/hidden. We are setting uploadpack.allowAnySHA1InWant=true in the RPC to enable partial clones
	rpcRequest.GitConfigOptions = []string{"uploadpack.hideRefs=refs/hidden", "uploadpack.allowAnySHA1InWant=false"}
	response, err = makePostUploadPackRequest(ctx, t, serverSocketPath, cfg.Auth.Token, rpcRequest, requestBodyCopy)
	testhelper.RequireGrpcError(t, err, codes.Unavailable)

	expected := fmt.Sprintf("0049ERR upload-pack: not our ref %v", want)
	assert.Equal(t, expected, response.String(), "Ref is hidden, expected error message did not appear")
}

func TestUploadPackRequestWithGitProtocol(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()
	cfg, repo, _ := testcfg.BuildWithRepo(t)

	readProto, cfg := gittest.EnableGitProtocolV2Support(t, cfg)

	serverSocketPath := runSmartHTTPServer(t, cfg)

	storagePath := cfg.Storages[0].Path
	testRepoPath := filepath.Join(storagePath, repo.RelativePath)
	relativePath, err := filepath.Rel(storagePath, testRepoPath)
	require.NoError(t, err)

	requestBody := &bytes.Buffer{}

	gittest.WritePktlineString(t, requestBody, "command=ls-refs\n")
	gittest.WritePktlineDelim(t, requestBody)
	gittest.WritePktlineString(t, requestBody, "peel\n")
	gittest.WritePktlineString(t, requestBody, "symrefs\n")
	gittest.WritePktlineFlush(t, requestBody)

	// Only a Git server with v2 will recognize this request.
	// Git v1 will throw a protocol error.
	rpcRequest := &gitalypb.PostUploadPackRequest{
		Repository: &gitalypb.Repository{
			StorageName:  cfg.Storages[0].Name,
			RelativePath: relativePath,
		},
		GitProtocol: git.ProtocolV2,
	}

	// The ref is successfully requested as it is not hidden
	_, err = makePostUploadPackRequest(ctx, t, serverSocketPath, cfg.Auth.Token, rpcRequest, requestBody)
	require.NoError(t, err)

	envData := readProto()
	require.Equal(t, fmt.Sprintf("GIT_PROTOCOL=%s\n", git.ProtocolV2), envData)
}

// This test is here because git-upload-pack returns a non-zero exit code
// on 'deepen' requests even though the request is being handled just
// fine from the client perspective.
func TestSuccessfulUploadPackDeepenRequest(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()
	cfg, repo, _ := testcfg.BuildWithRepo(t)

	serverSocketPath := runSmartHTTPServer(t, cfg)

	requestBody := &bytes.Buffer{}
	gittest.WritePktlineString(t, requestBody, fmt.Sprintf("want e63f41fe459e62e1228fcef60d7189127aeba95a %s\n", clientCapabilities))
	gittest.WritePktlineString(t, requestBody, "deepen 1")
	gittest.WritePktlineFlush(t, requestBody)

	rpcRequest := &gitalypb.PostUploadPackRequest{Repository: repo}
	response, err := makePostUploadPackRequest(ctx, t, serverSocketPath, cfg.Auth.Token, rpcRequest, requestBody)

	// This assertion is the main reason this test exists.
	assert.NoError(t, err)
	assert.Equal(t, `0034shallow e63f41fe459e62e1228fcef60d7189127aeba95a0000`, response.String())
}

func TestUploadPackWithPackObjectsHook(t *testing.T) {
	cfg, repo, repoPath := testcfg.BuildWithRepo(t)
	cfg.BinDir = testhelper.TempDir(t)

	outputPath := filepath.Join(cfg.BinDir, "output")
	hookScript := fmt.Sprintf("#!/bin/sh\necho 'I was invoked' >'%s'\nshift\nexec '%s' \"$@\"\n", outputPath, cfg.Git.BinPath)

	// We're using a custom pack-objects hook for git-upload-pack. In order
	// to assure that it's getting executed as expected, we're writing a
	// custom script which replaces the hook binary. It doesn't do anything
	// special, but writes a message into a status file and then errors
	// out. In the best case we'd have just printed the error to stderr and
	// check the return error message. But it's unfortunately not
	// transferred back.
	testhelper.WriteExecutable(t, filepath.Join(cfg.BinDir, "gitaly-hooks"), []byte(hookScript))

	serverSocketPath := runSmartHTTPServer(t, cfg)

	oldHead := bytes.TrimSpace(gittest.Exec(t, cfg, "-C", repoPath, "rev-parse", "master~"))
	newHead := bytes.TrimSpace(gittest.Exec(t, cfg, "-C", repoPath, "rev-parse", "master"))

	requestBuffer := &bytes.Buffer{}
	gittest.WritePktlineString(t, requestBuffer, fmt.Sprintf("want %s %s\n", newHead, clientCapabilities))
	gittest.WritePktlineFlush(t, requestBuffer)
	gittest.WritePktlineString(t, requestBuffer, fmt.Sprintf("have %s\n", oldHead))
	gittest.WritePktlineFlush(t, requestBuffer)

	ctx, cancel := testhelper.Context()
	defer cancel()

	_, err := makePostUploadPackRequest(ctx, t, serverSocketPath, cfg.Auth.Token, &gitalypb.PostUploadPackRequest{
		Repository: repo,
	}, requestBuffer)
	require.NoError(t, err)

	contents := testhelper.MustReadFile(t, outputPath)
	require.Equal(t, "I was invoked\n", string(contents))
}

func TestFailedUploadPackRequestDueToValidationError(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()
	cfg := testcfg.Build(t)

	serverSocketPath := runSmartHTTPServer(t, cfg)

	rpcRequests := []gitalypb.PostUploadPackRequest{
		{Repository: &gitalypb.Repository{StorageName: "fake", RelativePath: "path"}}, // Repository doesn't exist
		{Repository: nil}, // Repository is nil
		{Repository: &gitalypb.Repository{StorageName: cfg.Storages[0].Name, RelativePath: "path/to/repo"}, Data: []byte("Fail")}, // Data exists on first request
	}

	for _, rpcRequest := range rpcRequests {
		t.Run(fmt.Sprintf("%v", rpcRequest), func(t *testing.T) {
			_, err := makePostUploadPackRequest(ctx, t, serverSocketPath, cfg.Auth.Token, &rpcRequest, bytes.NewBuffer(nil))
			testhelper.RequireGrpcError(t, err, codes.InvalidArgument)
		})
	}
}

func makePostUploadPackRequest(ctx context.Context, t *testing.T, serverSocketPath, token string, in *gitalypb.PostUploadPackRequest, body io.Reader) (*bytes.Buffer, error) {
	client, conn := newSmartHTTPClient(t, serverSocketPath, token)
	defer conn.Close()

	stream, err := client.PostUploadPack(ctx)
	require.NoError(t, err)

	require.NoError(t, stream.Send(in))

	if body != nil {
		sw := streamio.NewWriter(func(p []byte) error {
			return stream.Send(&gitalypb.PostUploadPackRequest{Data: p})
		})

		_, err = io.Copy(sw, body)
		require.NoError(t, err)
		require.NoError(t, stream.CloseSend())
	}

	responseBuffer := &bytes.Buffer{}
	rr := streamio.NewReader(func() ([]byte, error) {
		resp, err := stream.Recv()
		return resp.GetData(), err
	})
	_, err = io.Copy(responseBuffer, rr)

	return responseBuffer, err
}

// The response contains bunch of things; metadata, progress messages, and a pack file. We're only
// interested in the pack file and its header values.
func extractPackDataFromResponse(t *testing.T, buf *bytes.Buffer) ([]byte, int, int) {
	var pack []byte

	// The response should have the following format.
	// PKT-LINE
	// PKT-LINE
	// ...
	// 0000
	scanner := pktline.NewScanner(buf)
	for scanner.Scan() {
		pkt := scanner.Bytes()
		if pktline.IsFlush(pkt) {
			break
		}

		// The first data byte of the packet is the band designator. We only care about data in band 1.
		if data := pktline.Data(pkt); len(data) > 0 && data[0] == 1 {
			pack = append(pack, data[1:]...)
		}
	}

	require.NoError(t, scanner.Err())
	require.NotEmpty(t, pack, "pack data should not be empty")

	// The packet is structured as follows:
	// 4 bytes for signature, here it's "PACK"
	// 4 bytes for header version
	// 4 bytes for header entries
	// The rest is the pack file
	require.Equal(t, "PACK", string(pack[:4]), "Invalid packet signature")
	version := int(binary.BigEndian.Uint32(pack[4:8]))
	entries := int(binary.BigEndian.Uint32(pack[8:12]))
	pack = pack[12:]

	return pack, version, entries
}

func TestUploadPackRequestForPartialCloneSuccess(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()
	cfg, _, repoPath := testcfg.BuildWithRepo(t)

	testhelper.ConfigureGitalyHooksBin(t, cfg)

	negotiationMetrics := prometheus.NewCounterVec(prometheus.CounterOpts{}, []string{"feature"})

	serverSocketPath := runSmartHTTPServer(t, cfg, WithPackfileNegotiationMetrics(negotiationMetrics))

	storagePath := cfg.Storages[0].Path
	remoteRepoRelativePath := "gitlab-test-remote"
	localRepoRelativePath := "gitlab-test-local"
	remoteRepoPath := filepath.Join(storagePath, remoteRepoRelativePath)
	localRepoPath := filepath.Join(storagePath, localRepoRelativePath)
	// Make a non-bare clone of the test repo to act as a remote one
	gittest.Exec(t, cfg, "clone", repoPath, remoteRepoPath)
	// Make a bare clone of the test repo to act as a local one and to leave the original repo intact for other tests
	gittest.Exec(t, cfg, "init", "--bare", localRepoPath)

	defer os.RemoveAll(localRepoPath)
	defer os.RemoveAll(remoteRepoPath)

	commitMsg := fmt.Sprintf("Testing UploadPack RPC around %d", time.Now().Unix())
	committerName := "Scrooge McDuck"
	committerEmail := "scrooge@mcduck.com"

	gittest.Exec(t, cfg, "-C", remoteRepoPath,
		"-c", fmt.Sprintf("user.name=%s", committerName),
		"-c", fmt.Sprintf("user.email=%s", committerEmail),
		"commit", "--allow-empty", "-m", commitMsg)

	// The commit ID we want to pull from the remote repo
	newHead := bytes.TrimSpace(gittest.Exec(t, cfg, "-C", remoteRepoPath, "rev-parse", "master"))
	// The commit ID we want to pull from the remote repo

	// UploadPack request is a "want" packet line followed by a packet flush, then many "have" packets followed by a packet flush.
	// This is explained a bit in https://git-scm.com/book/en/v2/Git-Internals-Transfer-Protocols#_downloading_data

	var requestBuffer bytes.Buffer
	gittest.WritePktlineString(t, &requestBuffer, fmt.Sprintf("want %s %s\n", newHead, clientCapabilities))
	gittest.WritePktlineString(t, &requestBuffer, fmt.Sprintf("filter %s\n", "blob:limit=200"))
	gittest.WritePktlineFlush(t, &requestBuffer)
	gittest.WritePktlineString(t, &requestBuffer, "done\n")
	gittest.WritePktlineFlush(t, &requestBuffer)

	req := &gitalypb.PostUploadPackRequest{
		Repository: &gitalypb.Repository{
			StorageName:  cfg.Storages[0].Name,
			RelativePath: filepath.Join(remoteRepoRelativePath, ".git"),
		},
	}

	responseBuffer, err := makePostUploadPackRequest(ctx, t, serverSocketPath, cfg.Auth.Token, req, &requestBuffer)
	require.NoError(t, err)

	pack, version, entries := extractPackDataFromResponse(t, responseBuffer)
	require.NotNil(t, pack, "Expected to find a pack file in response, found none")

	gittest.ExecStream(t, cfg, bytes.NewReader(pack), "-C", localRepoPath, "unpack-objects", fmt.Sprintf("--pack_header=%d,%d", version, entries))

	// a4a132b1b0d6720ca9254440a7ba8a6b9bbd69ec is README.md, which is a small file
	blobLessThanLimit := "a4a132b1b0d6720ca9254440a7ba8a6b9bbd69ec"

	// c1788657b95998a2f177a4f86d68a60f2a80117f is CONTRIBUTING.md, which is > 200 bytese
	blobGreaterThanLimit := "c1788657b95998a2f177a4f86d68a60f2a80117f"

	gittest.GitObjectMustExist(t, cfg.Git.BinPath, localRepoPath, blobLessThanLimit)
	gittest.GitObjectMustExist(t, cfg.Git.BinPath, remoteRepoPath, blobGreaterThanLimit)
	gittest.GitObjectMustNotExist(t, cfg.Git.BinPath, localRepoPath, blobGreaterThanLimit)

	newBranch := "new-branch"
	newHead = []byte(gittest.WriteCommit(t, cfg, remoteRepoPath, gittest.WithBranch(newBranch)))

	// after we delete the branch, we have a dangling commit
	gittest.Exec(t, cfg, "-C", remoteRepoPath, "branch", "-D", newBranch)

	requestBuffer.Reset()
	gittest.WritePktlineString(t, &requestBuffer, fmt.Sprintf("want %s %s\n", string(newHead), clientCapabilities))
	// add filtering
	gittest.WritePktlineFlush(t, &requestBuffer)
	gittest.WritePktlineFlush(t, &requestBuffer)

	_, err = makePostUploadPackRequest(ctx, t, serverSocketPath, cfg.Auth.Token, req, &requestBuffer)
	require.NoError(t, err)

	metric, err := negotiationMetrics.GetMetricWithLabelValues("filter")
	require.NoError(t, err)
	require.Equal(t, 1.0, promtest.ToFloat64(metric))
}
