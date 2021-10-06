package smarthttp

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"path/filepath"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/gittest"
	"gitlab.com/gitlab-org/gitaly/v14/internal/git/pktline"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper/testcfg"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/v14/streamio"
	"google.golang.org/grpc/codes"
)

const (
	clientCapabilities = `multi_ack_detailed no-done side-band-64k thin-pack include-tag ofs-delta deepen-since deepen-not filter agent=git/2.18.0`
)

func runTestWithAndWithoutConfigOptions(t *testing.T, tf func(t *testing.T, ctx context.Context, opts ...testcfg.Option), opts ...testcfg.Option) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	t.Run("no config options", func(t *testing.T) { tf(t, ctx) })

	if len(opts) > 0 {
		t.Run("with config options", func(t *testing.T) {
			tf(t, ctx, opts...)
		})
	}
}

func TestServer_PostUpload(t *testing.T) {
	runTestWithAndWithoutConfigOptions(t, testServerPostUpload, testcfg.WithPackObjectsCacheEnabled())
}

func testServerPostUpload(t *testing.T, ctx context.Context, opts ...testcfg.Option) {
	cfg, repo, repoPath := testcfg.BuildWithRepo(t, opts...)
	_, localRepoPath := gittest.CloneRepo(t, cfg, cfg.Storages[0])

	testhelper.BuildGitalyHooks(t, cfg)
	negotiationMetrics := prometheus.NewCounterVec(prometheus.CounterOpts{}, []string{"feature"})
	serverSocketPath := runSmartHTTPServer(t, cfg, WithPackfileNegotiationMetrics(negotiationMetrics))

	oldCommit, err := git.NewObjectIDFromHex("1e292f8fedd741b75372e19097c76d327140c312") // refs/heads/master
	require.NoError(t, err)
	newCommit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("master"), gittest.WithParents(oldCommit))

	// UploadPack request is a "want" packet line followed by a packet flush, then many "have" packets followed by a packet flush.
	// This is explained a bit in https://git-scm.com/book/en/v2/Git-Internals-Transfer-Protocols#_downloading_data
	requestBuffer := &bytes.Buffer{}
	gittest.WritePktlineString(t, requestBuffer, fmt.Sprintf("want %s %s\n", newCommit, clientCapabilities))
	gittest.WritePktlineFlush(t, requestBuffer)
	gittest.WritePktlineString(t, requestBuffer, fmt.Sprintf("have %s\n", oldCommit))
	gittest.WritePktlineFlush(t, requestBuffer)

	req := &gitalypb.PostUploadPackRequest{Repository: repo}
	responseBuffer, err := makePostUploadPackRequest(ctx, t, serverSocketPath, cfg.Auth.Token, req, requestBuffer)
	require.NoError(t, err)

	pack, version, entries := extractPackDataFromResponse(t, responseBuffer)
	require.NotEmpty(t, pack, "Expected to find a pack file in response, found none")

	gittest.ExecStream(t, cfg, bytes.NewReader(pack), "-C", localRepoPath, "unpack-objects", fmt.Sprintf("--pack_header=%d,%d", version, entries))

	gittest.GitObjectMustExist(t, cfg.Git.BinPath, localRepoPath, newCommit.String())

	metric, err := negotiationMetrics.GetMetricWithLabelValues("have")
	require.NoError(t, err)
	require.Equal(t, 1.0, promtest.ToFloat64(metric))
}

func TestServer_PostUploadPack_gitConfigOptions(t *testing.T) {
	runTestWithAndWithoutConfigOptions(t, testServerPostUploadPackGitConfigOptions, testcfg.WithPackObjectsCacheEnabled())
}

func testServerPostUploadPackGitConfigOptions(t *testing.T, ctx context.Context, opts ...testcfg.Option) {
	cfg, repo, repoPath := testcfg.BuildWithRepo(t, opts...)
	testhelper.BuildGitalyHooks(t, cfg)

	serverSocketPath := runSmartHTTPServer(t, cfg)

	want := "3dd08961455abf80ef9115f4afdc1c6f968b503c" // refs/heads/csv
	gittest.Exec(t, cfg, "-C", repoPath, "update-ref", "refs/hidden/csv", want)
	gittest.Exec(t, cfg, "-C", repoPath, "update-ref", "-d", "refs/heads/csv")

	have := "6ff234d2889b27d91c3442924ef6a100b1fc6f2b" // refs/heads/csv~1

	requestBody := &bytes.Buffer{}

	gittest.WritePktlineString(t, requestBody, fmt.Sprintf("want %s %s\n", want, clientCapabilities))
	gittest.WritePktlineFlush(t, requestBody)
	gittest.WritePktlineString(t, requestBody, fmt.Sprintf("have %s\n", have))
	gittest.WritePktlineFlush(t, requestBody)

	t.Run("sanity check: ref exists and can be fetched", func(t *testing.T) {
		rpcRequest := &gitalypb.PostUploadPackRequest{Repository: repo}

		response, err := makePostUploadPackRequest(ctx, t, serverSocketPath, cfg.Auth.Token, rpcRequest, bytes.NewReader(requestBody.Bytes()))
		require.NoError(t, err)
		_, _, count := extractPackDataFromResponse(t, response)
		require.Equal(t, 5, count, "pack should have 5 entries")
	})

	t.Run("failing request because of hidden ref config", func(t *testing.T) {
		rpcRequest := &gitalypb.PostUploadPackRequest{
			Repository: repo,
			GitConfigOptions: []string{
				"uploadpack.hideRefs=refs/hidden",
				"uploadpack.allowAnySHA1InWant=false",
			},
		}
		response, err := makePostUploadPackRequest(ctx, t, serverSocketPath, cfg.Auth.Token, rpcRequest, bytes.NewReader(requestBody.Bytes()))
		testhelper.RequireGrpcError(t, err, codes.Unavailable)

		// The failure message proves that upload-pack failed because of
		// GitConfigOptions, and that proves that passing GitConfigOptions works.
		expected := fmt.Sprintf("0049ERR upload-pack: not our ref %v", want)
		require.Equal(t, expected, response.String(), "Ref is hidden, expected error message did not appear")
	})
}

func TestServer_PostUploadPack_gitProtocol(t *testing.T) {
	runTestWithAndWithoutConfigOptions(t, testServerPostUploadPackGitProtocol, testcfg.WithPackObjectsCacheEnabled())
}

func testServerPostUploadPackGitProtocol(t *testing.T, ctx context.Context, opts ...testcfg.Option) {
	cfg, repo, _ := testcfg.BuildWithRepo(t, opts...)
	readProto, cfg := gittest.EnableGitProtocolV2Support(t, cfg)
	serverSocketPath := runSmartHTTPServer(t, cfg)

	// command=ls-refs does not exist in protocol v0, so if this succeeds, we're talking v2
	requestBody := &bytes.Buffer{}
	gittest.WritePktlineString(t, requestBody, "command=ls-refs\n")
	gittest.WritePktlineDelim(t, requestBody)
	gittest.WritePktlineString(t, requestBody, "peel\n")
	gittest.WritePktlineString(t, requestBody, "symrefs\n")
	gittest.WritePktlineFlush(t, requestBody)

	rpcRequest := &gitalypb.PostUploadPackRequest{
		Repository:  repo,
		GitProtocol: git.ProtocolV2,
	}

	_, err := makePostUploadPackRequest(ctx, t, serverSocketPath, cfg.Auth.Token, rpcRequest, requestBody)
	require.NoError(t, err)

	envData := readProto()
	require.Equal(t, fmt.Sprintf("GIT_PROTOCOL=%s\n", git.ProtocolV2), envData)
}

// This test is here because git-upload-pack returns a non-zero exit code
// on 'deepen' requests even though the request is being handled just
// fine from the client perspective.
func TestServer_PostUploadPack_suppressDeepenExitError(t *testing.T) {
	runTestWithAndWithoutConfigOptions(t, testServerPostUploadPackSuppressDeepenExitError, testcfg.WithPackObjectsCacheEnabled())
}

func testServerPostUploadPackSuppressDeepenExitError(t *testing.T, ctx context.Context, opts ...testcfg.Option) {
	cfg, repo, _ := testcfg.BuildWithRepo(t, opts...)
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

func TestServer_PostUploadPack_usesPackObjectsHook(t *testing.T) {
	cfg, repo, repoPath := testcfg.BuildWithRepo(t, testcfg.WithPackObjectsCacheEnabled())
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

func TestServer_PostUploadPack_validation(t *testing.T) {
	runTestWithAndWithoutConfigOptions(t, testServerPostUploadPackValidation, testcfg.WithPackObjectsCacheEnabled())
}

func testServerPostUploadPackValidation(t *testing.T, ctx context.Context, opts ...testcfg.Option) {
	cfg := testcfg.Build(t, opts...)
	serverSocketPath := runSmartHTTPServer(t, cfg)

	rpcRequests := []*gitalypb.PostUploadPackRequest{
		{Repository: &gitalypb.Repository{StorageName: "fake", RelativePath: "path"}}, // Repository doesn't exist
		{Repository: nil}, // Repository is nil
		{Repository: &gitalypb.Repository{StorageName: cfg.Storages[0].Name, RelativePath: "path/to/repo"}, Data: []byte("Fail")}, // Data exists on first request
	}

	for _, rpcRequest := range rpcRequests {
		t.Run(fmt.Sprintf("%v", rpcRequest), func(t *testing.T) {
			_, err := makePostUploadPackRequest(ctx, t, serverSocketPath, cfg.Auth.Token, rpcRequest, bytes.NewBuffer(nil))
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

func TestServer_PostUploadPack_partialClone(t *testing.T) {
	runTestWithAndWithoutConfigOptions(t, testServerPostUploadPackPartialClone, testcfg.WithPackObjectsCacheEnabled())
}

func testServerPostUploadPackPartialClone(t *testing.T, ctx context.Context, opts ...testcfg.Option) {
	cfg, repo, repoPath := testcfg.BuildWithRepo(t, opts...)
	_, localRepoPath := gittest.InitRepo(t, cfg, cfg.Storages[0])

	testhelper.BuildGitalyHooks(t, cfg)
	negotiationMetrics := prometheus.NewCounterVec(prometheus.CounterOpts{}, []string{"feature"})
	serverSocketPath := runSmartHTTPServer(t, cfg, WithPackfileNegotiationMetrics(negotiationMetrics))

	oldCommit, err := git.NewObjectIDFromHex("1e292f8fedd741b75372e19097c76d327140c312") // refs/heads/master
	require.NoError(t, err)
	newCommit := gittest.WriteCommit(t, cfg, repoPath, gittest.WithBranch("master"), gittest.WithParents(oldCommit))

	var requestBuffer bytes.Buffer
	gittest.WritePktlineString(t, &requestBuffer, fmt.Sprintf("want %s %s\n", newCommit, clientCapabilities))
	gittest.WritePktlineString(t, &requestBuffer, fmt.Sprintf("filter %s\n", "blob:limit=200"))
	gittest.WritePktlineFlush(t, &requestBuffer)
	gittest.WritePktlineString(t, &requestBuffer, "done\n")
	gittest.WritePktlineFlush(t, &requestBuffer)

	req := &gitalypb.PostUploadPackRequest{Repository: repo}
	responseBuffer, err := makePostUploadPackRequest(ctx, t, serverSocketPath, cfg.Auth.Token, req, &requestBuffer)
	require.NoError(t, err)

	pack, version, entries := extractPackDataFromResponse(t, responseBuffer)
	require.NotEmpty(t, pack, "Expected to find a pack file in response, found none")

	gittest.ExecStream(t, cfg, bytes.NewReader(pack), "-C", localRepoPath, "unpack-objects", fmt.Sprintf("--pack_header=%d,%d", version, entries))

	// a4a132b1b0d6720ca9254440a7ba8a6b9bbd69ec is README.md, which is a small file
	blobLessThanLimit := "a4a132b1b0d6720ca9254440a7ba8a6b9bbd69ec"

	// c1788657b95998a2f177a4f86d68a60f2a80117f is CONTRIBUTING.md, which is > 200 bytese
	blobGreaterThanLimit := "c1788657b95998a2f177a4f86d68a60f2a80117f"

	gittest.GitObjectMustExist(t, cfg.Git.BinPath, localRepoPath, blobLessThanLimit)
	gittest.GitObjectMustExist(t, cfg.Git.BinPath, repoPath, blobGreaterThanLimit)
	gittest.GitObjectMustNotExist(t, cfg.Git.BinPath, localRepoPath, blobGreaterThanLimit)

	metric, err := negotiationMetrics.GetMetricWithLabelValues("filter")
	require.NoError(t, err)
	require.Equal(t, 1.0, promtest.ToFloat64(metric))
}

func TestServer_PostUploadPack_allowAnySHA1InWant(t *testing.T) {
	ctx, cancel := testhelper.Context()
	defer cancel()

	cfg, repo, repoPath := testcfg.BuildWithRepo(t)
	_, localRepoPath := gittest.InitRepo(t, cfg, cfg.Storages[0])

	testhelper.BuildGitalyHooks(t, cfg)
	serverSocketPath := runSmartHTTPServer(t, cfg)
	newCommit := gittest.WriteCommit(t, cfg, repoPath)

	var requestBuffer bytes.Buffer
	gittest.WritePktlineString(t, &requestBuffer, fmt.Sprintf("want %s %s\n", newCommit, clientCapabilities))
	gittest.WritePktlineFlush(t, &requestBuffer)
	gittest.WritePktlineString(t, &requestBuffer, "done\n")
	gittest.WritePktlineFlush(t, &requestBuffer)

	req := &gitalypb.PostUploadPackRequest{Repository: repo}
	responseBuffer, err := makePostUploadPackRequest(ctx, t, serverSocketPath, cfg.Auth.Token, req, &requestBuffer)
	require.NoError(t, err)

	pack, version, entries := extractPackDataFromResponse(t, responseBuffer)
	require.NotEmpty(t, pack, "Expected to find a pack file in response, found none")

	gittest.ExecStream(t, cfg, bytes.NewReader(pack), "-C", localRepoPath, "unpack-objects", fmt.Sprintf("--pack_header=%d,%d", version, entries))

	gittest.GitObjectMustExist(t, cfg.Git.BinPath, localRepoPath, newCommit.String())
}
