package smarthttp

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"strconv"
	"testing"
	"time"

	"gitlab.com/gitlab-org/gitaly/internal/testhelper"

	pb "gitlab.com/gitlab-org/gitaly-proto/go"
	"gitlab.com/gitlab-org/gitaly/streamio"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
)

func TestSuccessfulUploadPackRequest(t *testing.T) {
	server := runSmartHTTPServer(t)
	defer server.Stop()

	storagePath := testhelper.GitlabTestStoragePath()
	remoteRepoRelativePath := "gitlab-test-remote"
	localRepoRelativePath := "gitlab-test-local"
	testRepoPath := path.Join(storagePath, testRepo.RelativePath)
	remoteRepoPath := path.Join(storagePath, remoteRepoRelativePath)
	localRepoPath := path.Join(storagePath, localRepoRelativePath)
	// Make a non-bare clone of the test repo to act as a remote one
	testhelper.MustRunCommand(t, nil, "git", "clone", testRepoPath, remoteRepoPath)
	// Make a bare clone of the test repo to act as a local one and to leave the original repo intact for other tests
	testhelper.MustRunCommand(t, nil, "git", "clone", "--bare", testRepoPath, localRepoPath)
	defer os.RemoveAll(localRepoPath)
	defer os.RemoveAll(remoteRepoPath)

	commitMsg := fmt.Sprintf("Testing UploadPack RPC around %d", time.Now().Unix())
	committerName := "Scrooge McDuck"
	committerEmail := "scrooge@mcduck.com"
	clientCapabilities := "multi_ack_detailed no-done side-band-64k thin-pack include-tag ofs-delta deepen-since deepen-not agent=git/2.12.0"

	// The latest commit ID on the local repo
	oldHead := bytes.TrimSpace(testhelper.MustRunCommand(t, nil, "git", "-C", remoteRepoPath, "rev-parse", "master"))

	testhelper.MustRunCommand(t, nil, "git", "-C", remoteRepoPath,
		"-c", fmt.Sprintf("user.name=%s", committerName),
		"-c", fmt.Sprintf("user.email=%s", committerEmail),
		"commit", "--allow-empty", "-m", commitMsg)

	// The commit ID we want to pull from the remote repo
	newHead := bytes.TrimSpace(testhelper.MustRunCommand(t, nil, "git", "-C", remoteRepoPath, "rev-parse", "master"))

	// UploadPack request is a "want" packet line followed by a packet flush, then many "have" packets followed by a packet flush.
	// This is explained a bit in https://git-scm.com/book/en/v2/Git-Internals-Transfer-Protocols#_downloading_data
	wantPkt := fmt.Sprintf("want %s %s\n", newHead, clientCapabilities)
	havePkt := fmt.Sprintf("have %s\n", oldHead)

	// We don't check for errors because per bytes.Buffer docs, Buffer.Write will always return a nil error.
	requestBuffer := &bytes.Buffer{}
	fmt.Fprintf(requestBuffer, "%04x%s%s", len(wantPkt)+4, wantPkt, pktFlushStr)
	fmt.Fprintf(requestBuffer, "%04x%s%s", len(havePkt)+4, havePkt, pktFlushStr)

	client, conn := newSmartHTTPClient(t)
	defer conn.Close()
	repo := &pb.Repository{StorageName: "default", RelativePath: path.Join(remoteRepoRelativePath, ".git")}
	rpcRequest := &pb.PostUploadPackRequest{Repository: repo}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream, err := client.PostUploadPack(ctx)
	require.NoError(t, err)

	require.NoError(t, stream.Send(rpcRequest))

	sw := streamio.NewWriter(func(p []byte) error {
		return stream.Send(&pb.PostUploadPackRequest{Data: p})
	})
	_, err = io.Copy(sw, requestBuffer)
	require.NoError(t, err)
	stream.CloseSend()

	responseBuffer := &bytes.Buffer{}
	rr := streamio.NewReader(func() ([]byte, error) {
		resp, err := stream.Recv()
		return resp.GetData(), err
	})
	_, err = io.Copy(responseBuffer, rr)
	require.NoError(t, err)

	// There's no git command we can pass it this response and do the work for us (extracting pack file, ...),
	// so we have to do it ourselves.
	pack, version, entries := extractPackDataFromResponse(t, responseBuffer)
	require.NotNil(t, pack, "Expected to find a pack file in response, found none")

	testhelper.MustRunCommand(t, bytes.NewReader(pack), "git", "-C", localRepoPath, "unpack-objects", fmt.Sprintf("--pack_header=%d,%d", version, entries))

	// The fact that this command succeeds means that we got the commit correctly, no further checks should be needed.
	testhelper.MustRunCommand(t, nil, "git", "-C", localRepoPath, "show", string(newHead))
}

// This test is here because git-upload-pack returns a non-zero exit code
// on 'deepen' requests even though the request is being handled just
// fine from the client perspective.
func TestSuccessfulUploadPackDeepenRequest(t *testing.T) {
	server := runSmartHTTPServer(t)
	defer server.Stop()

	client, conn := newSmartHTTPClient(t)
	defer conn.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream, err := client.PostUploadPack(ctx)
	require.NoError(t, err)

	require.NoError(t, stream.Send(&pb.PostUploadPackRequest{Repository: testRepo}))

	requestBody := `00a4want e63f41fe459e62e1228fcef60d7189127aeba95a multi_ack_detailed no-done side-band-64k thin-pack include-tag ofs-delta deepen-since deepen-not agent=git/2.12.2
000cdeepen 10000`
	require.NoError(t, stream.Send(&pb.PostUploadPackRequest{Data: []byte(requestBody)}))
	stream.CloseSend()

	rr := streamio.NewReader(func() ([]byte, error) {
		resp, err := stream.Recv()
		return resp.GetData(), err
	})

	response, err := ioutil.ReadAll(rr)
	// This assertion is the main reason this test exists.
	assert.NoError(t, err)
	assert.Equal(t, `0034shallow e63f41fe459e62e1228fcef60d7189127aeba95a0000`, string(response))
}

func TestFailedUploadPackRequestDueToValidationError(t *testing.T) {
	server := runSmartHTTPServer(t)
	defer server.Stop()

	client, conn := newSmartHTTPClient(t)
	defer conn.Close()

	rpcRequests := []pb.PostUploadPackRequest{
		{Repository: &pb.Repository{StorageName: "fake", RelativePath: "path"}}, // Repository doesn't exist
		{Repository: nil}, // Repository is nil
		{Repository: &pb.Repository{StorageName: "default", RelativePath: "path/to/repo"}, Data: []byte("Fail")}, // Data exists on first request
	}

	for _, rpcRequest := range rpcRequests {
		t.Run(fmt.Sprintf("%v", rpcRequest), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			stream, err := client.PostUploadPack(ctx)
			require.NoError(t, err)

			require.NoError(t, stream.Send(&rpcRequest))
			stream.CloseSend()

			err = drainPostUploadPackResponse(stream)
			testhelper.AssertGrpcError(t, err, codes.InvalidArgument, "")
		})
	}
}

func drainPostUploadPackResponse(stream pb.SmartHTTP_PostUploadPackClient) error {
	var err error
	for err == nil {
		_, err = stream.Recv()
	}
	return err
}

// The response contains bunch of things; metadata, progress messages, and a pack file. We're only
// interested in the pack file and its header values.
func extractPackDataFromResponse(t *testing.T, buf *bytes.Buffer) ([]byte, int, int) {
	var pack []byte

	// The response should have the following format, where <length> is always four hexadecimal digits.
	// <length><data>
	// <length><data>
	// ...
	// 0000
	for {
		pktLenStr := buf.Next(4)
		if len(pktLenStr) != 4 {
			return nil, 0, 0
		}
		if string(pktLenStr) == pktFlushStr {
			break
		}

		pktLen, err := strconv.ParseUint(string(pktLenStr), 16, 16)
		require.NoError(t, err)

		restPktLen := int(pktLen) - 4
		pkt := buf.Next(restPktLen)
		require.Equal(t, restPktLen, len(pkt), "Incomplete packet read")

		// The first byte of the packet is the band designator. We only care about data in band 1.
		if pkt[0] == 1 {
			pack = append(pack, pkt[1:]...)
		}
	}

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
