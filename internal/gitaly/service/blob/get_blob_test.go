package blob

import (
	"bytes"
	"fmt"
	"io"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/v14/streamio"
)

func TestSuccessfulGetBlob(t *testing.T) {
	_, repo, _, client := setup(t)

	maintenanceMdBlobData := testhelper.MustReadFile(t, "testdata/maintenance-md-blob.txt")
	testCases := []struct {
		desc     string
		oid      string
		contents []byte
		size     int
		limit    int
	}{
		{
			desc:     "unlimited fetch",
			oid:      "95d9f0a5e7bb054e9dd3975589b8dfc689e20e88",
			limit:    -1,
			contents: maintenanceMdBlobData,
			size:     len(maintenanceMdBlobData),
		},
		{
			desc:     "limit larger than blob size",
			oid:      "95d9f0a5e7bb054e9dd3975589b8dfc689e20e88",
			limit:    len(maintenanceMdBlobData) + 1,
			contents: maintenanceMdBlobData,
			size:     len(maintenanceMdBlobData),
		},
		{
			desc:  "limit zero",
			oid:   "95d9f0a5e7bb054e9dd3975589b8dfc689e20e88",
			limit: 0,
			size:  len(maintenanceMdBlobData),
		},
		{
			desc:     "limit greater than zero, less than blob size",
			oid:      "95d9f0a5e7bb054e9dd3975589b8dfc689e20e88",
			limit:    10,
			contents: maintenanceMdBlobData[:10],
			size:     len(maintenanceMdBlobData),
		},
		{
			desc:     "large blob",
			oid:      "08cf843fd8fe1c50757df0a13fcc44661996b4df",
			limit:    10,
			contents: []byte{0xff, 0xd8, 0xff, 0xe0, 0x00, 0x10, 0x4a, 0x46, 0x49, 0x46},
			size:     111803,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			request := &gitalypb.GetBlobRequest{
				Repository: repo,
				Oid:        tc.oid,
				Limit:      int64(tc.limit),
			}

			ctx, cancel := testhelper.Context()
			defer cancel()

			stream, err := client.GetBlob(ctx, request)
			require.NoError(t, err, "initiate RPC")

			reportedSize, reportedOid, data, err := getBlob(stream)
			require.NoError(t, err, "consume response")

			require.Equal(t, int64(tc.size), reportedSize, "real blob size")

			require.NotEmpty(t, reportedOid)
			require.True(t, bytes.Equal(tc.contents, data), "returned data exactly as expected")
		})
	}
}

func TestGetBlobNotFound(t *testing.T) {
	_, repo, _, client := setup(t)

	request := &gitalypb.GetBlobRequest{
		Repository: repo,
		Oid:        "doesnotexist",
	}

	ctx, cancel := testhelper.Context()
	defer cancel()

	stream, err := client.GetBlob(ctx, request)
	require.NoError(t, err)

	reportedSize, reportedOid, data, err := getBlob(stream)
	require.NoError(t, err)

	require.Zero(t, reportedSize)
	require.Empty(t, reportedOid)
	require.Zero(t, len(data))
}

func getBlob(stream gitalypb.BlobService_GetBlobClient) (int64, string, []byte, error) {
	firstResponse, err := stream.Recv()
	if err != nil {
		return 0, "", nil, err
	}

	data := &bytes.Buffer{}
	_, err = data.Write(firstResponse.GetData())
	if err != nil {
		return 0, "", nil, err
	}

	reader := streamio.NewReader(func() ([]byte, error) {
		response, err := stream.Recv()
		if response.GetSize() != 0 {
			return nil, fmt.Errorf("size may only be set in the first response message")
		}
		if len(response.GetOid()) != 0 {
			return nil, fmt.Errorf("oid may only be set in the first response message")
		}
		return response.GetData(), err
	})

	_, err = io.Copy(data, reader)
	return firstResponse.Size, firstResponse.Oid, data.Bytes(), err
}

func TestFailedGetBlobRequestDueToValidationError(t *testing.T) {
	_, repo, _, client := setup(t)

	oid := "d42783470dc29fde2cf459eb3199ee1d7e3f3a72"

	rpcRequests := []*gitalypb.GetBlobRequest{
		{Repository: &gitalypb.Repository{StorageName: "fake", RelativePath: "path"}, Oid: oid}, // Repository doesn't exist
		{Repository: nil, Oid: oid}, // Repository is nil
		{Repository: repo},          // Oid is empty
	}

	for i, rpcRequest := range rpcRequests {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			ctx, cancel := testhelper.Context()
			defer cancel()

			stream, err := client.GetBlob(ctx, rpcRequest)
			require.NoError(t, err, rpcRequest)
			_, err = stream.Recv()
			require.NotEqual(t, io.EOF, err, rpcRequest)
			require.Error(t, err, rpcRequest)
		})
	}
}
