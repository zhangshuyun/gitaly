package blob

import (
	"bytes"
	"io"
	"testing"

	pb "gitlab.com/gitlab-org/gitaly-proto/go"
	"gitlab.com/gitlab-org/gitaly/internal/testhelper"

	"github.com/stretchr/testify/require"
)

func TestSuccessfulGetBlobs(t *testing.T) {
	server := runBlobServer(t)
	defer server.Stop()

	client, conn := newBlobClient(t, serverSocketPath)
	defer conn.Close()
	maintenanceMdBlobData := testhelper.MustReadFile(t, "testdata/maintenance-md-blob.txt")
	testCases := []struct {
		desc  string
		oids  []string
		blobs []blob
		limit int
	}{
		{
			desc:  "unlimited fetch",
			oids:  []string{"95d9f0a5e7bb054e9dd3975589b8dfc689e20e88"},
			limit: -1,
			blobs: []blob{{data: maintenanceMdBlobData, size: int64(len(maintenanceMdBlobData))}},
		},
		{
			desc:  "limit larger than blob size",
			oids:  []string{"95d9f0a5e7bb054e9dd3975589b8dfc689e20e88"},
			limit: len(maintenanceMdBlobData) + 1,
			blobs: []blob{{data: maintenanceMdBlobData, size: int64(len(maintenanceMdBlobData))}},
		},
		{
			desc:  "limit zero",
			oids:  []string{"95d9f0a5e7bb054e9dd3975589b8dfc689e20e88"},
			limit: 0,
			blobs: []blob{{size: int64(len(maintenanceMdBlobData))}},
		},
		{
			desc:  "limit greater than zero, less than blob size",
			oids:  []string{"95d9f0a5e7bb054e9dd3975589b8dfc689e20e88"},
			limit: 10,
			blobs: []blob{{data: maintenanceMdBlobData[:10], size: int64(len(maintenanceMdBlobData))}},
		},
		{
			desc:  "large blob",
			oids:  []string{"08cf843fd8fe1c50757df0a13fcc44661996b4df"},
			limit: 10,
			blobs: []blob{{data: []byte{0xff, 0xd8, 0xff, 0xe0, 0x00, 0x10, 0x4a, 0x46, 0x49, 0x46}, size: 111803}},
		},
		{
			desc:  "two identical blobs, no limit",
			oids:  []string{"95d9f0a5e7bb054e9dd3975589b8dfc689e20e88", "95d9f0a5e7bb054e9dd3975589b8dfc689e20e88"},
			limit: -1,
			blobs: []blob{
				{data: maintenanceMdBlobData, size: int64(len(maintenanceMdBlobData))},
				{data: maintenanceMdBlobData, size: int64(len(maintenanceMdBlobData))},
			},
		},
		{
			desc:  "two identical blobs, with limit",
			oids:  []string{"95d9f0a5e7bb054e9dd3975589b8dfc689e20e88", "95d9f0a5e7bb054e9dd3975589b8dfc689e20e88"},
			limit: 20,
			blobs: []blob{
				{data: maintenanceMdBlobData[:20], size: int64(len(maintenanceMdBlobData))},
				{data: maintenanceMdBlobData[:20], size: int64(len(maintenanceMdBlobData))},
			},
		},
		{
			desc:  "two blobs, with limit",
			oids:  []string{"95d9f0a5e7bb054e9dd3975589b8dfc689e20e88", "08cf843fd8fe1c50757df0a13fcc44661996b4df"},
			limit: 10,
			blobs: []blob{
				{data: maintenanceMdBlobData[:10], size: int64(len(maintenanceMdBlobData))},
				{data: []byte{0xff, 0xd8, 0xff, 0xe0, 0x00, 0x10, 0x4a, 0x46, 0x49, 0x46}, size: 111803},
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			request := &pb.GetBlobsRequest{
				Repository: testRepo,
				Oids:       tc.oids,
				Limit:      int64(tc.limit),
			}

			ctx, cancel := testhelper.Context()
			defer cancel()

			stream, err := client.GetBlobs(ctx, request)
			require.NoError(t, err, "initiate RPC")

			blobs, err := getAllBlobs(stream)
			require.NoError(t, err, "consume response")

			require.Equal(t, len(tc.oids), len(blobs))

			for i, blob := range tc.blobs {
				t.Logf("testing oid[%d] %q", i, tc.oids[i])
				require.Equal(t, int64(blobs[i].size), blob.size, "real blob size")

				require.NotEmpty(t, blobs[i].oid)
				require.Equal(t, blobs[i].oid, tc.oids[i])
				require.Equal(t, len(blob.data), len(blobs[i].data), "returned data should have the same size")
				require.True(t, bytes.Equal(blob.data, blobs[i].data), "returned data exactly as expected for oid %q", tc.oids[i])
			}
		})
	}
}

func TestGetBlobsNotFound(t *testing.T) {
	server := runBlobServer(t)
	defer server.Stop()

	client, conn := newBlobClient(t, serverSocketPath)
	defer conn.Close()

	request := &pb.GetBlobsRequest{
		Repository: testRepo,
		Oids:       []string{"doesnotexist", "95d9f0a5e7bb054e9dd3975589b8dfc689e20e88"}, // Second exist
	}

	ctx, cancel := testhelper.Context()
	defer cancel()

	stream, err := client.GetBlobs(ctx, request)
	require.NoError(t, err)

	blobs, err := getAllBlobs(stream)
	require.NoError(t, err)

	require.Nil(t, blobs)
}

func TestFailedGetBlobsRequestDueToValidationError(t *testing.T) {
	server := runBlobServer(t)
	defer server.Stop()

	client, conn := newBlobClient(t, serverSocketPath)
	defer conn.Close()
	oid := "d42783470dc29fde2cf459eb3199ee1d7e3f3a72"

	tests := []struct {
		desc string
		req  pb.GetBlobsRequest
	}{
		{
			desc: "repo does not exist",
			req:  pb.GetBlobsRequest{Repository: &pb.Repository{StorageName: "fake", RelativePath: "path"}, Oids: []string{oid}},
		},
		{
			desc: "repo is nil",
			req:  pb.GetBlobsRequest{Repository: nil, Oids: []string{oid}},
		},
		{
			desc: "oid list is empty",
			req:  pb.GetBlobsRequest{Repository: testRepo},
		},
		{
			desc: "one oid is empty string",
			req:  pb.GetBlobsRequest{Repository: testRepo, Oids: []string{"foo", "", "bar"}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.desc, func(t *testing.T) {
			ctx, cancel := testhelper.Context()
			defer cancel()

			stream, err := client.GetBlobs(ctx, &tc.req)
			require.NoError(t, err)
			_, err = stream.Recv()
			require.NotEqual(t, io.EOF, err)
			require.Error(t, err)
		})
	}
}

type blob struct {
	oid  string
	size int64
	data []byte
}

func getAllBlobs(stream pb.BlobService_GetBlobsClient) ([]*blob, error) {
	var (
		blobs   []*blob
		curBlob = &blob{}
		err     error
	)

	resp, err := stream.Recv()
	for err == nil {
		if resp.GetOid() != "" {
			if curBlob.oid != "" {
				blobs = append(blobs, curBlob)
			}
			curBlob = &blob{oid: resp.GetOid(), size: resp.GetSize()}
		}
		curBlob.data = append(curBlob.data, resp.GetData()...)
		resp, err = stream.Recv()
	}

	if curBlob.oid != "" {
		blobs = append(blobs, curBlob)
	}

	if err != io.EOF {
		return nil, err
	}
	return blobs, nil
}
