package protoregistry_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/internal/praefect/protoregistry"
)

func TestProtoRegistryRequestFactory(t *testing.T) {
	r := protoregistry.New()
	require.NoError(t, r.RegisterFiles(protoregistry.GitalyProtoFileDescriptors...))

	mInfo, err := r.LookupMethod("/gitaly.RepositoryService/RepositoryExists")
	require.NoError(t, err)

	req, err := mInfo.NewRequest()
	require.NoError(t, err)
	require.NotNil(t, req)
}
