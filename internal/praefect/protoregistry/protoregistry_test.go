package protoregistry_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v14/internal/praefect/protoregistry"
	"gitlab.com/gitlab-org/gitaly/v14/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

func TestNewProtoRegistry(t *testing.T) {
	expectedResults := map[string]map[string]protoregistry.OpType{
		"BlobService": {
			"GetBlob":        protoregistry.OpAccessor,
			"GetBlobs":       protoregistry.OpAccessor,
			"GetLFSPointers": protoregistry.OpAccessor,
		},
		"CleanupService": {
			"ApplyBfgObjectMapStream": protoregistry.OpMutator,
		},
		"CommitService": {
			"CommitIsAncestor":         protoregistry.OpAccessor,
			"CommitLanguages":          protoregistry.OpAccessor,
			"CommitStats":              protoregistry.OpAccessor,
			"CommitsByMessage":         protoregistry.OpAccessor,
			"CountCommits":             protoregistry.OpAccessor,
			"CountDivergingCommits":    protoregistry.OpAccessor,
			"FilterShasWithSignatures": protoregistry.OpAccessor,
			"FindAllCommits":           protoregistry.OpAccessor,
			"FindCommit":               protoregistry.OpAccessor,
			"FindCommits":              protoregistry.OpAccessor,
			"GetTreeEntries":           protoregistry.OpAccessor,
			"LastCommitForPath":        protoregistry.OpAccessor,
			"ListCommitsByOid":         protoregistry.OpAccessor,
			"ListFiles":                protoregistry.OpAccessor,
			"ListLastCommitsForTree":   protoregistry.OpAccessor,
			"RawBlame":                 protoregistry.OpAccessor,
			"TreeEntry":                protoregistry.OpAccessor,
		},
		"ConflictsService": {
			"ListConflictFiles": protoregistry.OpAccessor,
			"ResolveConflicts":  protoregistry.OpMutator,
		},
		"DiffService": {
			"CommitDelta": protoregistry.OpAccessor,
			"CommitDiff":  protoregistry.OpAccessor,
			"DiffStats":   protoregistry.OpAccessor,
			"RawDiff":     protoregistry.OpAccessor,
			"RawPatch":    protoregistry.OpAccessor,
		},
		"NamespaceService": {
			"AddNamespace":    protoregistry.OpMutator,
			"NamespaceExists": protoregistry.OpAccessor,
			"RemoveNamespace": protoregistry.OpMutator,
			"RenameNamespace": protoregistry.OpMutator,
		},
		"ObjectPoolService": {
			"CreateObjectPool":           protoregistry.OpMutator,
			"DeleteObjectPool":           protoregistry.OpMutator,
			"DisconnectGitAlternates":    protoregistry.OpMutator,
			"LinkRepositoryToObjectPool": protoregistry.OpMutator,
			"ReduplicateRepository":      protoregistry.OpMutator,
		},
		"OperationService": {
			"UserApplyPatch":      protoregistry.OpMutator,
			"UserCherryPick":      protoregistry.OpMutator,
			"UserCommitFiles":     protoregistry.OpMutator,
			"UserCreateBranch":    protoregistry.OpMutator,
			"UserCreateTag":       protoregistry.OpMutator,
			"UserDeleteBranch":    protoregistry.OpMutator,
			"UserDeleteTag":       protoregistry.OpMutator,
			"UserFFBranch":        protoregistry.OpMutator,
			"UserMergeBranch":     protoregistry.OpMutator,
			"UserMergeToRef":      protoregistry.OpMutator,
			"UserRevert":          protoregistry.OpMutator,
			"UserSquash":          protoregistry.OpMutator,
			"UserUpdateBranch":    protoregistry.OpMutator,
			"UserUpdateSubmodule": protoregistry.OpMutator,
		},
		"RefService": {
			"DeleteRefs":                      protoregistry.OpMutator,
			"FindAllBranchNames":              protoregistry.OpAccessor,
			"FindAllBranches":                 protoregistry.OpAccessor,
			"FindAllRemoteBranches":           protoregistry.OpAccessor,
			"FindAllTagNames":                 protoregistry.OpAccessor,
			"FindAllTags":                     protoregistry.OpAccessor,
			"FindBranch":                      protoregistry.OpAccessor,
			"FindDefaultBranchName":           protoregistry.OpAccessor,
			"FindLocalBranches":               protoregistry.OpAccessor,
			"GetTagMessages":                  protoregistry.OpAccessor,
			"ListBranchNamesContainingCommit": protoregistry.OpAccessor,
			"ListTagNamesContainingCommit":    protoregistry.OpAccessor,
			"PackRefs":                        protoregistry.OpMutator,
			"RefExists":                       protoregistry.OpAccessor,
		},
		"RemoteService": {
			"FindRemoteRepository": protoregistry.OpAccessor,
			"FindRemoteRootRef":    protoregistry.OpAccessor,
			"UpdateRemoteMirror":   protoregistry.OpAccessor,
		},
		"RepositoryService": {
			"ApplyGitattributes":           protoregistry.OpMutator,
			"BackupCustomHooks":            protoregistry.OpAccessor,
			"CalculateChecksum":            protoregistry.OpAccessor,
			"Cleanup":                      protoregistry.OpMutator,
			"CreateBundle":                 protoregistry.OpAccessor,
			"CreateFork":                   protoregistry.OpMutator,
			"CreateRepository":             protoregistry.OpMutator,
			"CreateRepositoryFromBundle":   protoregistry.OpMutator,
			"CreateRepositoryFromSnapshot": protoregistry.OpMutator,
			"CreateRepositoryFromURL":      protoregistry.OpMutator,
			"FetchBundle":                  protoregistry.OpMutator,
			"FetchRemote":                  protoregistry.OpMutator,
			"FetchSourceBranch":            protoregistry.OpMutator,
			"FindLicense":                  protoregistry.OpAccessor,
			"FindMergeBase":                protoregistry.OpAccessor,
			"Fsck":                         protoregistry.OpAccessor,
			"GarbageCollect":               protoregistry.OpMutator,
			"GetArchive":                   protoregistry.OpAccessor,
			"GetInfoAttributes":            protoregistry.OpAccessor,
			"GetRawChanges":                protoregistry.OpAccessor,
			"GetSnapshot":                  protoregistry.OpAccessor,
			"HasLocalBranches":             protoregistry.OpAccessor,
			"OptimizeRepository":           protoregistry.OpMutator,
			"RepackFull":                   protoregistry.OpMutator,
			"RepackIncremental":            protoregistry.OpMutator,
			"RepositoryExists":             protoregistry.OpAccessor,
			"RepositorySize":               protoregistry.OpAccessor,
			"RestoreCustomHooks":           protoregistry.OpMutator,
			"SearchFilesByContent":         protoregistry.OpAccessor,
			"SearchFilesByName":            protoregistry.OpAccessor,
			"WriteRef":                     protoregistry.OpMutator,
		},
		"SmartHTTPService": {
			"InfoRefsReceivePack": protoregistry.OpAccessor,
			"InfoRefsUploadPack":  protoregistry.OpAccessor,
			"PostReceivePack":     protoregistry.OpMutator,
			"PostUploadPack":      protoregistry.OpAccessor,
		},
		"SSHService": {
			"SSHReceivePack":   protoregistry.OpMutator,
			"SSHUploadArchive": protoregistry.OpAccessor,
			"SSHUploadPack":    protoregistry.OpAccessor,
		},
		"WikiService": {
			"WikiFindPage":    protoregistry.OpAccessor,
			"WikiGetAllPages": protoregistry.OpAccessor,
			"WikiListPages":   protoregistry.OpAccessor,
			"WikiUpdatePage":  protoregistry.OpMutator,
			"WikiWritePage":   protoregistry.OpMutator,
		},
	}

	for serviceName, methods := range expectedResults {
		for methodName, opType := range methods {
			method := fmt.Sprintf("/gitaly.%s/%s", serviceName, methodName)

			methodInfo, err := protoregistry.GitalyProtoPreregistered.LookupMethod(method)
			require.NoError(t, err)

			require.Equalf(t, opType, methodInfo.Operation, "expect %s:%s to have the correct op type", serviceName, methodName)
			require.Equal(t, method, methodInfo.FullMethodName())
			require.False(t, protoregistry.GitalyProtoPreregistered.IsInterceptedMethod(method), method)
		}
	}
}

func TestNewProtoRegistry_IsInterceptedMethod(t *testing.T) {
	for service, methods := range map[string][]string{
		"ServerService": {
			"ServerInfo",
			"DiskStatistics",
		},
		"PraefectInfoService": {
			"RepositoryReplicas",
			"DatalossCheck",
			"SetAuthoritativeStorage",
		},
	} {
		t.Run(service, func(t *testing.T) {
			for _, method := range methods {
				t.Run(method, func(t *testing.T) {
					fullMethodName := fmt.Sprintf("/gitaly.%s/%s", service, method)
					require.True(t, protoregistry.GitalyProtoPreregistered.IsInterceptedMethod(fullMethodName))
					methodInfo, err := protoregistry.GitalyProtoPreregistered.LookupMethod(fullMethodName)
					require.Empty(t, methodInfo)
					require.Error(t, err, "full method name not found:")
				})
			}
		})
	}
}

func TestRequestFactory(t *testing.T) {
	mInfo, err := protoregistry.GitalyProtoPreregistered.LookupMethod("/gitaly.RepositoryService/RepositoryExists")
	require.NoError(t, err)

	pb, err := mInfo.UnmarshalRequestProto([]byte{})
	require.NoError(t, err)

	testhelper.ProtoEqual(t, &gitalypb.RepositoryExistsRequest{}, pb)
}

func TestMethodInfoScope(t *testing.T) {
	for _, tt := range []struct {
		method string
		scope  protoregistry.Scope
	}{
		{
			method: "/gitaly.RepositoryService/RepositoryExists",
			scope:  protoregistry.ScopeRepository,
		},
	} {
		t.Run(tt.method, func(t *testing.T) {
			mInfo, err := protoregistry.GitalyProtoPreregistered.LookupMethod(tt.method)
			require.NoError(t, err)

			require.Exactly(t, tt.scope, mInfo.Scope)
		})
	}
}
