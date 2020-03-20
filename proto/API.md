# Protocol Documentation
<a name="top"></a>

## Table of Contents

- [blob.proto](#blob.proto)
    - [GetAllLFSPointersRequest](#gitaly.GetAllLFSPointersRequest)
    - [GetAllLFSPointersResponse](#gitaly.GetAllLFSPointersResponse)
    - [GetBlobRequest](#gitaly.GetBlobRequest)
    - [GetBlobResponse](#gitaly.GetBlobResponse)
    - [GetBlobsRequest](#gitaly.GetBlobsRequest)
    - [GetBlobsRequest.RevisionPath](#gitaly.GetBlobsRequest.RevisionPath)
    - [GetBlobsResponse](#gitaly.GetBlobsResponse)
    - [GetLFSPointersRequest](#gitaly.GetLFSPointersRequest)
    - [GetLFSPointersResponse](#gitaly.GetLFSPointersResponse)
    - [GetNewLFSPointersRequest](#gitaly.GetNewLFSPointersRequest)
    - [GetNewLFSPointersResponse](#gitaly.GetNewLFSPointersResponse)
    - [LFSPointer](#gitaly.LFSPointer)
    - [NewBlobObject](#gitaly.NewBlobObject)
  
  
  
    - [BlobService](#gitaly.BlobService)
  

- [cleanup.proto](#cleanup.proto)
    - [ApplyBfgObjectMapStreamRequest](#gitaly.ApplyBfgObjectMapStreamRequest)
    - [ApplyBfgObjectMapStreamResponse](#gitaly.ApplyBfgObjectMapStreamResponse)
    - [ApplyBfgObjectMapStreamResponse.Entry](#gitaly.ApplyBfgObjectMapStreamResponse.Entry)
  
  
  
    - [CleanupService](#gitaly.CleanupService)
  

- [commit.proto](#commit.proto)
    - [CommitIsAncestorRequest](#gitaly.CommitIsAncestorRequest)
    - [CommitIsAncestorResponse](#gitaly.CommitIsAncestorResponse)
    - [CommitLanguagesRequest](#gitaly.CommitLanguagesRequest)
    - [CommitLanguagesResponse](#gitaly.CommitLanguagesResponse)
    - [CommitLanguagesResponse.Language](#gitaly.CommitLanguagesResponse.Language)
    - [CommitStatsRequest](#gitaly.CommitStatsRequest)
    - [CommitStatsResponse](#gitaly.CommitStatsResponse)
    - [CommitsBetweenRequest](#gitaly.CommitsBetweenRequest)
    - [CommitsBetweenResponse](#gitaly.CommitsBetweenResponse)
    - [CommitsByMessageRequest](#gitaly.CommitsByMessageRequest)
    - [CommitsByMessageResponse](#gitaly.CommitsByMessageResponse)
    - [CountCommitsRequest](#gitaly.CountCommitsRequest)
    - [CountCommitsResponse](#gitaly.CountCommitsResponse)
    - [CountDivergingCommitsRequest](#gitaly.CountDivergingCommitsRequest)
    - [CountDivergingCommitsResponse](#gitaly.CountDivergingCommitsResponse)
    - [ExtractCommitSignatureRequest](#gitaly.ExtractCommitSignatureRequest)
    - [ExtractCommitSignatureResponse](#gitaly.ExtractCommitSignatureResponse)
    - [FilterShasWithSignaturesRequest](#gitaly.FilterShasWithSignaturesRequest)
    - [FilterShasWithSignaturesResponse](#gitaly.FilterShasWithSignaturesResponse)
    - [FindAllCommitsRequest](#gitaly.FindAllCommitsRequest)
    - [FindAllCommitsResponse](#gitaly.FindAllCommitsResponse)
    - [FindCommitRequest](#gitaly.FindCommitRequest)
    - [FindCommitResponse](#gitaly.FindCommitResponse)
    - [FindCommitsRequest](#gitaly.FindCommitsRequest)
    - [FindCommitsResponse](#gitaly.FindCommitsResponse)
    - [GetCommitMessagesRequest](#gitaly.GetCommitMessagesRequest)
    - [GetCommitMessagesResponse](#gitaly.GetCommitMessagesResponse)
    - [GetCommitSignaturesRequest](#gitaly.GetCommitSignaturesRequest)
    - [GetCommitSignaturesResponse](#gitaly.GetCommitSignaturesResponse)
    - [GetTreeEntriesRequest](#gitaly.GetTreeEntriesRequest)
    - [GetTreeEntriesResponse](#gitaly.GetTreeEntriesResponse)
    - [LastCommitForPathRequest](#gitaly.LastCommitForPathRequest)
    - [LastCommitForPathResponse](#gitaly.LastCommitForPathResponse)
    - [ListCommitsByOidRequest](#gitaly.ListCommitsByOidRequest)
    - [ListCommitsByOidResponse](#gitaly.ListCommitsByOidResponse)
    - [ListCommitsByRefNameRequest](#gitaly.ListCommitsByRefNameRequest)
    - [ListCommitsByRefNameResponse](#gitaly.ListCommitsByRefNameResponse)
    - [ListFilesRequest](#gitaly.ListFilesRequest)
    - [ListFilesResponse](#gitaly.ListFilesResponse)
    - [ListLastCommitsForTreeRequest](#gitaly.ListLastCommitsForTreeRequest)
    - [ListLastCommitsForTreeResponse](#gitaly.ListLastCommitsForTreeResponse)
    - [ListLastCommitsForTreeResponse.CommitForTree](#gitaly.ListLastCommitsForTreeResponse.CommitForTree)
    - [RawBlameRequest](#gitaly.RawBlameRequest)
    - [RawBlameResponse](#gitaly.RawBlameResponse)
    - [TreeEntry](#gitaly.TreeEntry)
    - [TreeEntryRequest](#gitaly.TreeEntryRequest)
    - [TreeEntryResponse](#gitaly.TreeEntryResponse)
  
    - [FindAllCommitsRequest.Order](#gitaly.FindAllCommitsRequest.Order)
    - [FindCommitsRequest.Order](#gitaly.FindCommitsRequest.Order)
    - [TreeEntry.EntryType](#gitaly.TreeEntry.EntryType)
    - [TreeEntryResponse.ObjectType](#gitaly.TreeEntryResponse.ObjectType)
  
  
    - [CommitService](#gitaly.CommitService)
  

- [conflicts.proto](#conflicts.proto)
    - [ConflictFile](#gitaly.ConflictFile)
    - [ConflictFileHeader](#gitaly.ConflictFileHeader)
    - [ListConflictFilesRequest](#gitaly.ListConflictFilesRequest)
    - [ListConflictFilesResponse](#gitaly.ListConflictFilesResponse)
    - [ResolveConflictsRequest](#gitaly.ResolveConflictsRequest)
    - [ResolveConflictsRequestHeader](#gitaly.ResolveConflictsRequestHeader)
    - [ResolveConflictsResponse](#gitaly.ResolveConflictsResponse)
  
  
  
    - [ConflictsService](#gitaly.ConflictsService)
  

- [diff.proto](#diff.proto)
    - [CommitDelta](#gitaly.CommitDelta)
    - [CommitDeltaRequest](#gitaly.CommitDeltaRequest)
    - [CommitDeltaResponse](#gitaly.CommitDeltaResponse)
    - [CommitDiffRequest](#gitaly.CommitDiffRequest)
    - [CommitDiffResponse](#gitaly.CommitDiffResponse)
    - [DiffStats](#gitaly.DiffStats)
    - [DiffStatsRequest](#gitaly.DiffStatsRequest)
    - [DiffStatsResponse](#gitaly.DiffStatsResponse)
    - [RawDiffRequest](#gitaly.RawDiffRequest)
    - [RawDiffResponse](#gitaly.RawDiffResponse)
    - [RawPatchRequest](#gitaly.RawPatchRequest)
    - [RawPatchResponse](#gitaly.RawPatchResponse)
  
  
  
    - [DiffService](#gitaly.DiffService)
  

- [hook.proto](#hook.proto)
    - [PostReceiveHookRequest](#gitaly.PostReceiveHookRequest)
    - [PostReceiveHookResponse](#gitaly.PostReceiveHookResponse)
    - [PreReceiveHookRequest](#gitaly.PreReceiveHookRequest)
    - [PreReceiveHookResponse](#gitaly.PreReceiveHookResponse)
    - [UpdateHookRequest](#gitaly.UpdateHookRequest)
    - [UpdateHookResponse](#gitaly.UpdateHookResponse)
  
  
  
    - [HookService](#gitaly.HookService)
  

- [internal.proto](#internal.proto)
    - [WalkReposRequest](#gitaly.WalkReposRequest)
    - [WalkReposResponse](#gitaly.WalkReposResponse)
  
  
  
    - [InternalGitaly](#gitaly.InternalGitaly)
  

- [lint.proto](#lint.proto)
    - [OperationMsg](#gitaly.OperationMsg)
  
    - [OperationMsg.Operation](#gitaly.OperationMsg.Operation)
    - [OperationMsg.Scope](#gitaly.OperationMsg.Scope)
  
    - [File-level Extensions](#lint.proto-extensions)
    - [File-level Extensions](#lint.proto-extensions)
    - [File-level Extensions](#lint.proto-extensions)
    - [File-level Extensions](#lint.proto-extensions)
    - [File-level Extensions](#lint.proto-extensions)
  
  

- [namespace.proto](#namespace.proto)
    - [AddNamespaceRequest](#gitaly.AddNamespaceRequest)
    - [AddNamespaceResponse](#gitaly.AddNamespaceResponse)
    - [NamespaceExistsRequest](#gitaly.NamespaceExistsRequest)
    - [NamespaceExistsResponse](#gitaly.NamespaceExistsResponse)
    - [RemoveNamespaceRequest](#gitaly.RemoveNamespaceRequest)
    - [RemoveNamespaceResponse](#gitaly.RemoveNamespaceResponse)
    - [RenameNamespaceRequest](#gitaly.RenameNamespaceRequest)
    - [RenameNamespaceResponse](#gitaly.RenameNamespaceResponse)
  
  
  
    - [NamespaceService](#gitaly.NamespaceService)
  

- [objectpool.proto](#objectpool.proto)
    - [CreateObjectPoolRequest](#gitaly.CreateObjectPoolRequest)
    - [CreateObjectPoolResponse](#gitaly.CreateObjectPoolResponse)
    - [DeleteObjectPoolRequest](#gitaly.DeleteObjectPoolRequest)
    - [DeleteObjectPoolResponse](#gitaly.DeleteObjectPoolResponse)
    - [DisconnectGitAlternatesRequest](#gitaly.DisconnectGitAlternatesRequest)
    - [DisconnectGitAlternatesResponse](#gitaly.DisconnectGitAlternatesResponse)
    - [FetchIntoObjectPoolRequest](#gitaly.FetchIntoObjectPoolRequest)
    - [FetchIntoObjectPoolResponse](#gitaly.FetchIntoObjectPoolResponse)
    - [GetObjectPoolRequest](#gitaly.GetObjectPoolRequest)
    - [GetObjectPoolResponse](#gitaly.GetObjectPoolResponse)
    - [LinkRepositoryToObjectPoolRequest](#gitaly.LinkRepositoryToObjectPoolRequest)
    - [LinkRepositoryToObjectPoolResponse](#gitaly.LinkRepositoryToObjectPoolResponse)
    - [ReduplicateRepositoryRequest](#gitaly.ReduplicateRepositoryRequest)
    - [ReduplicateRepositoryResponse](#gitaly.ReduplicateRepositoryResponse)
    - [UnlinkRepositoryFromObjectPoolRequest](#gitaly.UnlinkRepositoryFromObjectPoolRequest)
    - [UnlinkRepositoryFromObjectPoolResponse](#gitaly.UnlinkRepositoryFromObjectPoolResponse)
  
  
  
    - [ObjectPoolService](#gitaly.ObjectPoolService)
  

- [operations.proto](#operations.proto)
    - [OperationBranchUpdate](#gitaly.OperationBranchUpdate)
    - [UserApplyPatchRequest](#gitaly.UserApplyPatchRequest)
    - [UserApplyPatchRequest.Header](#gitaly.UserApplyPatchRequest.Header)
    - [UserApplyPatchResponse](#gitaly.UserApplyPatchResponse)
    - [UserCherryPickRequest](#gitaly.UserCherryPickRequest)
    - [UserCherryPickResponse](#gitaly.UserCherryPickResponse)
    - [UserCommitFilesAction](#gitaly.UserCommitFilesAction)
    - [UserCommitFilesActionHeader](#gitaly.UserCommitFilesActionHeader)
    - [UserCommitFilesRequest](#gitaly.UserCommitFilesRequest)
    - [UserCommitFilesRequestHeader](#gitaly.UserCommitFilesRequestHeader)
    - [UserCommitFilesResponse](#gitaly.UserCommitFilesResponse)
    - [UserCreateBranchRequest](#gitaly.UserCreateBranchRequest)
    - [UserCreateBranchResponse](#gitaly.UserCreateBranchResponse)
    - [UserCreateTagRequest](#gitaly.UserCreateTagRequest)
    - [UserCreateTagResponse](#gitaly.UserCreateTagResponse)
    - [UserDeleteBranchRequest](#gitaly.UserDeleteBranchRequest)
    - [UserDeleteBranchResponse](#gitaly.UserDeleteBranchResponse)
    - [UserDeleteTagRequest](#gitaly.UserDeleteTagRequest)
    - [UserDeleteTagResponse](#gitaly.UserDeleteTagResponse)
    - [UserFFBranchRequest](#gitaly.UserFFBranchRequest)
    - [UserFFBranchResponse](#gitaly.UserFFBranchResponse)
    - [UserMergeBranchRequest](#gitaly.UserMergeBranchRequest)
    - [UserMergeBranchResponse](#gitaly.UserMergeBranchResponse)
    - [UserMergeToRefRequest](#gitaly.UserMergeToRefRequest)
    - [UserMergeToRefResponse](#gitaly.UserMergeToRefResponse)
    - [UserRebaseConfirmableRequest](#gitaly.UserRebaseConfirmableRequest)
    - [UserRebaseConfirmableRequest.Header](#gitaly.UserRebaseConfirmableRequest.Header)
    - [UserRebaseConfirmableResponse](#gitaly.UserRebaseConfirmableResponse)
    - [UserRevertRequest](#gitaly.UserRevertRequest)
    - [UserRevertResponse](#gitaly.UserRevertResponse)
    - [UserSquashRequest](#gitaly.UserSquashRequest)
    - [UserSquashResponse](#gitaly.UserSquashResponse)
    - [UserUpdateBranchRequest](#gitaly.UserUpdateBranchRequest)
    - [UserUpdateBranchResponse](#gitaly.UserUpdateBranchResponse)
    - [UserUpdateSubmoduleRequest](#gitaly.UserUpdateSubmoduleRequest)
    - [UserUpdateSubmoduleResponse](#gitaly.UserUpdateSubmoduleResponse)
  
    - [UserCherryPickResponse.CreateTreeError](#gitaly.UserCherryPickResponse.CreateTreeError)
    - [UserCommitFilesActionHeader.ActionType](#gitaly.UserCommitFilesActionHeader.ActionType)
    - [UserRevertResponse.CreateTreeError](#gitaly.UserRevertResponse.CreateTreeError)
  
  
    - [OperationService](#gitaly.OperationService)
  

- [praefect.proto](#praefect.proto)
    - [RepositoryReplicasRequest](#gitaly.RepositoryReplicasRequest)
    - [RepositoryReplicasResponse](#gitaly.RepositoryReplicasResponse)
    - [RepositoryReplicasResponse.RepositoryDetails](#gitaly.RepositoryReplicasResponse.RepositoryDetails)
  
  
  
    - [PraefectInfoService](#gitaly.PraefectInfoService)
  

- [ref.proto](#ref.proto)
    - [CreateBranchRequest](#gitaly.CreateBranchRequest)
    - [CreateBranchResponse](#gitaly.CreateBranchResponse)
    - [DeleteBranchRequest](#gitaly.DeleteBranchRequest)
    - [DeleteBranchResponse](#gitaly.DeleteBranchResponse)
    - [DeleteRefsRequest](#gitaly.DeleteRefsRequest)
    - [DeleteRefsResponse](#gitaly.DeleteRefsResponse)
    - [FindAllBranchNamesRequest](#gitaly.FindAllBranchNamesRequest)
    - [FindAllBranchNamesResponse](#gitaly.FindAllBranchNamesResponse)
    - [FindAllBranchesRequest](#gitaly.FindAllBranchesRequest)
    - [FindAllBranchesResponse](#gitaly.FindAllBranchesResponse)
    - [FindAllBranchesResponse.Branch](#gitaly.FindAllBranchesResponse.Branch)
    - [FindAllRemoteBranchesRequest](#gitaly.FindAllRemoteBranchesRequest)
    - [FindAllRemoteBranchesResponse](#gitaly.FindAllRemoteBranchesResponse)
    - [FindAllTagNamesRequest](#gitaly.FindAllTagNamesRequest)
    - [FindAllTagNamesResponse](#gitaly.FindAllTagNamesResponse)
    - [FindAllTagsRequest](#gitaly.FindAllTagsRequest)
    - [FindAllTagsResponse](#gitaly.FindAllTagsResponse)
    - [FindBranchRequest](#gitaly.FindBranchRequest)
    - [FindBranchResponse](#gitaly.FindBranchResponse)
    - [FindDefaultBranchNameRequest](#gitaly.FindDefaultBranchNameRequest)
    - [FindDefaultBranchNameResponse](#gitaly.FindDefaultBranchNameResponse)
    - [FindLocalBranchCommitAuthor](#gitaly.FindLocalBranchCommitAuthor)
    - [FindLocalBranchResponse](#gitaly.FindLocalBranchResponse)
    - [FindLocalBranchesRequest](#gitaly.FindLocalBranchesRequest)
    - [FindLocalBranchesResponse](#gitaly.FindLocalBranchesResponse)
    - [FindRefNameRequest](#gitaly.FindRefNameRequest)
    - [FindRefNameResponse](#gitaly.FindRefNameResponse)
    - [FindTagRequest](#gitaly.FindTagRequest)
    - [FindTagResponse](#gitaly.FindTagResponse)
    - [GetTagMessagesRequest](#gitaly.GetTagMessagesRequest)
    - [GetTagMessagesResponse](#gitaly.GetTagMessagesResponse)
    - [ListBranchNamesContainingCommitRequest](#gitaly.ListBranchNamesContainingCommitRequest)
    - [ListBranchNamesContainingCommitResponse](#gitaly.ListBranchNamesContainingCommitResponse)
    - [ListNewBlobsRequest](#gitaly.ListNewBlobsRequest)
    - [ListNewBlobsResponse](#gitaly.ListNewBlobsResponse)
    - [ListNewCommitsRequest](#gitaly.ListNewCommitsRequest)
    - [ListNewCommitsResponse](#gitaly.ListNewCommitsResponse)
    - [ListTagNamesContainingCommitRequest](#gitaly.ListTagNamesContainingCommitRequest)
    - [ListTagNamesContainingCommitResponse](#gitaly.ListTagNamesContainingCommitResponse)
    - [PackRefsRequest](#gitaly.PackRefsRequest)
    - [PackRefsResponse](#gitaly.PackRefsResponse)
    - [RefExistsRequest](#gitaly.RefExistsRequest)
    - [RefExistsResponse](#gitaly.RefExistsResponse)
  
    - [CreateBranchResponse.Status](#gitaly.CreateBranchResponse.Status)
    - [FindLocalBranchesRequest.SortBy](#gitaly.FindLocalBranchesRequest.SortBy)
  
  
    - [RefService](#gitaly.RefService)
  

- [remote.proto](#remote.proto)
    - [AddRemoteRequest](#gitaly.AddRemoteRequest)
    - [AddRemoteResponse](#gitaly.AddRemoteResponse)
    - [FetchInternalRemoteRequest](#gitaly.FetchInternalRemoteRequest)
    - [FetchInternalRemoteResponse](#gitaly.FetchInternalRemoteResponse)
    - [FindRemoteRepositoryRequest](#gitaly.FindRemoteRepositoryRequest)
    - [FindRemoteRepositoryResponse](#gitaly.FindRemoteRepositoryResponse)
    - [FindRemoteRootRefRequest](#gitaly.FindRemoteRootRefRequest)
    - [FindRemoteRootRefResponse](#gitaly.FindRemoteRootRefResponse)
    - [ListRemotesRequest](#gitaly.ListRemotesRequest)
    - [ListRemotesResponse](#gitaly.ListRemotesResponse)
    - [ListRemotesResponse.Remote](#gitaly.ListRemotesResponse.Remote)
    - [RemoveRemoteRequest](#gitaly.RemoveRemoteRequest)
    - [RemoveRemoteResponse](#gitaly.RemoveRemoteResponse)
    - [UpdateRemoteMirrorRequest](#gitaly.UpdateRemoteMirrorRequest)
    - [UpdateRemoteMirrorResponse](#gitaly.UpdateRemoteMirrorResponse)
  
  
  
    - [RemoteService](#gitaly.RemoteService)
  

- [repository-service.proto](#repository-service.proto)
    - [ApplyGitattributesRequest](#gitaly.ApplyGitattributesRequest)
    - [ApplyGitattributesResponse](#gitaly.ApplyGitattributesResponse)
    - [BackupCustomHooksRequest](#gitaly.BackupCustomHooksRequest)
    - [BackupCustomHooksResponse](#gitaly.BackupCustomHooksResponse)
    - [CalculateChecksumRequest](#gitaly.CalculateChecksumRequest)
    - [CalculateChecksumResponse](#gitaly.CalculateChecksumResponse)
    - [CleanupRequest](#gitaly.CleanupRequest)
    - [CleanupResponse](#gitaly.CleanupResponse)
    - [CloneFromPoolInternalRequest](#gitaly.CloneFromPoolInternalRequest)
    - [CloneFromPoolInternalResponse](#gitaly.CloneFromPoolInternalResponse)
    - [CloneFromPoolRequest](#gitaly.CloneFromPoolRequest)
    - [CloneFromPoolResponse](#gitaly.CloneFromPoolResponse)
    - [CreateBundleRequest](#gitaly.CreateBundleRequest)
    - [CreateBundleResponse](#gitaly.CreateBundleResponse)
    - [CreateForkRequest](#gitaly.CreateForkRequest)
    - [CreateForkResponse](#gitaly.CreateForkResponse)
    - [CreateRepositoryFromBundleRequest](#gitaly.CreateRepositoryFromBundleRequest)
    - [CreateRepositoryFromBundleResponse](#gitaly.CreateRepositoryFromBundleResponse)
    - [CreateRepositoryFromSnapshotRequest](#gitaly.CreateRepositoryFromSnapshotRequest)
    - [CreateRepositoryFromSnapshotResponse](#gitaly.CreateRepositoryFromSnapshotResponse)
    - [CreateRepositoryFromURLRequest](#gitaly.CreateRepositoryFromURLRequest)
    - [CreateRepositoryFromURLResponse](#gitaly.CreateRepositoryFromURLResponse)
    - [CreateRepositoryRequest](#gitaly.CreateRepositoryRequest)
    - [CreateRepositoryResponse](#gitaly.CreateRepositoryResponse)
    - [DeleteConfigRequest](#gitaly.DeleteConfigRequest)
    - [DeleteConfigResponse](#gitaly.DeleteConfigResponse)
    - [FetchHTTPRemoteRequest](#gitaly.FetchHTTPRemoteRequest)
    - [FetchHTTPRemoteResponse](#gitaly.FetchHTTPRemoteResponse)
    - [FetchRemoteRequest](#gitaly.FetchRemoteRequest)
    - [FetchRemoteResponse](#gitaly.FetchRemoteResponse)
    - [FetchSourceBranchRequest](#gitaly.FetchSourceBranchRequest)
    - [FetchSourceBranchResponse](#gitaly.FetchSourceBranchResponse)
    - [FindLicenseRequest](#gitaly.FindLicenseRequest)
    - [FindLicenseResponse](#gitaly.FindLicenseResponse)
    - [FindMergeBaseRequest](#gitaly.FindMergeBaseRequest)
    - [FindMergeBaseResponse](#gitaly.FindMergeBaseResponse)
    - [FsckRequest](#gitaly.FsckRequest)
    - [FsckResponse](#gitaly.FsckResponse)
    - [GarbageCollectRequest](#gitaly.GarbageCollectRequest)
    - [GarbageCollectResponse](#gitaly.GarbageCollectResponse)
    - [GetArchiveRequest](#gitaly.GetArchiveRequest)
    - [GetArchiveResponse](#gitaly.GetArchiveResponse)
    - [GetInfoAttributesRequest](#gitaly.GetInfoAttributesRequest)
    - [GetInfoAttributesResponse](#gitaly.GetInfoAttributesResponse)
    - [GetObjectDirectorySizeRequest](#gitaly.GetObjectDirectorySizeRequest)
    - [GetObjectDirectorySizeResponse](#gitaly.GetObjectDirectorySizeResponse)
    - [GetRawChangesRequest](#gitaly.GetRawChangesRequest)
    - [GetRawChangesResponse](#gitaly.GetRawChangesResponse)
    - [GetRawChangesResponse.RawChange](#gitaly.GetRawChangesResponse.RawChange)
    - [GetSnapshotRequest](#gitaly.GetSnapshotRequest)
    - [GetSnapshotResponse](#gitaly.GetSnapshotResponse)
    - [HasLocalBranchesRequest](#gitaly.HasLocalBranchesRequest)
    - [HasLocalBranchesResponse](#gitaly.HasLocalBranchesResponse)
    - [IsRebaseInProgressRequest](#gitaly.IsRebaseInProgressRequest)
    - [IsRebaseInProgressResponse](#gitaly.IsRebaseInProgressResponse)
    - [IsSquashInProgressRequest](#gitaly.IsSquashInProgressRequest)
    - [IsSquashInProgressResponse](#gitaly.IsSquashInProgressResponse)
    - [Remote](#gitaly.Remote)
    - [RemoveRepositoryRequest](#gitaly.RemoveRepositoryRequest)
    - [RemoveRepositoryResponse](#gitaly.RemoveRepositoryResponse)
    - [RenameRepositoryRequest](#gitaly.RenameRepositoryRequest)
    - [RenameRepositoryResponse](#gitaly.RenameRepositoryResponse)
    - [RepackFullRequest](#gitaly.RepackFullRequest)
    - [RepackFullResponse](#gitaly.RepackFullResponse)
    - [RepackIncrementalRequest](#gitaly.RepackIncrementalRequest)
    - [RepackIncrementalResponse](#gitaly.RepackIncrementalResponse)
    - [ReplicateRepositoryRequest](#gitaly.ReplicateRepositoryRequest)
    - [ReplicateRepositoryResponse](#gitaly.ReplicateRepositoryResponse)
    - [RepositoryExistsRequest](#gitaly.RepositoryExistsRequest)
    - [RepositoryExistsResponse](#gitaly.RepositoryExistsResponse)
    - [RepositorySizeRequest](#gitaly.RepositorySizeRequest)
    - [RepositorySizeResponse](#gitaly.RepositorySizeResponse)
    - [RestoreCustomHooksRequest](#gitaly.RestoreCustomHooksRequest)
    - [RestoreCustomHooksResponse](#gitaly.RestoreCustomHooksResponse)
    - [SearchFilesByContentRequest](#gitaly.SearchFilesByContentRequest)
    - [SearchFilesByContentResponse](#gitaly.SearchFilesByContentResponse)
    - [SearchFilesByNameRequest](#gitaly.SearchFilesByNameRequest)
    - [SearchFilesByNameResponse](#gitaly.SearchFilesByNameResponse)
    - [SetConfigRequest](#gitaly.SetConfigRequest)
    - [SetConfigRequest.Entry](#gitaly.SetConfigRequest.Entry)
    - [SetConfigResponse](#gitaly.SetConfigResponse)
    - [WriteRefRequest](#gitaly.WriteRefRequest)
    - [WriteRefResponse](#gitaly.WriteRefResponse)
  
    - [GetArchiveRequest.Format](#gitaly.GetArchiveRequest.Format)
    - [GetRawChangesResponse.RawChange.Operation](#gitaly.GetRawChangesResponse.RawChange.Operation)
  
  
    - [RepositoryService](#gitaly.RepositoryService)
  

- [server.proto](#server.proto)
    - [DiskStatisticsRequest](#gitaly.DiskStatisticsRequest)
    - [DiskStatisticsResponse](#gitaly.DiskStatisticsResponse)
    - [DiskStatisticsResponse.StorageStatus](#gitaly.DiskStatisticsResponse.StorageStatus)
    - [ServerInfoRequest](#gitaly.ServerInfoRequest)
    - [ServerInfoResponse](#gitaly.ServerInfoResponse)
    - [ServerInfoResponse.StorageStatus](#gitaly.ServerInfoResponse.StorageStatus)
  
  
  
    - [ServerService](#gitaly.ServerService)
  

- [shared.proto](#shared.proto)
    - [Branch](#gitaly.Branch)
    - [CommitAuthor](#gitaly.CommitAuthor)
    - [ExitStatus](#gitaly.ExitStatus)
    - [GitCommit](#gitaly.GitCommit)
    - [ObjectPool](#gitaly.ObjectPool)
    - [Repository](#gitaly.Repository)
    - [Tag](#gitaly.Tag)
    - [User](#gitaly.User)
  
    - [ObjectType](#gitaly.ObjectType)
    - [SignatureType](#gitaly.SignatureType)
  
  
  

- [smarthttp.proto](#smarthttp.proto)
    - [InfoRefsRequest](#gitaly.InfoRefsRequest)
    - [InfoRefsResponse](#gitaly.InfoRefsResponse)
    - [PostReceivePackRequest](#gitaly.PostReceivePackRequest)
    - [PostReceivePackResponse](#gitaly.PostReceivePackResponse)
    - [PostUploadPackRequest](#gitaly.PostUploadPackRequest)
    - [PostUploadPackResponse](#gitaly.PostUploadPackResponse)
  
  
  
    - [SmartHTTPService](#gitaly.SmartHTTPService)
  

- [ssh.proto](#ssh.proto)
    - [SSHReceivePackRequest](#gitaly.SSHReceivePackRequest)
    - [SSHReceivePackResponse](#gitaly.SSHReceivePackResponse)
    - [SSHUploadArchiveRequest](#gitaly.SSHUploadArchiveRequest)
    - [SSHUploadArchiveResponse](#gitaly.SSHUploadArchiveResponse)
    - [SSHUploadPackRequest](#gitaly.SSHUploadPackRequest)
    - [SSHUploadPackResponse](#gitaly.SSHUploadPackResponse)
  
  
  
    - [SSHService](#gitaly.SSHService)
  

- [wiki.proto](#wiki.proto)
    - [WikiCommitDetails](#gitaly.WikiCommitDetails)
    - [WikiDeletePageRequest](#gitaly.WikiDeletePageRequest)
    - [WikiDeletePageResponse](#gitaly.WikiDeletePageResponse)
    - [WikiFindFileRequest](#gitaly.WikiFindFileRequest)
    - [WikiFindFileResponse](#gitaly.WikiFindFileResponse)
    - [WikiFindPageRequest](#gitaly.WikiFindPageRequest)
    - [WikiFindPageResponse](#gitaly.WikiFindPageResponse)
    - [WikiGetAllPagesRequest](#gitaly.WikiGetAllPagesRequest)
    - [WikiGetAllPagesResponse](#gitaly.WikiGetAllPagesResponse)
    - [WikiGetPageVersionsRequest](#gitaly.WikiGetPageVersionsRequest)
    - [WikiGetPageVersionsResponse](#gitaly.WikiGetPageVersionsResponse)
    - [WikiListPagesRequest](#gitaly.WikiListPagesRequest)
    - [WikiListPagesResponse](#gitaly.WikiListPagesResponse)
    - [WikiPage](#gitaly.WikiPage)
    - [WikiPageVersion](#gitaly.WikiPageVersion)
    - [WikiUpdatePageRequest](#gitaly.WikiUpdatePageRequest)
    - [WikiUpdatePageResponse](#gitaly.WikiUpdatePageResponse)
    - [WikiWritePageRequest](#gitaly.WikiWritePageRequest)
    - [WikiWritePageResponse](#gitaly.WikiWritePageResponse)
  
    - [WikiGetAllPagesRequest.SortBy](#gitaly.WikiGetAllPagesRequest.SortBy)
    - [WikiListPagesRequest.SortBy](#gitaly.WikiListPagesRequest.SortBy)
  
  
    - [WikiService](#gitaly.WikiService)
  

- [Scalar Value Types](#scalar-value-types)



<a name="blob.proto"></a>
<p align="right"><a href="#top">Top</a></p>

## blob.proto



<a name="gitaly.GetAllLFSPointersRequest"></a>

### GetAllLFSPointersRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  |  |






<a name="gitaly.GetAllLFSPointersResponse"></a>

### GetAllLFSPointersResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| lfs_pointers | [LFSPointer](#gitaly.LFSPointer) | repeated |  |






<a name="gitaly.GetBlobRequest"></a>

### GetBlobRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  |  |
| oid | [string](#string) |  | Object ID (SHA1) of the blob we want to get |
| limit | [int64](#int64) |  | Maximum number of bytes we want to receive. Use &#39;-1&#39; to get the full blob no matter how big. |






<a name="gitaly.GetBlobResponse"></a>

### GetBlobResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| size | [int64](#int64) |  | Blob size; present only in first response message |
| data | [bytes](#bytes) |  | Chunk of blob data |
| oid | [string](#string) |  | Object ID of the actual blob returned. Empty if no blob was found. |






<a name="gitaly.GetBlobsRequest"></a>

### GetBlobsRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  |  |
| revision_paths | [GetBlobsRequest.RevisionPath](#gitaly.GetBlobsRequest.RevisionPath) | repeated | Revision/Path pairs of the blobs we want to get. |
| limit | [int64](#int64) |  | Maximum number of bytes we want to receive. Use &#39;-1&#39; to get the full blobs no matter how big. |






<a name="gitaly.GetBlobsRequest.RevisionPath"></a>

### GetBlobsRequest.RevisionPath



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| revision | [string](#string) |  |  |
| path | [bytes](#bytes) |  |  |






<a name="gitaly.GetBlobsResponse"></a>

### GetBlobsResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| size | [int64](#int64) |  | Blob size; present only on the first message per blob |
| data | [bytes](#bytes) |  | Chunk of blob data, could span over multiple messages. |
| oid | [string](#string) |  | Object ID of the current blob. Only present on the first message per blob. Empty if no blob was found. |
| is_submodule | [bool](#bool) |  |  |
| mode | [int32](#int32) |  |  |
| revision | [string](#string) |  |  |
| path | [bytes](#bytes) |  |  |
| type | [ObjectType](#gitaly.ObjectType) |  |  |






<a name="gitaly.GetLFSPointersRequest"></a>

### GetLFSPointersRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  |  |
| blob_ids | [string](#string) | repeated |  |






<a name="gitaly.GetLFSPointersResponse"></a>

### GetLFSPointersResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| lfs_pointers | [LFSPointer](#gitaly.LFSPointer) | repeated |  |






<a name="gitaly.GetNewLFSPointersRequest"></a>

### GetNewLFSPointersRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  |  |
| revision | [bytes](#bytes) |  |  |
| limit | [int32](#int32) |  |  |
| not_in_all | [bool](#bool) |  | Note: When `not_in_all` is true, `not_in_refs` is ignored |
| not_in_refs | [bytes](#bytes) | repeated |  |






<a name="gitaly.GetNewLFSPointersResponse"></a>

### GetNewLFSPointersResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| lfs_pointers | [LFSPointer](#gitaly.LFSPointer) | repeated |  |






<a name="gitaly.LFSPointer"></a>

### LFSPointer



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| size | [int64](#int64) |  |  |
| data | [bytes](#bytes) |  |  |
| oid | [string](#string) |  |  |






<a name="gitaly.NewBlobObject"></a>

### NewBlobObject



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| size | [int64](#int64) |  |  |
| oid | [string](#string) |  |  |
| path | [bytes](#bytes) |  |  |





 

 

 


<a name="gitaly.BlobService"></a>

### BlobService


| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| GetBlob | [GetBlobRequest](#gitaly.GetBlobRequest) | [GetBlobResponse](#gitaly.GetBlobResponse) stream | GetBlob returns the contents of a blob object referenced by its object ID. We use a stream to return a chunked arbitrarily large binary response |
| GetBlobs | [GetBlobsRequest](#gitaly.GetBlobsRequest) | [GetBlobsResponse](#gitaly.GetBlobsResponse) stream |  |
| GetLFSPointers | [GetLFSPointersRequest](#gitaly.GetLFSPointersRequest) | [GetLFSPointersResponse](#gitaly.GetLFSPointersResponse) stream |  |
| GetNewLFSPointers | [GetNewLFSPointersRequest](#gitaly.GetNewLFSPointersRequest) | [GetNewLFSPointersResponse](#gitaly.GetNewLFSPointersResponse) stream |  |
| GetAllLFSPointers | [GetAllLFSPointersRequest](#gitaly.GetAllLFSPointersRequest) | [GetAllLFSPointersResponse](#gitaly.GetAllLFSPointersResponse) stream |  |

 



<a name="cleanup.proto"></a>
<p align="right"><a href="#top">Top</a></p>

## cleanup.proto



<a name="gitaly.ApplyBfgObjectMapStreamRequest"></a>

### ApplyBfgObjectMapStreamRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  | Only available on the first message |
| object_map | [bytes](#bytes) |  | A raw object-map file as generated by BFG: https://rtyley.github.io/bfg-repo-cleaner Each line in the file has two object SHAs, space-separated - the original SHA of the object, and the SHA after BFG has rewritten the object. |






<a name="gitaly.ApplyBfgObjectMapStreamResponse"></a>

### ApplyBfgObjectMapStreamResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| entries | [ApplyBfgObjectMapStreamResponse.Entry](#gitaly.ApplyBfgObjectMapStreamResponse.Entry) | repeated |  |






<a name="gitaly.ApplyBfgObjectMapStreamResponse.Entry"></a>

### ApplyBfgObjectMapStreamResponse.Entry
We send back each parsed entry in the request&#39;s object map so the client
can take action


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| type | [ObjectType](#gitaly.ObjectType) |  |  |
| old_oid | [string](#string) |  |  |
| new_oid | [string](#string) |  |  |





 

 

 


<a name="gitaly.CleanupService"></a>

### CleanupService


| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| ApplyBfgObjectMapStream | [ApplyBfgObjectMapStreamRequest](#gitaly.ApplyBfgObjectMapStreamRequest) stream | [ApplyBfgObjectMapStreamResponse](#gitaly.ApplyBfgObjectMapStreamResponse) stream |  |

 



<a name="commit.proto"></a>
<p align="right"><a href="#top">Top</a></p>

## commit.proto



<a name="gitaly.CommitIsAncestorRequest"></a>

### CommitIsAncestorRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  |  |
| ancestor_id | [string](#string) |  |  |
| child_id | [string](#string) |  |  |






<a name="gitaly.CommitIsAncestorResponse"></a>

### CommitIsAncestorResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| value | [bool](#bool) |  |  |






<a name="gitaly.CommitLanguagesRequest"></a>

### CommitLanguagesRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  |  |
| revision | [bytes](#bytes) |  |  |






<a name="gitaly.CommitLanguagesResponse"></a>

### CommitLanguagesResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| languages | [CommitLanguagesResponse.Language](#gitaly.CommitLanguagesResponse.Language) | repeated |  |






<a name="gitaly.CommitLanguagesResponse.Language"></a>

### CommitLanguagesResponse.Language



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [string](#string) |  |  |
| share | [float](#float) |  |  |
| color | [string](#string) |  |  |
| file_count | [uint32](#uint32) |  |  |
| bytes | [uint64](#uint64) |  |  |






<a name="gitaly.CommitStatsRequest"></a>

### CommitStatsRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  |  |
| revision | [bytes](#bytes) |  |  |






<a name="gitaly.CommitStatsResponse"></a>

### CommitStatsResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| oid | [string](#string) |  | OID is the commit. Empty means not found |
| additions | [int32](#int32) |  |  |
| deletions | [int32](#int32) |  |  |






<a name="gitaly.CommitsBetweenRequest"></a>

### CommitsBetweenRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  |  |
| from | [bytes](#bytes) |  |  |
| to | [bytes](#bytes) |  |  |






<a name="gitaly.CommitsBetweenResponse"></a>

### CommitsBetweenResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| commits | [GitCommit](#gitaly.GitCommit) | repeated |  |






<a name="gitaly.CommitsByMessageRequest"></a>

### CommitsByMessageRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  |  |
| revision | [bytes](#bytes) |  |  |
| offset | [int32](#int32) |  |  |
| limit | [int32](#int32) |  |  |
| path | [bytes](#bytes) |  |  |
| query | [string](#string) |  |  |






<a name="gitaly.CommitsByMessageResponse"></a>

### CommitsByMessageResponse
One &#39;page&#39; of the paginated response of CommitsByMessage


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| commits | [GitCommit](#gitaly.GitCommit) | repeated |  |






<a name="gitaly.CountCommitsRequest"></a>

### CountCommitsRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  |  |
| revision | [bytes](#bytes) |  |  |
| after | [google.protobuf.Timestamp](#google.protobuf.Timestamp) |  |  |
| before | [google.protobuf.Timestamp](#google.protobuf.Timestamp) |  |  |
| path | [bytes](#bytes) |  |  |
| max_count | [int32](#int32) |  |  |
| all | [bool](#bool) |  | all and revision are mutually exclusive |
| first_parent | [bool](#bool) |  |  |






<a name="gitaly.CountCommitsResponse"></a>

### CountCommitsResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| count | [int32](#int32) |  |  |






<a name="gitaly.CountDivergingCommitsRequest"></a>

### CountDivergingCommitsRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  |  |
| from | [bytes](#bytes) |  |  |
| to | [bytes](#bytes) |  |  |
| max_count | [int32](#int32) |  |  |






<a name="gitaly.CountDivergingCommitsResponse"></a>

### CountDivergingCommitsResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| left_count | [int32](#int32) |  |  |
| right_count | [int32](#int32) |  |  |






<a name="gitaly.ExtractCommitSignatureRequest"></a>

### ExtractCommitSignatureRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  |  |
| commit_id | [string](#string) |  |  |






<a name="gitaly.ExtractCommitSignatureResponse"></a>

### ExtractCommitSignatureResponse
Either of the &#39;signature&#39; and &#39;signed_text&#39; fields may be present. It
is up to the caller to stitch them together.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| signature | [bytes](#bytes) |  |  |
| signed_text | [bytes](#bytes) |  |  |






<a name="gitaly.FilterShasWithSignaturesRequest"></a>

### FilterShasWithSignaturesRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  |  |
| shas | [bytes](#bytes) | repeated |  |






<a name="gitaly.FilterShasWithSignaturesResponse"></a>

### FilterShasWithSignaturesResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| shas | [bytes](#bytes) | repeated |  |






<a name="gitaly.FindAllCommitsRequest"></a>

### FindAllCommitsRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  |  |
| revision | [bytes](#bytes) |  | When nil, return all commits reachable by any branch in the repo |
| max_count | [int32](#int32) |  |  |
| skip | [int32](#int32) |  |  |
| order | [FindAllCommitsRequest.Order](#gitaly.FindAllCommitsRequest.Order) |  |  |






<a name="gitaly.FindAllCommitsResponse"></a>

### FindAllCommitsResponse
A single &#39;page&#39; of the result set


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| commits | [GitCommit](#gitaly.GitCommit) | repeated |  |






<a name="gitaly.FindCommitRequest"></a>

### FindCommitRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  |  |
| revision | [bytes](#bytes) |  |  |






<a name="gitaly.FindCommitResponse"></a>

### FindCommitResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| commit | [GitCommit](#gitaly.GitCommit) |  | commit is nil when the commit was not found |






<a name="gitaly.FindCommitsRequest"></a>

### FindCommitsRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  |  |
| revision | [bytes](#bytes) |  |  |
| limit | [int32](#int32) |  |  |
| offset | [int32](#int32) |  |  |
| paths | [bytes](#bytes) | repeated |  |
| follow | [bool](#bool) |  |  |
| skip_merges | [bool](#bool) |  |  |
| disable_walk | [bool](#bool) |  |  |
| after | [google.protobuf.Timestamp](#google.protobuf.Timestamp) |  |  |
| before | [google.protobuf.Timestamp](#google.protobuf.Timestamp) |  |  |
| all | [bool](#bool) |  | all and revision are mutually exclusive |
| first_parent | [bool](#bool) |  |  |
| author | [bytes](#bytes) |  |  |
| order | [FindCommitsRequest.Order](#gitaly.FindCommitsRequest.Order) |  |  |






<a name="gitaly.FindCommitsResponse"></a>

### FindCommitsResponse
A single &#39;page&#39; of the result set


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| commits | [GitCommit](#gitaly.GitCommit) | repeated |  |






<a name="gitaly.GetCommitMessagesRequest"></a>

### GetCommitMessagesRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  |  |
| commit_ids | [string](#string) | repeated |  |






<a name="gitaly.GetCommitMessagesResponse"></a>

### GetCommitMessagesResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| commit_id | [string](#string) |  | Only present for a new commit message |
| message | [bytes](#bytes) |  |  |






<a name="gitaly.GetCommitSignaturesRequest"></a>

### GetCommitSignaturesRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  |  |
| commit_ids | [string](#string) | repeated |  |






<a name="gitaly.GetCommitSignaturesResponse"></a>

### GetCommitSignaturesResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| commit_id | [string](#string) |  | Only present for a new commit signature data. |
| signature | [bytes](#bytes) |  | See ExtractCommitSignatureResponse above for how these fields should be handled. |
| signed_text | [bytes](#bytes) |  |  |






<a name="gitaly.GetTreeEntriesRequest"></a>

### GetTreeEntriesRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  |  |
| revision | [bytes](#bytes) |  |  |
| path | [bytes](#bytes) |  |  |
| recursive | [bool](#bool) |  |  |






<a name="gitaly.GetTreeEntriesResponse"></a>

### GetTreeEntriesResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| entries | [TreeEntry](#gitaly.TreeEntry) | repeated |  |






<a name="gitaly.LastCommitForPathRequest"></a>

### LastCommitForPathRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  |  |
| revision | [bytes](#bytes) |  |  |
| path | [bytes](#bytes) |  |  |






<a name="gitaly.LastCommitForPathResponse"></a>

### LastCommitForPathResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| commit | [GitCommit](#gitaly.GitCommit) |  | commit is nil when the commit was not found |






<a name="gitaly.ListCommitsByOidRequest"></a>

### ListCommitsByOidRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  |  |
| oid | [string](#string) | repeated |  |






<a name="gitaly.ListCommitsByOidResponse"></a>

### ListCommitsByOidResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| commits | [GitCommit](#gitaly.GitCommit) | repeated |  |






<a name="gitaly.ListCommitsByRefNameRequest"></a>

### ListCommitsByRefNameRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  |  |
| ref_names | [bytes](#bytes) | repeated |  |






<a name="gitaly.ListCommitsByRefNameResponse"></a>

### ListCommitsByRefNameResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| commits | [GitCommit](#gitaly.GitCommit) | repeated |  |






<a name="gitaly.ListFilesRequest"></a>

### ListFilesRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  |  |
| revision | [bytes](#bytes) |  |  |






<a name="gitaly.ListFilesResponse"></a>

### ListFilesResponse
A single &#39;page&#39; of the paginated response


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| paths | [bytes](#bytes) | repeated | Remember to force encoding utf-8 on the client side |






<a name="gitaly.ListLastCommitsForTreeRequest"></a>

### ListLastCommitsForTreeRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  |  |
| revision | [string](#string) |  |  |
| path | [bytes](#bytes) |  |  |
| limit | [int32](#int32) |  | limit == -1 will get the last commit for all paths |
| offset | [int32](#int32) |  |  |






<a name="gitaly.ListLastCommitsForTreeResponse"></a>

### ListLastCommitsForTreeResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| commits | [ListLastCommitsForTreeResponse.CommitForTree](#gitaly.ListLastCommitsForTreeResponse.CommitForTree) | repeated |  |






<a name="gitaly.ListLastCommitsForTreeResponse.CommitForTree"></a>

### ListLastCommitsForTreeResponse.CommitForTree



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| commit | [GitCommit](#gitaly.GitCommit) |  |  |
| path_bytes | [bytes](#bytes) |  |  |






<a name="gitaly.RawBlameRequest"></a>

### RawBlameRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  |  |
| revision | [bytes](#bytes) |  |  |
| path | [bytes](#bytes) |  |  |






<a name="gitaly.RawBlameResponse"></a>

### RawBlameResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| data | [bytes](#bytes) |  |  |






<a name="gitaly.TreeEntry"></a>

### TreeEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| oid | [string](#string) |  | OID of the object this tree entry points to |
| root_oid | [string](#string) |  | OID of the tree attached to commit_oid |
| path | [bytes](#bytes) |  | Path relative to repository root |
| type | [TreeEntry.EntryType](#gitaly.TreeEntry.EntryType) |  |  |
| mode | [int32](#int32) |  | File mode e.g. 0644 |
| commit_oid | [string](#string) |  | The commit object via which this entry was retrieved |
| flat_path | [bytes](#bytes) |  | Relative path of the first subdir that doesn&#39;t have only one directory descendant |






<a name="gitaly.TreeEntryRequest"></a>

### TreeEntryRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  |  |
| revision | [bytes](#bytes) |  | commit ID or refname |
| path | [bytes](#bytes) |  | entry path relative to repository root |
| limit | [int64](#int64) |  |  |






<a name="gitaly.TreeEntryResponse"></a>

### TreeEntryResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| type | [TreeEntryResponse.ObjectType](#gitaly.TreeEntryResponse.ObjectType) |  |  |
| oid | [string](#string) |  | SHA1 object ID |
| size | [int64](#int64) |  |  |
| mode | [int32](#int32) |  | file mode |
| data | [bytes](#bytes) |  | raw object contents |





 


<a name="gitaly.FindAllCommitsRequest.Order"></a>

### FindAllCommitsRequest.Order


| Name | Number | Description |
| ---- | ------ | ----------- |
| NONE | 0 |  |
| TOPO | 1 |  |
| DATE | 2 |  |



<a name="gitaly.FindCommitsRequest.Order"></a>

### FindCommitsRequest.Order


| Name | Number | Description |
| ---- | ------ | ----------- |
| NONE | 0 |  |
| TOPO | 1 |  |



<a name="gitaly.TreeEntry.EntryType"></a>

### TreeEntry.EntryType
TODO: Replace this enum with ObjectType in shared.proto

| Name | Number | Description |
| ---- | ------ | ----------- |
| BLOB | 0 |  |
| TREE | 1 |  |
| COMMIT | 3 |  |



<a name="gitaly.TreeEntryResponse.ObjectType"></a>

### TreeEntryResponse.ObjectType
TODO: Replace this enum with ObjectType in shared.proto

| Name | Number | Description |
| ---- | ------ | ----------- |
| COMMIT | 0 |  |
| BLOB | 1 |  |
| TREE | 2 |  |
| TAG | 3 |  |


 

 


<a name="gitaly.CommitService"></a>

### CommitService


| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| CommitIsAncestor | [CommitIsAncestorRequest](#gitaly.CommitIsAncestorRequest) | [CommitIsAncestorResponse](#gitaly.CommitIsAncestorResponse) |  |
| TreeEntry | [TreeEntryRequest](#gitaly.TreeEntryRequest) | [TreeEntryResponse](#gitaly.TreeEntryResponse) stream |  |
| CommitsBetween | [CommitsBetweenRequest](#gitaly.CommitsBetweenRequest) | [CommitsBetweenResponse](#gitaly.CommitsBetweenResponse) stream |  |
| CountCommits | [CountCommitsRequest](#gitaly.CountCommitsRequest) | [CountCommitsResponse](#gitaly.CountCommitsResponse) |  |
| CountDivergingCommits | [CountDivergingCommitsRequest](#gitaly.CountDivergingCommitsRequest) | [CountDivergingCommitsResponse](#gitaly.CountDivergingCommitsResponse) |  |
| GetTreeEntries | [GetTreeEntriesRequest](#gitaly.GetTreeEntriesRequest) | [GetTreeEntriesResponse](#gitaly.GetTreeEntriesResponse) stream |  |
| ListFiles | [ListFilesRequest](#gitaly.ListFilesRequest) | [ListFilesResponse](#gitaly.ListFilesResponse) stream |  |
| FindCommit | [FindCommitRequest](#gitaly.FindCommitRequest) | [FindCommitResponse](#gitaly.FindCommitResponse) |  |
| CommitStats | [CommitStatsRequest](#gitaly.CommitStatsRequest) | [CommitStatsResponse](#gitaly.CommitStatsResponse) |  |
| FindAllCommits | [FindAllCommitsRequest](#gitaly.FindAllCommitsRequest) | [FindAllCommitsResponse](#gitaly.FindAllCommitsResponse) stream | Use a stream to paginate the result set |
| FindCommits | [FindCommitsRequest](#gitaly.FindCommitsRequest) | [FindCommitsResponse](#gitaly.FindCommitsResponse) stream |  |
| CommitLanguages | [CommitLanguagesRequest](#gitaly.CommitLanguagesRequest) | [CommitLanguagesResponse](#gitaly.CommitLanguagesResponse) |  |
| RawBlame | [RawBlameRequest](#gitaly.RawBlameRequest) | [RawBlameResponse](#gitaly.RawBlameResponse) stream |  |
| LastCommitForPath | [LastCommitForPathRequest](#gitaly.LastCommitForPathRequest) | [LastCommitForPathResponse](#gitaly.LastCommitForPathResponse) |  |
| ListLastCommitsForTree | [ListLastCommitsForTreeRequest](#gitaly.ListLastCommitsForTreeRequest) | [ListLastCommitsForTreeResponse](#gitaly.ListLastCommitsForTreeResponse) stream |  |
| CommitsByMessage | [CommitsByMessageRequest](#gitaly.CommitsByMessageRequest) | [CommitsByMessageResponse](#gitaly.CommitsByMessageResponse) stream |  |
| ListCommitsByOid | [ListCommitsByOidRequest](#gitaly.ListCommitsByOidRequest) | [ListCommitsByOidResponse](#gitaly.ListCommitsByOidResponse) stream |  |
| ListCommitsByRefName | [ListCommitsByRefNameRequest](#gitaly.ListCommitsByRefNameRequest) | [ListCommitsByRefNameResponse](#gitaly.ListCommitsByRefNameResponse) stream |  |
| FilterShasWithSignatures | [FilterShasWithSignaturesRequest](#gitaly.FilterShasWithSignaturesRequest) stream | [FilterShasWithSignaturesResponse](#gitaly.FilterShasWithSignaturesResponse) stream |  |
| GetCommitSignatures | [GetCommitSignaturesRequest](#gitaly.GetCommitSignaturesRequest) | [GetCommitSignaturesResponse](#gitaly.GetCommitSignaturesResponse) stream |  |
| GetCommitMessages | [GetCommitMessagesRequest](#gitaly.GetCommitMessagesRequest) | [GetCommitMessagesResponse](#gitaly.GetCommitMessagesResponse) stream |  |

 



<a name="conflicts.proto"></a>
<p align="right"><a href="#top">Top</a></p>

## conflicts.proto



<a name="gitaly.ConflictFile"></a>

### ConflictFile



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| header | [ConflictFileHeader](#gitaly.ConflictFileHeader) |  |  |
| content | [bytes](#bytes) |  |  |






<a name="gitaly.ConflictFileHeader"></a>

### ConflictFileHeader



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| commit_oid | [string](#string) |  |  |
| their_path | [bytes](#bytes) |  |  |
| our_path | [bytes](#bytes) |  |  |
| our_mode | [int32](#int32) |  |  |






<a name="gitaly.ListConflictFilesRequest"></a>

### ListConflictFilesRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  |  |
| our_commit_oid | [string](#string) |  |  |
| their_commit_oid | [string](#string) |  |  |






<a name="gitaly.ListConflictFilesResponse"></a>

### ListConflictFilesResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| files | [ConflictFile](#gitaly.ConflictFile) | repeated |  |






<a name="gitaly.ResolveConflictsRequest"></a>

### ResolveConflictsRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| header | [ResolveConflictsRequestHeader](#gitaly.ResolveConflictsRequestHeader) |  |  |
| files_json | [bytes](#bytes) |  |  |






<a name="gitaly.ResolveConflictsRequestHeader"></a>

### ResolveConflictsRequestHeader



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  |  |
| our_commit_oid | [string](#string) |  |  |
| target_repository | [Repository](#gitaly.Repository) |  |  |
| their_commit_oid | [string](#string) |  |  |
| source_branch | [bytes](#bytes) |  |  |
| target_branch | [bytes](#bytes) |  |  |
| commit_message | [bytes](#bytes) |  |  |
| user | [User](#gitaly.User) |  |  |






<a name="gitaly.ResolveConflictsResponse"></a>

### ResolveConflictsResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| resolution_error | [string](#string) |  |  |





 

 

 


<a name="gitaly.ConflictsService"></a>

### ConflictsService


| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| ListConflictFiles | [ListConflictFilesRequest](#gitaly.ListConflictFilesRequest) | [ListConflictFilesResponse](#gitaly.ListConflictFilesResponse) stream |  |
| ResolveConflicts | [ResolveConflictsRequest](#gitaly.ResolveConflictsRequest) stream | [ResolveConflictsResponse](#gitaly.ResolveConflictsResponse) |  |

 



<a name="diff.proto"></a>
<p align="right"><a href="#top">Top</a></p>

## diff.proto



<a name="gitaly.CommitDelta"></a>

### CommitDelta



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| from_path | [bytes](#bytes) |  |  |
| to_path | [bytes](#bytes) |  |  |
| from_id | [string](#string) |  | Blob ID as returned via `git diff --full-index` |
| to_id | [string](#string) |  |  |
| old_mode | [int32](#int32) |  |  |
| new_mode | [int32](#int32) |  |  |






<a name="gitaly.CommitDeltaRequest"></a>

### CommitDeltaRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  |  |
| left_commit_id | [string](#string) |  |  |
| right_commit_id | [string](#string) |  |  |
| paths | [bytes](#bytes) | repeated |  |






<a name="gitaly.CommitDeltaResponse"></a>

### CommitDeltaResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| deltas | [CommitDelta](#gitaly.CommitDelta) | repeated |  |






<a name="gitaly.CommitDiffRequest"></a>

### CommitDiffRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  |  |
| left_commit_id | [string](#string) |  |  |
| right_commit_id | [string](#string) |  |  |
| ignore_whitespace_change | [bool](#bool) |  |  |
| paths | [bytes](#bytes) | repeated |  |
| collapse_diffs | [bool](#bool) |  |  |
| enforce_limits | [bool](#bool) |  |  |
| max_files | [int32](#int32) |  | These limits are only enforced when enforce_limits == true. |
| max_lines | [int32](#int32) |  |  |
| max_bytes | [int32](#int32) |  |  |
| max_patch_bytes | [int32](#int32) |  | Limitation of a single diff patch, patches surpassing this limit are pruned by default. If this is 0 you will get back empty patches. |
| safe_max_files | [int32](#int32) |  | These limits are only enforced if collapse_diffs == true. |
| safe_max_lines | [int32](#int32) |  |  |
| safe_max_bytes | [int32](#int32) |  |  |






<a name="gitaly.CommitDiffResponse"></a>

### CommitDiffResponse
A CommitDiffResponse corresponds to a single changed file in a commit.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| from_path | [bytes](#bytes) |  |  |
| to_path | [bytes](#bytes) |  |  |
| from_id | [string](#string) |  | Blob ID as returned via `git diff --full-index` |
| to_id | [string](#string) |  |  |
| old_mode | [int32](#int32) |  |  |
| new_mode | [int32](#int32) |  |  |
| binary | [bool](#bool) |  |  |
| raw_patch_data | [bytes](#bytes) |  |  |
| end_of_patch | [bool](#bool) |  |  |
| overflow_marker | [bool](#bool) |  | Indicates the diff file at which we overflow according to the limitations sent, in which case only this attribute will be set. |
| collapsed | [bool](#bool) |  | Indicates the patch surpassed a &#34;safe&#34; limit and was therefore pruned, but the client may still request the full patch on a separate request. |
| too_large | [bool](#bool) |  | Indicates the patch was pruned since it surpassed a hard limit, and can therefore not be expanded. |






<a name="gitaly.DiffStats"></a>

### DiffStats



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| path | [bytes](#bytes) |  |  |
| additions | [int32](#int32) |  |  |
| deletions | [int32](#int32) |  |  |






<a name="gitaly.DiffStatsRequest"></a>

### DiffStatsRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  |  |
| left_commit_id | [string](#string) |  |  |
| right_commit_id | [string](#string) |  |  |






<a name="gitaly.DiffStatsResponse"></a>

### DiffStatsResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| stats | [DiffStats](#gitaly.DiffStats) | repeated |  |






<a name="gitaly.RawDiffRequest"></a>

### RawDiffRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  |  |
| left_commit_id | [string](#string) |  |  |
| right_commit_id | [string](#string) |  |  |






<a name="gitaly.RawDiffResponse"></a>

### RawDiffResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| data | [bytes](#bytes) |  |  |






<a name="gitaly.RawPatchRequest"></a>

### RawPatchRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  |  |
| left_commit_id | [string](#string) |  |  |
| right_commit_id | [string](#string) |  |  |






<a name="gitaly.RawPatchResponse"></a>

### RawPatchResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| data | [bytes](#bytes) |  |  |





 

 

 


<a name="gitaly.DiffService"></a>

### DiffService


| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| CommitDiff | [CommitDiffRequest](#gitaly.CommitDiffRequest) | [CommitDiffResponse](#gitaly.CommitDiffResponse) stream | Returns stream of CommitDiffResponse with patches chunked over messages |
| CommitDelta | [CommitDeltaRequest](#gitaly.CommitDeltaRequest) | [CommitDeltaResponse](#gitaly.CommitDeltaResponse) stream | Return a stream so we can divide the response in chunks of deltas |
| RawDiff | [RawDiffRequest](#gitaly.RawDiffRequest) | [RawDiffResponse](#gitaly.RawDiffResponse) stream |  |
| RawPatch | [RawPatchRequest](#gitaly.RawPatchRequest) | [RawPatchResponse](#gitaly.RawPatchResponse) stream |  |
| DiffStats | [DiffStatsRequest](#gitaly.DiffStatsRequest) | [DiffStatsResponse](#gitaly.DiffStatsResponse) stream |  |

 



<a name="hook.proto"></a>
<p align="right"><a href="#top">Top</a></p>

## hook.proto



<a name="gitaly.PostReceiveHookRequest"></a>

### PostReceiveHookRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  |  |
| key_id | [string](#string) |  |  |
| stdin | [bytes](#bytes) |  |  |
| git_push_options | [string](#string) | repeated |  |






<a name="gitaly.PostReceiveHookResponse"></a>

### PostReceiveHookResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| stdout | [bytes](#bytes) |  |  |
| stderr | [bytes](#bytes) |  |  |
| exit_status | [ExitStatus](#gitaly.ExitStatus) |  |  |






<a name="gitaly.PreReceiveHookRequest"></a>

### PreReceiveHookRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  |  |
| key_id | [string](#string) |  |  |
| protocol | [string](#string) |  |  |
| stdin | [bytes](#bytes) |  |  |






<a name="gitaly.PreReceiveHookResponse"></a>

### PreReceiveHookResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| stdout | [bytes](#bytes) |  |  |
| stderr | [bytes](#bytes) |  |  |
| exit_status | [ExitStatus](#gitaly.ExitStatus) |  |  |






<a name="gitaly.UpdateHookRequest"></a>

### UpdateHookRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  |  |
| key_id | [string](#string) |  |  |
| ref | [bytes](#bytes) |  |  |
| old_value | [string](#string) |  |  |
| new_value | [string](#string) |  |  |






<a name="gitaly.UpdateHookResponse"></a>

### UpdateHookResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| stdout | [bytes](#bytes) |  |  |
| stderr | [bytes](#bytes) |  |  |
| exit_status | [ExitStatus](#gitaly.ExitStatus) |  |  |





 

 

 


<a name="gitaly.HookService"></a>

### HookService


| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| PreReceiveHook | [PreReceiveHookRequest](#gitaly.PreReceiveHookRequest) stream | [PreReceiveHookResponse](#gitaly.PreReceiveHookResponse) stream |  |
| PostReceiveHook | [PostReceiveHookRequest](#gitaly.PostReceiveHookRequest) stream | [PostReceiveHookResponse](#gitaly.PostReceiveHookResponse) stream |  |
| UpdateHook | [UpdateHookRequest](#gitaly.UpdateHookRequest) | [UpdateHookResponse](#gitaly.UpdateHookResponse) stream |  |

 



<a name="internal.proto"></a>
<p align="right"><a href="#top">Top</a></p>

## internal.proto



<a name="gitaly.WalkReposRequest"></a>

### WalkReposRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| storage_name | [string](#string) |  |  |






<a name="gitaly.WalkReposResponse"></a>

### WalkReposResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| relative_path | [string](#string) |  |  |





 

 

 


<a name="gitaly.InternalGitaly"></a>

### InternalGitaly
InternalGitaly is a gRPC service meant to be served by a Gitaly node, but
only reachable by Praefect or other Gitalies

| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| WalkRepos | [WalkReposRequest](#gitaly.WalkReposRequest) | [WalkReposResponse](#gitaly.WalkReposResponse) stream | WalkRepos walks the storage and streams back all known git repos on the requested storage |

 



<a name="lint.proto"></a>
<p align="right"><a href="#top">Top</a></p>

## lint.proto



<a name="gitaly.OperationMsg"></a>

### OperationMsg



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| op | [OperationMsg.Operation](#gitaly.OperationMsg.Operation) |  |  |
| scope_level | [OperationMsg.Scope](#gitaly.OperationMsg.Scope) |  | Scope level indicates what level an RPC interacts with a server: - REPOSITORY: scoped to only a single repo - SERVER: affects the entire server and potentially all repos - STORAGE: scoped to a specific storage location and all repos within |





 


<a name="gitaly.OperationMsg.Operation"></a>

### OperationMsg.Operation


| Name | Number | Description |
| ---- | ------ | ----------- |
| UNKNOWN | 0 |  |
| MUTATOR | 1 |  |
| ACCESSOR | 2 |  |



<a name="gitaly.OperationMsg.Scope"></a>

### OperationMsg.Scope


| Name | Number | Description |
| ---- | ------ | ----------- |
| REPOSITORY | 0 |  |
| SERVER | 1 |  |
| STORAGE | 2 |  |


 


<a name="lint.proto-extensions"></a>

### File-level Extensions
| Extension | Type | Base | Number | Description |
| --------- | ---- | ---- | ------ | ----------- |
| additional_repository | bool | .google.protobuf.FieldOptions | 91236 | Used to mark additional repository |
| repository | bool | .google.protobuf.FieldOptions | 91234 | If this operation modifies a repository, this annotations will specify the location of the Repository field within the request message.

Repository annotation is used mark field used as repository when parent message is marked as target or additional repository |
| storage | bool | .google.protobuf.FieldOptions | 91233 | Used to mark field containing name of affected storage.

Random high number.. |
| target_repository | bool | .google.protobuf.FieldOptions | 91235 | Used to mark target repository |
| op_type | OperationMsg | .google.protobuf.MethodOptions | 82303 | Random high number.. |

 

 



<a name="namespace.proto"></a>
<p align="right"><a href="#top">Top</a></p>

## namespace.proto



<a name="gitaly.AddNamespaceRequest"></a>

### AddNamespaceRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| storage_name | [string](#string) |  |  |
| name | [string](#string) |  |  |






<a name="gitaly.AddNamespaceResponse"></a>

### AddNamespaceResponse







<a name="gitaly.NamespaceExistsRequest"></a>

### NamespaceExistsRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| storage_name | [string](#string) |  |  |
| name | [string](#string) |  |  |






<a name="gitaly.NamespaceExistsResponse"></a>

### NamespaceExistsResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| exists | [bool](#bool) |  |  |






<a name="gitaly.RemoveNamespaceRequest"></a>

### RemoveNamespaceRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| storage_name | [string](#string) |  |  |
| name | [string](#string) |  |  |






<a name="gitaly.RemoveNamespaceResponse"></a>

### RemoveNamespaceResponse







<a name="gitaly.RenameNamespaceRequest"></a>

### RenameNamespaceRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| storage_name | [string](#string) |  |  |
| from | [string](#string) |  |  |
| to | [string](#string) |  |  |






<a name="gitaly.RenameNamespaceResponse"></a>

### RenameNamespaceResponse






 

 

 


<a name="gitaly.NamespaceService"></a>

### NamespaceService


| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| AddNamespace | [AddNamespaceRequest](#gitaly.AddNamespaceRequest) | [AddNamespaceResponse](#gitaly.AddNamespaceResponse) |  |
| RemoveNamespace | [RemoveNamespaceRequest](#gitaly.RemoveNamespaceRequest) | [RemoveNamespaceResponse](#gitaly.RemoveNamespaceResponse) |  |
| RenameNamespace | [RenameNamespaceRequest](#gitaly.RenameNamespaceRequest) | [RenameNamespaceResponse](#gitaly.RenameNamespaceResponse) |  |
| NamespaceExists | [NamespaceExistsRequest](#gitaly.NamespaceExistsRequest) | [NamespaceExistsResponse](#gitaly.NamespaceExistsResponse) |  |

 



<a name="objectpool.proto"></a>
<p align="right"><a href="#top">Top</a></p>

## objectpool.proto



<a name="gitaly.CreateObjectPoolRequest"></a>

### CreateObjectPoolRequest
Creates an object pool from the repository. The client is responsible for
joining this pool later with this repository.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| object_pool | [ObjectPool](#gitaly.ObjectPool) |  |  |
| origin | [Repository](#gitaly.Repository) |  |  |






<a name="gitaly.CreateObjectPoolResponse"></a>

### CreateObjectPoolResponse







<a name="gitaly.DeleteObjectPoolRequest"></a>

### DeleteObjectPoolRequest
Removes the directory from disk, caller is responsible for leaving the object
pool before calling this RPC


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| object_pool | [ObjectPool](#gitaly.ObjectPool) |  |  |






<a name="gitaly.DeleteObjectPoolResponse"></a>

### DeleteObjectPoolResponse







<a name="gitaly.DisconnectGitAlternatesRequest"></a>

### DisconnectGitAlternatesRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  |  |






<a name="gitaly.DisconnectGitAlternatesResponse"></a>

### DisconnectGitAlternatesResponse







<a name="gitaly.FetchIntoObjectPoolRequest"></a>

### FetchIntoObjectPoolRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| origin | [Repository](#gitaly.Repository) |  |  |
| object_pool | [ObjectPool](#gitaly.ObjectPool) |  |  |
| repack | [bool](#bool) |  |  |






<a name="gitaly.FetchIntoObjectPoolResponse"></a>

### FetchIntoObjectPoolResponse







<a name="gitaly.GetObjectPoolRequest"></a>

### GetObjectPoolRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  |  |






<a name="gitaly.GetObjectPoolResponse"></a>

### GetObjectPoolResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| object_pool | [ObjectPool](#gitaly.ObjectPool) |  |  |






<a name="gitaly.LinkRepositoryToObjectPoolRequest"></a>

### LinkRepositoryToObjectPoolRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| object_pool | [ObjectPool](#gitaly.ObjectPool) |  |  |
| repository | [Repository](#gitaly.Repository) |  |  |






<a name="gitaly.LinkRepositoryToObjectPoolResponse"></a>

### LinkRepositoryToObjectPoolResponse







<a name="gitaly.ReduplicateRepositoryRequest"></a>

### ReduplicateRepositoryRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  |  |






<a name="gitaly.ReduplicateRepositoryResponse"></a>

### ReduplicateRepositoryResponse







<a name="gitaly.UnlinkRepositoryFromObjectPoolRequest"></a>

### UnlinkRepositoryFromObjectPoolRequest
This RPC doesn&#39;t require the ObjectPool as it will remove the alternates file
from the pool participant. The caller is responsible no data loss occurs.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  | already specified as the target repo field |
| object_pool | [ObjectPool](#gitaly.ObjectPool) |  |  |






<a name="gitaly.UnlinkRepositoryFromObjectPoolResponse"></a>

### UnlinkRepositoryFromObjectPoolResponse






 

 

 


<a name="gitaly.ObjectPoolService"></a>

### ObjectPoolService


| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| CreateObjectPool | [CreateObjectPoolRequest](#gitaly.CreateObjectPoolRequest) | [CreateObjectPoolResponse](#gitaly.CreateObjectPoolResponse) |  |
| DeleteObjectPool | [DeleteObjectPoolRequest](#gitaly.DeleteObjectPoolRequest) | [DeleteObjectPoolResponse](#gitaly.DeleteObjectPoolResponse) |  |
| LinkRepositoryToObjectPool | [LinkRepositoryToObjectPoolRequest](#gitaly.LinkRepositoryToObjectPoolRequest) | [LinkRepositoryToObjectPoolResponse](#gitaly.LinkRepositoryToObjectPoolResponse) | Repositories are assumed to be stored on the same disk |
| UnlinkRepositoryFromObjectPool | [UnlinkRepositoryFromObjectPoolRequest](#gitaly.UnlinkRepositoryFromObjectPoolRequest) | [UnlinkRepositoryFromObjectPoolResponse](#gitaly.UnlinkRepositoryFromObjectPoolResponse) |  |
| ReduplicateRepository | [ReduplicateRepositoryRequest](#gitaly.ReduplicateRepositoryRequest) | [ReduplicateRepositoryResponse](#gitaly.ReduplicateRepositoryResponse) |  |
| DisconnectGitAlternates | [DisconnectGitAlternatesRequest](#gitaly.DisconnectGitAlternatesRequest) | [DisconnectGitAlternatesResponse](#gitaly.DisconnectGitAlternatesResponse) |  |
| FetchIntoObjectPool | [FetchIntoObjectPoolRequest](#gitaly.FetchIntoObjectPoolRequest) | [FetchIntoObjectPoolResponse](#gitaly.FetchIntoObjectPoolResponse) |  |
| GetObjectPool | [GetObjectPoolRequest](#gitaly.GetObjectPoolRequest) | [GetObjectPoolResponse](#gitaly.GetObjectPoolResponse) |  |

 



<a name="operations.proto"></a>
<p align="right"><a href="#top">Top</a></p>

## operations.proto



<a name="gitaly.OperationBranchUpdate"></a>

### OperationBranchUpdate



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| commit_id | [string](#string) |  | If this string is non-empty the branch has been updated. |
| repo_created | [bool](#bool) |  | Used for cache invalidation in GitLab |
| branch_created | [bool](#bool) |  | Used for cache invalidation in GitLab |






<a name="gitaly.UserApplyPatchRequest"></a>

### UserApplyPatchRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| header | [UserApplyPatchRequest.Header](#gitaly.UserApplyPatchRequest.Header) |  |  |
| patches | [bytes](#bytes) |  |  |






<a name="gitaly.UserApplyPatchRequest.Header"></a>

### UserApplyPatchRequest.Header



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  |  |
| user | [User](#gitaly.User) |  |  |
| target_branch | [bytes](#bytes) |  |  |






<a name="gitaly.UserApplyPatchResponse"></a>

### UserApplyPatchResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| branch_update | [OperationBranchUpdate](#gitaly.OperationBranchUpdate) |  |  |






<a name="gitaly.UserCherryPickRequest"></a>

### UserCherryPickRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  |  |
| user | [User](#gitaly.User) |  |  |
| commit | [GitCommit](#gitaly.GitCommit) |  |  |
| branch_name | [bytes](#bytes) |  |  |
| message | [bytes](#bytes) |  |  |
| start_branch_name | [bytes](#bytes) |  |  |
| start_repository | [Repository](#gitaly.Repository) |  |  |






<a name="gitaly.UserCherryPickResponse"></a>

### UserCherryPickResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| branch_update | [OperationBranchUpdate](#gitaly.OperationBranchUpdate) |  |  |
| create_tree_error | [string](#string) |  |  |
| commit_error | [string](#string) |  |  |
| pre_receive_error | [string](#string) |  |  |
| create_tree_error_code | [UserCherryPickResponse.CreateTreeError](#gitaly.UserCherryPickResponse.CreateTreeError) |  |  |






<a name="gitaly.UserCommitFilesAction"></a>

### UserCommitFilesAction



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| header | [UserCommitFilesActionHeader](#gitaly.UserCommitFilesActionHeader) |  |  |
| content | [bytes](#bytes) |  |  |






<a name="gitaly.UserCommitFilesActionHeader"></a>

### UserCommitFilesActionHeader



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| action | [UserCommitFilesActionHeader.ActionType](#gitaly.UserCommitFilesActionHeader.ActionType) |  |  |
| file_path | [bytes](#bytes) |  |  |
| previous_path | [bytes](#bytes) |  |  |
| base64_content | [bool](#bool) |  |  |
| execute_filemode | [bool](#bool) |  |  |
| infer_content | [bool](#bool) |  | Move actions that change the file path, but not its content, should set infer_content to true instead of populating the content field. Ignored for other action types. |






<a name="gitaly.UserCommitFilesRequest"></a>

### UserCommitFilesRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| header | [UserCommitFilesRequestHeader](#gitaly.UserCommitFilesRequestHeader) |  | For each request stream there should be first a request with a header and then n requests with actions |
| action | [UserCommitFilesAction](#gitaly.UserCommitFilesAction) |  |  |






<a name="gitaly.UserCommitFilesRequestHeader"></a>

### UserCommitFilesRequestHeader



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  |  |
| user | [User](#gitaly.User) |  |  |
| branch_name | [bytes](#bytes) |  |  |
| commit_message | [bytes](#bytes) |  |  |
| commit_author_name | [bytes](#bytes) |  |  |
| commit_author_email | [bytes](#bytes) |  |  |
| start_branch_name | [bytes](#bytes) |  |  |
| start_repository | [Repository](#gitaly.Repository) |  |  |
| force | [bool](#bool) |  |  |
| start_sha | [string](#string) |  |  |






<a name="gitaly.UserCommitFilesResponse"></a>

### UserCommitFilesResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| branch_update | [OperationBranchUpdate](#gitaly.OperationBranchUpdate) |  |  |
| index_error | [string](#string) |  |  |
| pre_receive_error | [string](#string) |  |  |






<a name="gitaly.UserCreateBranchRequest"></a>

### UserCreateBranchRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  |  |
| branch_name | [bytes](#bytes) |  |  |
| user | [User](#gitaly.User) |  |  |
| start_point | [bytes](#bytes) |  |  |






<a name="gitaly.UserCreateBranchResponse"></a>

### UserCreateBranchResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| branch | [Branch](#gitaly.Branch) |  |  |
| pre_receive_error | [string](#string) |  | Error returned by the pre-receive hook. If no error was thrown, it&#39;s the empty string (&#34;&#34;) |






<a name="gitaly.UserCreateTagRequest"></a>

### UserCreateTagRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  |  |
| tag_name | [bytes](#bytes) |  |  |
| user | [User](#gitaly.User) |  |  |
| target_revision | [bytes](#bytes) |  |  |
| message | [bytes](#bytes) |  |  |






<a name="gitaly.UserCreateTagResponse"></a>

### UserCreateTagResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| tag | [Tag](#gitaly.Tag) |  |  |
| exists | [bool](#bool) |  |  |
| pre_receive_error | [string](#string) |  |  |






<a name="gitaly.UserDeleteBranchRequest"></a>

### UserDeleteBranchRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  |  |
| branch_name | [bytes](#bytes) |  |  |
| user | [User](#gitaly.User) |  |  |






<a name="gitaly.UserDeleteBranchResponse"></a>

### UserDeleteBranchResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| pre_receive_error | [string](#string) |  |  |






<a name="gitaly.UserDeleteTagRequest"></a>

### UserDeleteTagRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  |  |
| tag_name | [bytes](#bytes) |  |  |
| user | [User](#gitaly.User) |  |  |






<a name="gitaly.UserDeleteTagResponse"></a>

### UserDeleteTagResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| pre_receive_error | [string](#string) |  |  |






<a name="gitaly.UserFFBranchRequest"></a>

### UserFFBranchRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  |  |
| user | [User](#gitaly.User) |  |  |
| commit_id | [string](#string) |  |  |
| branch | [bytes](#bytes) |  |  |






<a name="gitaly.UserFFBranchResponse"></a>

### UserFFBranchResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| branch_update | [OperationBranchUpdate](#gitaly.OperationBranchUpdate) |  |  |
| pre_receive_error | [string](#string) |  |  |






<a name="gitaly.UserMergeBranchRequest"></a>

### UserMergeBranchRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  | First message |
| user | [User](#gitaly.User) |  |  |
| commit_id | [string](#string) |  |  |
| branch | [bytes](#bytes) |  |  |
| message | [bytes](#bytes) |  |  |
| apply | [bool](#bool) |  | Second message Tell the server to apply the merge to the branch |






<a name="gitaly.UserMergeBranchResponse"></a>

### UserMergeBranchResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| commit_id | [string](#string) |  | First message The merge commit the branch will be updated to. The caller can still abort the merge. |
| branch_update | [OperationBranchUpdate](#gitaly.OperationBranchUpdate) |  | Second message If set, the merge has been applied to the branch. |
| pre_receive_error | [string](#string) |  |  |






<a name="gitaly.UserMergeToRefRequest"></a>

### UserMergeToRefRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  | UserMergeRef creates a merge commit and updates target_ref to point to that new commit. The first parent of the merge commit (the main line) is taken from first_parent_ref. The second parent is specified by its commit ID in source_sha. If target_ref already exists it will be overwritten. |
| user | [User](#gitaly.User) |  |  |
| source_sha | [string](#string) |  |  |
| branch | [bytes](#bytes) |  | branch is deprecated in favor of `first_parent_ref`. |
| target_ref | [bytes](#bytes) |  |  |
| message | [bytes](#bytes) |  |  |
| first_parent_ref | [bytes](#bytes) |  |  |






<a name="gitaly.UserMergeToRefResponse"></a>

### UserMergeToRefResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| commit_id | [string](#string) |  |  |
| pre_receive_error | [string](#string) |  |  |






<a name="gitaly.UserRebaseConfirmableRequest"></a>

### UserRebaseConfirmableRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| header | [UserRebaseConfirmableRequest.Header](#gitaly.UserRebaseConfirmableRequest.Header) |  | For each request stream there must be first a request with a header containing details about the rebase to perform. |
| apply | [bool](#bool) |  | A second request must be made to confirm that the rebase should be applied to the branch. |






<a name="gitaly.UserRebaseConfirmableRequest.Header"></a>

### UserRebaseConfirmableRequest.Header



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  |  |
| user | [User](#gitaly.User) |  |  |
| rebase_id | [string](#string) |  |  |
| branch | [bytes](#bytes) |  |  |
| branch_sha | [string](#string) |  |  |
| remote_repository | [Repository](#gitaly.Repository) |  |  |
| remote_branch | [bytes](#bytes) |  |  |
| git_push_options | [string](#string) | repeated |  |






<a name="gitaly.UserRebaseConfirmableResponse"></a>

### UserRebaseConfirmableResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| rebase_sha | [string](#string) |  | The first response will contain the rebase commit the branch will be updated to. The caller can still abort the rebase. |
| rebase_applied | [bool](#bool) |  | The second response confirms that the rebase has been applied to the branch. |
| pre_receive_error | [string](#string) |  |  |
| git_error | [string](#string) |  |  |






<a name="gitaly.UserRevertRequest"></a>

### UserRevertRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  |  |
| user | [User](#gitaly.User) |  |  |
| commit | [GitCommit](#gitaly.GitCommit) |  |  |
| branch_name | [bytes](#bytes) |  |  |
| message | [bytes](#bytes) |  |  |
| start_branch_name | [bytes](#bytes) |  |  |
| start_repository | [Repository](#gitaly.Repository) |  |  |






<a name="gitaly.UserRevertResponse"></a>

### UserRevertResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| branch_update | [OperationBranchUpdate](#gitaly.OperationBranchUpdate) |  |  |
| create_tree_error | [string](#string) |  |  |
| commit_error | [string](#string) |  |  |
| pre_receive_error | [string](#string) |  |  |
| create_tree_error_code | [UserRevertResponse.CreateTreeError](#gitaly.UserRevertResponse.CreateTreeError) |  |  |






<a name="gitaly.UserSquashRequest"></a>

### UserSquashRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  |  |
| user | [User](#gitaly.User) |  |  |
| squash_id | [string](#string) |  |  |
| start_sha | [string](#string) |  |  |
| end_sha | [string](#string) |  |  |
| author | [User](#gitaly.User) |  |  |
| commit_message | [bytes](#bytes) |  |  |






<a name="gitaly.UserSquashResponse"></a>

### UserSquashResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| squash_sha | [string](#string) |  |  |
| git_error | [string](#string) |  |  |






<a name="gitaly.UserUpdateBranchRequest"></a>

### UserUpdateBranchRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  |  |
| branch_name | [bytes](#bytes) |  |  |
| user | [User](#gitaly.User) |  |  |
| newrev | [bytes](#bytes) |  |  |
| oldrev | [bytes](#bytes) |  |  |






<a name="gitaly.UserUpdateBranchResponse"></a>

### UserUpdateBranchResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| pre_receive_error | [string](#string) |  |  |






<a name="gitaly.UserUpdateSubmoduleRequest"></a>

### UserUpdateSubmoduleRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  |  |
| user | [User](#gitaly.User) |  |  |
| commit_sha | [string](#string) |  |  |
| branch | [bytes](#bytes) |  |  |
| submodule | [bytes](#bytes) |  |  |
| commit_message | [bytes](#bytes) |  |  |






<a name="gitaly.UserUpdateSubmoduleResponse"></a>

### UserUpdateSubmoduleResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| branch_update | [OperationBranchUpdate](#gitaly.OperationBranchUpdate) |  |  |
| pre_receive_error | [string](#string) |  |  |
| commit_error | [string](#string) |  |  |





 


<a name="gitaly.UserCherryPickResponse.CreateTreeError"></a>

### UserCherryPickResponse.CreateTreeError


| Name | Number | Description |
| ---- | ------ | ----------- |
| NONE | 0 |  |
| EMPTY | 1 |  |
| CONFLICT | 2 |  |



<a name="gitaly.UserCommitFilesActionHeader.ActionType"></a>

### UserCommitFilesActionHeader.ActionType


| Name | Number | Description |
| ---- | ------ | ----------- |
| CREATE | 0 |  |
| CREATE_DIR | 1 |  |
| UPDATE | 2 |  |
| MOVE | 3 |  |
| DELETE | 4 |  |
| CHMOD | 5 |  |



<a name="gitaly.UserRevertResponse.CreateTreeError"></a>

### UserRevertResponse.CreateTreeError


| Name | Number | Description |
| ---- | ------ | ----------- |
| NONE | 0 |  |
| EMPTY | 1 |  |
| CONFLICT | 2 |  |


 

 


<a name="gitaly.OperationService"></a>

### OperationService


| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| UserCreateBranch | [UserCreateBranchRequest](#gitaly.UserCreateBranchRequest) | [UserCreateBranchResponse](#gitaly.UserCreateBranchResponse) |  |
| UserUpdateBranch | [UserUpdateBranchRequest](#gitaly.UserUpdateBranchRequest) | [UserUpdateBranchResponse](#gitaly.UserUpdateBranchResponse) |  |
| UserDeleteBranch | [UserDeleteBranchRequest](#gitaly.UserDeleteBranchRequest) | [UserDeleteBranchResponse](#gitaly.UserDeleteBranchResponse) |  |
| UserCreateTag | [UserCreateTagRequest](#gitaly.UserCreateTagRequest) | [UserCreateTagResponse](#gitaly.UserCreateTagResponse) |  |
| UserDeleteTag | [UserDeleteTagRequest](#gitaly.UserDeleteTagRequest) | [UserDeleteTagResponse](#gitaly.UserDeleteTagResponse) |  |
| UserMergeToRef | [UserMergeToRefRequest](#gitaly.UserMergeToRefRequest) | [UserMergeToRefResponse](#gitaly.UserMergeToRefResponse) |  |
| UserMergeBranch | [UserMergeBranchRequest](#gitaly.UserMergeBranchRequest) stream | [UserMergeBranchResponse](#gitaly.UserMergeBranchResponse) stream |  |
| UserFFBranch | [UserFFBranchRequest](#gitaly.UserFFBranchRequest) | [UserFFBranchResponse](#gitaly.UserFFBranchResponse) |  |
| UserCherryPick | [UserCherryPickRequest](#gitaly.UserCherryPickRequest) | [UserCherryPickResponse](#gitaly.UserCherryPickResponse) |  |
| UserCommitFiles | [UserCommitFilesRequest](#gitaly.UserCommitFilesRequest) stream | [UserCommitFilesResponse](#gitaly.UserCommitFilesResponse) |  |
| UserRebaseConfirmable | [UserRebaseConfirmableRequest](#gitaly.UserRebaseConfirmableRequest) stream | [UserRebaseConfirmableResponse](#gitaly.UserRebaseConfirmableResponse) stream |  |
| UserRevert | [UserRevertRequest](#gitaly.UserRevertRequest) | [UserRevertResponse](#gitaly.UserRevertResponse) |  |
| UserSquash | [UserSquashRequest](#gitaly.UserSquashRequest) | [UserSquashResponse](#gitaly.UserSquashResponse) |  |
| UserApplyPatch | [UserApplyPatchRequest](#gitaly.UserApplyPatchRequest) stream | [UserApplyPatchResponse](#gitaly.UserApplyPatchResponse) |  |
| UserUpdateSubmodule | [UserUpdateSubmoduleRequest](#gitaly.UserUpdateSubmoduleRequest) | [UserUpdateSubmoduleResponse](#gitaly.UserUpdateSubmoduleResponse) |  |

 



<a name="praefect.proto"></a>
<p align="right"><a href="#top">Top</a></p>

## praefect.proto



<a name="gitaly.RepositoryReplicasRequest"></a>

### RepositoryReplicasRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  |  |






<a name="gitaly.RepositoryReplicasResponse"></a>

### RepositoryReplicasResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| primary | [RepositoryReplicasResponse.RepositoryDetails](#gitaly.RepositoryReplicasResponse.RepositoryDetails) |  |  |
| replicas | [RepositoryReplicasResponse.RepositoryDetails](#gitaly.RepositoryReplicasResponse.RepositoryDetails) | repeated |  |






<a name="gitaly.RepositoryReplicasResponse.RepositoryDetails"></a>

### RepositoryReplicasResponse.RepositoryDetails



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  |  |
| checksum | [string](#string) |  |  |





 

 

 


<a name="gitaly.PraefectInfoService"></a>

### PraefectInfoService


| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| RepositoryReplicas | [RepositoryReplicasRequest](#gitaly.RepositoryReplicasRequest) | [RepositoryReplicasResponse](#gitaly.RepositoryReplicasResponse) |  |

 



<a name="ref.proto"></a>
<p align="right"><a href="#top">Top</a></p>

## ref.proto



<a name="gitaly.CreateBranchRequest"></a>

### CreateBranchRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  |  |
| name | [bytes](#bytes) |  |  |
| start_point | [bytes](#bytes) |  |  |






<a name="gitaly.CreateBranchResponse"></a>

### CreateBranchResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| status | [CreateBranchResponse.Status](#gitaly.CreateBranchResponse.Status) |  |  |
| branch | [Branch](#gitaly.Branch) |  |  |






<a name="gitaly.DeleteBranchRequest"></a>

### DeleteBranchRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  |  |
| name | [bytes](#bytes) |  |  |






<a name="gitaly.DeleteBranchResponse"></a>

### DeleteBranchResponse
Not clear if we need to do status signaling; we can add fields later.






<a name="gitaly.DeleteRefsRequest"></a>

### DeleteRefsRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  |  |
| except_with_prefix | [bytes](#bytes) | repeated | The following two fields are mutually exclusive |
| refs | [bytes](#bytes) | repeated |  |






<a name="gitaly.DeleteRefsResponse"></a>

### DeleteRefsResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| git_error | [string](#string) |  |  |






<a name="gitaly.FindAllBranchNamesRequest"></a>

### FindAllBranchNamesRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  |  |






<a name="gitaly.FindAllBranchNamesResponse"></a>

### FindAllBranchNamesResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| names | [bytes](#bytes) | repeated |  |






<a name="gitaly.FindAllBranchesRequest"></a>

### FindAllBranchesRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  |  |
| merged_only | [bool](#bool) |  | Only return branches that are merged into root ref |
| merged_branches | [bytes](#bytes) | repeated | If merged_only is true, this is a list of branches from which we return those merged into the root ref |






<a name="gitaly.FindAllBranchesResponse"></a>

### FindAllBranchesResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| branches | [FindAllBranchesResponse.Branch](#gitaly.FindAllBranchesResponse.Branch) | repeated |  |






<a name="gitaly.FindAllBranchesResponse.Branch"></a>

### FindAllBranchesResponse.Branch



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [bytes](#bytes) |  |  |
| target | [GitCommit](#gitaly.GitCommit) |  |  |






<a name="gitaly.FindAllRemoteBranchesRequest"></a>

### FindAllRemoteBranchesRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  |  |
| remote_name | [string](#string) |  |  |






<a name="gitaly.FindAllRemoteBranchesResponse"></a>

### FindAllRemoteBranchesResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| branches | [Branch](#gitaly.Branch) | repeated |  |






<a name="gitaly.FindAllTagNamesRequest"></a>

### FindAllTagNamesRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  |  |






<a name="gitaly.FindAllTagNamesResponse"></a>

### FindAllTagNamesResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| names | [bytes](#bytes) | repeated |  |






<a name="gitaly.FindAllTagsRequest"></a>

### FindAllTagsRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  |  |






<a name="gitaly.FindAllTagsResponse"></a>

### FindAllTagsResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| tags | [Tag](#gitaly.Tag) | repeated |  |






<a name="gitaly.FindBranchRequest"></a>

### FindBranchRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  |  |
| name | [bytes](#bytes) |  | Name can be &#39;master&#39; but also &#39;refs/heads/master&#39; |






<a name="gitaly.FindBranchResponse"></a>

### FindBranchResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| branch | [Branch](#gitaly.Branch) |  |  |






<a name="gitaly.FindDefaultBranchNameRequest"></a>

### FindDefaultBranchNameRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  |  |






<a name="gitaly.FindDefaultBranchNameResponse"></a>

### FindDefaultBranchNameResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [bytes](#bytes) |  |  |






<a name="gitaly.FindLocalBranchCommitAuthor"></a>

### FindLocalBranchCommitAuthor



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [bytes](#bytes) |  |  |
| email | [bytes](#bytes) |  |  |
| date | [google.protobuf.Timestamp](#google.protobuf.Timestamp) |  |  |
| timezone | [bytes](#bytes) |  |  |






<a name="gitaly.FindLocalBranchResponse"></a>

### FindLocalBranchResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [bytes](#bytes) |  |  |
| commit_id | [string](#string) |  |  |
| commit_subject | [bytes](#bytes) |  |  |
| commit_author | [FindLocalBranchCommitAuthor](#gitaly.FindLocalBranchCommitAuthor) |  |  |
| commit_committer | [FindLocalBranchCommitAuthor](#gitaly.FindLocalBranchCommitAuthor) |  |  |
| commit | [GitCommit](#gitaly.GitCommit) |  |  |






<a name="gitaly.FindLocalBranchesRequest"></a>

### FindLocalBranchesRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  |  |
| sort_by | [FindLocalBranchesRequest.SortBy](#gitaly.FindLocalBranchesRequest.SortBy) |  |  |






<a name="gitaly.FindLocalBranchesResponse"></a>

### FindLocalBranchesResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| branches | [FindLocalBranchResponse](#gitaly.FindLocalBranchResponse) | repeated |  |






<a name="gitaly.FindRefNameRequest"></a>

### FindRefNameRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  |  |
| commit_id | [string](#string) |  | Require that the resulting ref contains this commit as an ancestor |
| prefix | [bytes](#bytes) |  | Example prefix: &#34;refs/heads/&#34;. Type bytes because that is the type of ref names. |






<a name="gitaly.FindRefNameResponse"></a>

### FindRefNameResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [bytes](#bytes) |  | Example name: &#34;refs/heads/master&#34;. Cannot assume UTF8, so the type is bytes. |






<a name="gitaly.FindTagRequest"></a>

### FindTagRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  |  |
| tag_name | [bytes](#bytes) |  |  |






<a name="gitaly.FindTagResponse"></a>

### FindTagResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| tag | [Tag](#gitaly.Tag) |  |  |






<a name="gitaly.GetTagMessagesRequest"></a>

### GetTagMessagesRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  |  |
| tag_ids | [string](#string) | repeated |  |






<a name="gitaly.GetTagMessagesResponse"></a>

### GetTagMessagesResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| message | [bytes](#bytes) |  |  |
| tag_id | [string](#string) |  | Only present for a new tag message |






<a name="gitaly.ListBranchNamesContainingCommitRequest"></a>

### ListBranchNamesContainingCommitRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  |  |
| commit_id | [string](#string) |  |  |
| limit | [uint32](#uint32) |  | Limit the number of tag names to be returned If the limit is set to zero, all items will be returned |






<a name="gitaly.ListBranchNamesContainingCommitResponse"></a>

### ListBranchNamesContainingCommitResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| branch_names | [bytes](#bytes) | repeated |  |






<a name="gitaly.ListNewBlobsRequest"></a>

### ListNewBlobsRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  |  |
| commit_id | [string](#string) |  |  |
| limit | [uint32](#uint32) |  | Limit the number of revs to be returned fro mgit-rev-list If the limit is set to zero, all items will be returned |






<a name="gitaly.ListNewBlobsResponse"></a>

### ListNewBlobsResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| new_blob_objects | [NewBlobObject](#gitaly.NewBlobObject) | repeated |  |






<a name="gitaly.ListNewCommitsRequest"></a>

### ListNewCommitsRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  |  |
| commit_id | [string](#string) |  |  |






<a name="gitaly.ListNewCommitsResponse"></a>

### ListNewCommitsResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| commits | [GitCommit](#gitaly.GitCommit) | repeated |  |






<a name="gitaly.ListTagNamesContainingCommitRequest"></a>

### ListTagNamesContainingCommitRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  |  |
| commit_id | [string](#string) |  |  |
| limit | [uint32](#uint32) |  | Limit the number of tag names to be returned If the limit is set to zero, all items will be returned |






<a name="gitaly.ListTagNamesContainingCommitResponse"></a>

### ListTagNamesContainingCommitResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| tag_names | [bytes](#bytes) | repeated |  |






<a name="gitaly.PackRefsRequest"></a>

### PackRefsRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  |  |
| all_refs | [bool](#bool) |  |  |






<a name="gitaly.PackRefsResponse"></a>

### PackRefsResponse







<a name="gitaly.RefExistsRequest"></a>

### RefExistsRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  |  |
| ref | [bytes](#bytes) |  | Any ref, e.g. &#39;refs/heads/master&#39; or &#39;refs/tags/v1.0.1&#39;. Must start with &#39;refs/&#39;. |






<a name="gitaly.RefExistsResponse"></a>

### RefExistsResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| value | [bool](#bool) |  |  |





 


<a name="gitaly.CreateBranchResponse.Status"></a>

### CreateBranchResponse.Status


| Name | Number | Description |
| ---- | ------ | ----------- |
| OK | 0 |  |
| ERR_EXISTS | 1 |  |
| ERR_INVALID | 2 |  |
| ERR_INVALID_START_POINT | 3 |  |



<a name="gitaly.FindLocalBranchesRequest.SortBy"></a>

### FindLocalBranchesRequest.SortBy


| Name | Number | Description |
| ---- | ------ | ----------- |
| NAME | 0 |  |
| UPDATED_ASC | 1 |  |
| UPDATED_DESC | 2 |  |


 

 


<a name="gitaly.RefService"></a>

### RefService


| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| FindDefaultBranchName | [FindDefaultBranchNameRequest](#gitaly.FindDefaultBranchNameRequest) | [FindDefaultBranchNameResponse](#gitaly.FindDefaultBranchNameResponse) |  |
| FindAllBranchNames | [FindAllBranchNamesRequest](#gitaly.FindAllBranchNamesRequest) | [FindAllBranchNamesResponse](#gitaly.FindAllBranchNamesResponse) stream |  |
| FindAllTagNames | [FindAllTagNamesRequest](#gitaly.FindAllTagNamesRequest) | [FindAllTagNamesResponse](#gitaly.FindAllTagNamesResponse) stream |  |
| FindRefName | [FindRefNameRequest](#gitaly.FindRefNameRequest) | [FindRefNameResponse](#gitaly.FindRefNameResponse) | Find a Ref matching the given constraints. Response may be empty. |
| FindLocalBranches | [FindLocalBranchesRequest](#gitaly.FindLocalBranchesRequest) | [FindLocalBranchesResponse](#gitaly.FindLocalBranchesResponse) stream | Return a stream so we can divide the response in chunks of branches |
| FindAllBranches | [FindAllBranchesRequest](#gitaly.FindAllBranchesRequest) | [FindAllBranchesResponse](#gitaly.FindAllBranchesResponse) stream |  |
| FindAllTags | [FindAllTagsRequest](#gitaly.FindAllTagsRequest) | [FindAllTagsResponse](#gitaly.FindAllTagsResponse) stream |  |
| FindTag | [FindTagRequest](#gitaly.FindTagRequest) | [FindTagResponse](#gitaly.FindTagResponse) |  |
| FindAllRemoteBranches | [FindAllRemoteBranchesRequest](#gitaly.FindAllRemoteBranchesRequest) | [FindAllRemoteBranchesResponse](#gitaly.FindAllRemoteBranchesResponse) stream |  |
| RefExists | [RefExistsRequest](#gitaly.RefExistsRequest) | [RefExistsResponse](#gitaly.RefExistsResponse) |  |
| FindBranch | [FindBranchRequest](#gitaly.FindBranchRequest) | [FindBranchResponse](#gitaly.FindBranchResponse) |  |
| DeleteRefs | [DeleteRefsRequest](#gitaly.DeleteRefsRequest) | [DeleteRefsResponse](#gitaly.DeleteRefsResponse) |  |
| ListBranchNamesContainingCommit | [ListBranchNamesContainingCommitRequest](#gitaly.ListBranchNamesContainingCommitRequest) | [ListBranchNamesContainingCommitResponse](#gitaly.ListBranchNamesContainingCommitResponse) stream |  |
| ListTagNamesContainingCommit | [ListTagNamesContainingCommitRequest](#gitaly.ListTagNamesContainingCommitRequest) | [ListTagNamesContainingCommitResponse](#gitaly.ListTagNamesContainingCommitResponse) stream |  |
| GetTagMessages | [GetTagMessagesRequest](#gitaly.GetTagMessagesRequest) | [GetTagMessagesResponse](#gitaly.GetTagMessagesResponse) stream |  |
| ListNewCommits | [ListNewCommitsRequest](#gitaly.ListNewCommitsRequest) | [ListNewCommitsResponse](#gitaly.ListNewCommitsResponse) stream | Returns commits that are only reachable from the ref passed |
| ListNewBlobs | [ListNewBlobsRequest](#gitaly.ListNewBlobsRequest) | [ListNewBlobsResponse](#gitaly.ListNewBlobsResponse) stream |  |
| PackRefs | [PackRefsRequest](#gitaly.PackRefsRequest) | [PackRefsResponse](#gitaly.PackRefsResponse) |  |

 



<a name="remote.proto"></a>
<p align="right"><a href="#top">Top</a></p>

## remote.proto



<a name="gitaly.AddRemoteRequest"></a>

### AddRemoteRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  |  |
| name | [string](#string) |  |  |
| url | [string](#string) |  |  |
| mirror_refmaps | [string](#string) | repeated | If any, the remote is configured as a mirror with those mappings |






<a name="gitaly.AddRemoteResponse"></a>

### AddRemoteResponse







<a name="gitaly.FetchInternalRemoteRequest"></a>

### FetchInternalRemoteRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  |  |
| remote_repository | [Repository](#gitaly.Repository) |  |  |






<a name="gitaly.FetchInternalRemoteResponse"></a>

### FetchInternalRemoteResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| result | [bool](#bool) |  |  |






<a name="gitaly.FindRemoteRepositoryRequest"></a>

### FindRemoteRepositoryRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| remote | [string](#string) |  |  |






<a name="gitaly.FindRemoteRepositoryResponse"></a>

### FindRemoteRepositoryResponse
This migth throw a GRPC Unavailable code, to signal the request failure
is transient.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| exists | [bool](#bool) |  |  |






<a name="gitaly.FindRemoteRootRefRequest"></a>

### FindRemoteRootRefRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  |  |
| remote | [string](#string) |  |  |






<a name="gitaly.FindRemoteRootRefResponse"></a>

### FindRemoteRootRefResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| ref | [string](#string) |  |  |






<a name="gitaly.ListRemotesRequest"></a>

### ListRemotesRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  |  |






<a name="gitaly.ListRemotesResponse"></a>

### ListRemotesResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| remotes | [ListRemotesResponse.Remote](#gitaly.ListRemotesResponse.Remote) | repeated |  |






<a name="gitaly.ListRemotesResponse.Remote"></a>

### ListRemotesResponse.Remote



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [string](#string) |  |  |
| fetch_url | [string](#string) |  |  |
| push_url | [string](#string) |  |  |






<a name="gitaly.RemoveRemoteRequest"></a>

### RemoveRemoteRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  |  |
| name | [string](#string) |  |  |






<a name="gitaly.RemoveRemoteResponse"></a>

### RemoveRemoteResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| result | [bool](#bool) |  |  |






<a name="gitaly.UpdateRemoteMirrorRequest"></a>

### UpdateRemoteMirrorRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  |  |
| ref_name | [string](#string) |  |  |
| only_branches_matching | [bytes](#bytes) | repeated |  |
| ssh_key | [string](#string) |  |  |
| known_hosts | [string](#string) |  |  |
| keep_divergent_refs | [bool](#bool) |  |  |






<a name="gitaly.UpdateRemoteMirrorResponse"></a>

### UpdateRemoteMirrorResponse






 

 

 


<a name="gitaly.RemoteService"></a>

### RemoteService


| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| AddRemote | [AddRemoteRequest](#gitaly.AddRemoteRequest) | [AddRemoteResponse](#gitaly.AddRemoteResponse) |  |
| FetchInternalRemote | [FetchInternalRemoteRequest](#gitaly.FetchInternalRemoteRequest) | [FetchInternalRemoteResponse](#gitaly.FetchInternalRemoteResponse) |  |
| RemoveRemote | [RemoveRemoteRequest](#gitaly.RemoveRemoteRequest) | [RemoveRemoteResponse](#gitaly.RemoveRemoteResponse) |  |
| UpdateRemoteMirror | [UpdateRemoteMirrorRequest](#gitaly.UpdateRemoteMirrorRequest) stream | [UpdateRemoteMirrorResponse](#gitaly.UpdateRemoteMirrorResponse) |  |
| FindRemoteRepository | [FindRemoteRepositoryRequest](#gitaly.FindRemoteRepositoryRequest) | [FindRemoteRepositoryResponse](#gitaly.FindRemoteRepositoryResponse) |  |
| FindRemoteRootRef | [FindRemoteRootRefRequest](#gitaly.FindRemoteRootRefRequest) | [FindRemoteRootRefResponse](#gitaly.FindRemoteRootRefResponse) |  |
| ListRemotes | [ListRemotesRequest](#gitaly.ListRemotesRequest) | [ListRemotesResponse](#gitaly.ListRemotesResponse) stream |  |

 



<a name="repository-service.proto"></a>
<p align="right"><a href="#top">Top</a></p>

## repository-service.proto



<a name="gitaly.ApplyGitattributesRequest"></a>

### ApplyGitattributesRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  |  |
| revision | [bytes](#bytes) |  |  |






<a name="gitaly.ApplyGitattributesResponse"></a>

### ApplyGitattributesResponse







<a name="gitaly.BackupCustomHooksRequest"></a>

### BackupCustomHooksRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  |  |






<a name="gitaly.BackupCustomHooksResponse"></a>

### BackupCustomHooksResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| data | [bytes](#bytes) |  |  |






<a name="gitaly.CalculateChecksumRequest"></a>

### CalculateChecksumRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  |  |






<a name="gitaly.CalculateChecksumResponse"></a>

### CalculateChecksumResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| checksum | [string](#string) |  |  |






<a name="gitaly.CleanupRequest"></a>

### CleanupRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  |  |






<a name="gitaly.CleanupResponse"></a>

### CleanupResponse







<a name="gitaly.CloneFromPoolInternalRequest"></a>

### CloneFromPoolInternalRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  |  |
| pool | [ObjectPool](#gitaly.ObjectPool) |  |  |
| source_repository | [Repository](#gitaly.Repository) |  |  |






<a name="gitaly.CloneFromPoolInternalResponse"></a>

### CloneFromPoolInternalResponse







<a name="gitaly.CloneFromPoolRequest"></a>

### CloneFromPoolRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  |  |
| pool | [ObjectPool](#gitaly.ObjectPool) |  |  |
| remote | [Remote](#gitaly.Remote) |  |  |






<a name="gitaly.CloneFromPoolResponse"></a>

### CloneFromPoolResponse







<a name="gitaly.CreateBundleRequest"></a>

### CreateBundleRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  |  |






<a name="gitaly.CreateBundleResponse"></a>

### CreateBundleResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| data | [bytes](#bytes) |  |  |






<a name="gitaly.CreateForkRequest"></a>

### CreateForkRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  |  |
| source_repository | [Repository](#gitaly.Repository) |  |  |






<a name="gitaly.CreateForkResponse"></a>

### CreateForkResponse







<a name="gitaly.CreateRepositoryFromBundleRequest"></a>

### CreateRepositoryFromBundleRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  | Only available on the first message |
| data | [bytes](#bytes) |  |  |






<a name="gitaly.CreateRepositoryFromBundleResponse"></a>

### CreateRepositoryFromBundleResponse







<a name="gitaly.CreateRepositoryFromSnapshotRequest"></a>

### CreateRepositoryFromSnapshotRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  |  |
| http_url | [string](#string) |  |  |
| http_auth | [string](#string) |  |  |






<a name="gitaly.CreateRepositoryFromSnapshotResponse"></a>

### CreateRepositoryFromSnapshotResponse







<a name="gitaly.CreateRepositoryFromURLRequest"></a>

### CreateRepositoryFromURLRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  |  |
| url | [string](#string) |  |  |






<a name="gitaly.CreateRepositoryFromURLResponse"></a>

### CreateRepositoryFromURLResponse







<a name="gitaly.CreateRepositoryRequest"></a>

### CreateRepositoryRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  |  |






<a name="gitaly.CreateRepositoryResponse"></a>

### CreateRepositoryResponse







<a name="gitaly.DeleteConfigRequest"></a>

### DeleteConfigRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  |  |
| keys | [string](#string) | repeated |  |






<a name="gitaly.DeleteConfigResponse"></a>

### DeleteConfigResponse







<a name="gitaly.FetchHTTPRemoteRequest"></a>

### FetchHTTPRemoteRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  |  |
| remote | [Remote](#gitaly.Remote) |  |  |
| timeout | [int32](#int32) |  |  |






<a name="gitaly.FetchHTTPRemoteResponse"></a>

### FetchHTTPRemoteResponse







<a name="gitaly.FetchRemoteRequest"></a>

### FetchRemoteRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  |  |
| remote | [string](#string) |  |  |
| force | [bool](#bool) |  |  |
| no_tags | [bool](#bool) |  |  |
| timeout | [int32](#int32) |  |  |
| ssh_key | [string](#string) |  |  |
| known_hosts | [string](#string) |  |  |
| no_prune | [bool](#bool) |  |  |
| remote_params | [Remote](#gitaly.Remote) |  |  |






<a name="gitaly.FetchRemoteResponse"></a>

### FetchRemoteResponse







<a name="gitaly.FetchSourceBranchRequest"></a>

### FetchSourceBranchRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  |  |
| source_repository | [Repository](#gitaly.Repository) |  |  |
| source_branch | [bytes](#bytes) |  |  |
| target_ref | [bytes](#bytes) |  |  |






<a name="gitaly.FetchSourceBranchResponse"></a>

### FetchSourceBranchResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| result | [bool](#bool) |  |  |






<a name="gitaly.FindLicenseRequest"></a>

### FindLicenseRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  |  |






<a name="gitaly.FindLicenseResponse"></a>

### FindLicenseResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| license_short_name | [string](#string) |  |  |






<a name="gitaly.FindMergeBaseRequest"></a>

### FindMergeBaseRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  |  |
| revisions | [bytes](#bytes) | repeated | We use a repeated field because rugged supports finding a base for more than 2 revisions, so if we needed that in the future we don&#39;t need to change the protocol. |






<a name="gitaly.FindMergeBaseResponse"></a>

### FindMergeBaseResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| base | [string](#string) |  |  |






<a name="gitaly.FsckRequest"></a>

### FsckRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  |  |






<a name="gitaly.FsckResponse"></a>

### FsckResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| error | [bytes](#bytes) |  |  |






<a name="gitaly.GarbageCollectRequest"></a>

### GarbageCollectRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  |  |
| create_bitmap | [bool](#bool) |  |  |






<a name="gitaly.GarbageCollectResponse"></a>

### GarbageCollectResponse







<a name="gitaly.GetArchiveRequest"></a>

### GetArchiveRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  |  |
| commit_id | [string](#string) |  |  |
| prefix | [string](#string) |  |  |
| format | [GetArchiveRequest.Format](#gitaly.GetArchiveRequest.Format) |  |  |
| path | [bytes](#bytes) |  |  |






<a name="gitaly.GetArchiveResponse"></a>

### GetArchiveResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| data | [bytes](#bytes) |  |  |






<a name="gitaly.GetInfoAttributesRequest"></a>

### GetInfoAttributesRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  |  |






<a name="gitaly.GetInfoAttributesResponse"></a>

### GetInfoAttributesResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| attributes | [bytes](#bytes) |  |  |






<a name="gitaly.GetObjectDirectorySizeRequest"></a>

### GetObjectDirectorySizeRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  |  |






<a name="gitaly.GetObjectDirectorySizeResponse"></a>

### GetObjectDirectorySizeResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| size | [int64](#int64) |  | Object directory size in kilobytes |






<a name="gitaly.GetRawChangesRequest"></a>

### GetRawChangesRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  |  |
| from_revision | [string](#string) |  |  |
| to_revision | [string](#string) |  |  |






<a name="gitaly.GetRawChangesResponse"></a>

### GetRawChangesResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| raw_changes | [GetRawChangesResponse.RawChange](#gitaly.GetRawChangesResponse.RawChange) | repeated |  |






<a name="gitaly.GetRawChangesResponse.RawChange"></a>

### GetRawChangesResponse.RawChange



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| blob_id | [string](#string) |  |  |
| size | [int64](#int64) |  |  |
| new_path | [string](#string) |  | use fields 9 and 10 in place of 3 and 4 (respectively) |
| old_path | [string](#string) |  |  |
| operation | [GetRawChangesResponse.RawChange.Operation](#gitaly.GetRawChangesResponse.RawChange.Operation) |  |  |
| raw_operation | [string](#string) |  |  |
| old_mode | [int32](#int32) |  |  |
| new_mode | [int32](#int32) |  |  |
| new_path_bytes | [bytes](#bytes) |  | the following fields, 9 and 10, will eventually replace 3 and 4 |
| old_path_bytes | [bytes](#bytes) |  |  |






<a name="gitaly.GetSnapshotRequest"></a>

### GetSnapshotRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  |  |






<a name="gitaly.GetSnapshotResponse"></a>

### GetSnapshotResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| data | [bytes](#bytes) |  |  |






<a name="gitaly.HasLocalBranchesRequest"></a>

### HasLocalBranchesRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  |  |






<a name="gitaly.HasLocalBranchesResponse"></a>

### HasLocalBranchesResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| value | [bool](#bool) |  |  |






<a name="gitaly.IsRebaseInProgressRequest"></a>

### IsRebaseInProgressRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  |  |
| rebase_id | [string](#string) |  |  |






<a name="gitaly.IsRebaseInProgressResponse"></a>

### IsRebaseInProgressResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| in_progress | [bool](#bool) |  |  |






<a name="gitaly.IsSquashInProgressRequest"></a>

### IsSquashInProgressRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  |  |
| squash_id | [string](#string) |  |  |






<a name="gitaly.IsSquashInProgressResponse"></a>

### IsSquashInProgressResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| in_progress | [bool](#bool) |  |  |






<a name="gitaly.Remote"></a>

### Remote



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| url | [string](#string) |  |  |
| name | [string](#string) |  |  |
| http_authorization_header | [string](#string) |  |  |
| mirror_refmaps | [string](#string) | repeated |  |






<a name="gitaly.RemoveRepositoryRequest"></a>

### RemoveRepositoryRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  |  |






<a name="gitaly.RemoveRepositoryResponse"></a>

### RemoveRepositoryResponse







<a name="gitaly.RenameRepositoryRequest"></a>

### RenameRepositoryRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  |  |
| relative_path | [string](#string) |  |  |






<a name="gitaly.RenameRepositoryResponse"></a>

### RenameRepositoryResponse







<a name="gitaly.RepackFullRequest"></a>

### RepackFullRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  |  |
| create_bitmap | [bool](#bool) |  |  |






<a name="gitaly.RepackFullResponse"></a>

### RepackFullResponse







<a name="gitaly.RepackIncrementalRequest"></a>

### RepackIncrementalRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  |  |






<a name="gitaly.RepackIncrementalResponse"></a>

### RepackIncrementalResponse







<a name="gitaly.ReplicateRepositoryRequest"></a>

### ReplicateRepositoryRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  |  |
| source | [Repository](#gitaly.Repository) |  |  |






<a name="gitaly.ReplicateRepositoryResponse"></a>

### ReplicateRepositoryResponse







<a name="gitaly.RepositoryExistsRequest"></a>

### RepositoryExistsRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  |  |






<a name="gitaly.RepositoryExistsResponse"></a>

### RepositoryExistsResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| exists | [bool](#bool) |  |  |






<a name="gitaly.RepositorySizeRequest"></a>

### RepositorySizeRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  |  |






<a name="gitaly.RepositorySizeResponse"></a>

### RepositorySizeResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| size | [int64](#int64) |  | Repository size in kilobytes |






<a name="gitaly.RestoreCustomHooksRequest"></a>

### RestoreCustomHooksRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  |  |
| data | [bytes](#bytes) |  |  |






<a name="gitaly.RestoreCustomHooksResponse"></a>

### RestoreCustomHooksResponse







<a name="gitaly.SearchFilesByContentRequest"></a>

### SearchFilesByContentRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  |  |
| query | [string](#string) |  |  |
| ref | [bytes](#bytes) |  |  |
| chunked_response | [bool](#bool) |  |  |






<a name="gitaly.SearchFilesByContentResponse"></a>

### SearchFilesByContentResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| matches | [bytes](#bytes) | repeated |  |
| match_data | [bytes](#bytes) |  |  |
| end_of_match | [bool](#bool) |  |  |






<a name="gitaly.SearchFilesByNameRequest"></a>

### SearchFilesByNameRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  |  |
| query | [string](#string) |  |  |
| ref | [bytes](#bytes) |  |  |






<a name="gitaly.SearchFilesByNameResponse"></a>

### SearchFilesByNameResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| files | [bytes](#bytes) | repeated |  |






<a name="gitaly.SetConfigRequest"></a>

### SetConfigRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  |  |
| entries | [SetConfigRequest.Entry](#gitaly.SetConfigRequest.Entry) | repeated |  |






<a name="gitaly.SetConfigRequest.Entry"></a>

### SetConfigRequest.Entry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| value_str | [string](#string) |  |  |
| value_int32 | [int32](#int32) |  |  |
| value_bool | [bool](#bool) |  |  |






<a name="gitaly.SetConfigResponse"></a>

### SetConfigResponse







<a name="gitaly.WriteRefRequest"></a>

### WriteRefRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  |  |
| ref | [bytes](#bytes) |  |  |
| revision | [bytes](#bytes) |  |  |
| old_revision | [bytes](#bytes) |  |  |
| force | [bool](#bool) |  |  |






<a name="gitaly.WriteRefResponse"></a>

### WriteRefResponse






 


<a name="gitaly.GetArchiveRequest.Format"></a>

### GetArchiveRequest.Format


| Name | Number | Description |
| ---- | ------ | ----------- |
| ZIP | 0 |  |
| TAR | 1 |  |
| TAR_GZ | 2 |  |
| TAR_BZ2 | 3 |  |



<a name="gitaly.GetRawChangesResponse.RawChange.Operation"></a>

### GetRawChangesResponse.RawChange.Operation


| Name | Number | Description |
| ---- | ------ | ----------- |
| UNKNOWN | 0 |  |
| ADDED | 1 |  |
| COPIED | 2 |  |
| DELETED | 3 |  |
| MODIFIED | 4 |  |
| RENAMED | 5 |  |
| TYPE_CHANGED | 6 |  |


 

 


<a name="gitaly.RepositoryService"></a>

### RepositoryService


| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| RepositoryExists | [RepositoryExistsRequest](#gitaly.RepositoryExistsRequest) | [RepositoryExistsResponse](#gitaly.RepositoryExistsResponse) |  |
| RepackIncremental | [RepackIncrementalRequest](#gitaly.RepackIncrementalRequest) | [RepackIncrementalResponse](#gitaly.RepackIncrementalResponse) |  |
| RepackFull | [RepackFullRequest](#gitaly.RepackFullRequest) | [RepackFullResponse](#gitaly.RepackFullResponse) |  |
| GarbageCollect | [GarbageCollectRequest](#gitaly.GarbageCollectRequest) | [GarbageCollectResponse](#gitaly.GarbageCollectResponse) |  |
| RepositorySize | [RepositorySizeRequest](#gitaly.RepositorySizeRequest) | [RepositorySizeResponse](#gitaly.RepositorySizeResponse) |  |
| ApplyGitattributes | [ApplyGitattributesRequest](#gitaly.ApplyGitattributesRequest) | [ApplyGitattributesResponse](#gitaly.ApplyGitattributesResponse) |  |
| FetchRemote | [FetchRemoteRequest](#gitaly.FetchRemoteRequest) | [FetchRemoteResponse](#gitaly.FetchRemoteResponse) |  |
| CreateRepository | [CreateRepositoryRequest](#gitaly.CreateRepositoryRequest) | [CreateRepositoryResponse](#gitaly.CreateRepositoryResponse) |  |
| GetArchive | [GetArchiveRequest](#gitaly.GetArchiveRequest) | [GetArchiveResponse](#gitaly.GetArchiveResponse) stream |  |
| HasLocalBranches | [HasLocalBranchesRequest](#gitaly.HasLocalBranchesRequest) | [HasLocalBranchesResponse](#gitaly.HasLocalBranchesResponse) |  |
| FetchSourceBranch | [FetchSourceBranchRequest](#gitaly.FetchSourceBranchRequest) | [FetchSourceBranchResponse](#gitaly.FetchSourceBranchResponse) |  |
| Fsck | [FsckRequest](#gitaly.FsckRequest) | [FsckResponse](#gitaly.FsckResponse) |  |
| WriteRef | [WriteRefRequest](#gitaly.WriteRefRequest) | [WriteRefResponse](#gitaly.WriteRefResponse) |  |
| FindMergeBase | [FindMergeBaseRequest](#gitaly.FindMergeBaseRequest) | [FindMergeBaseResponse](#gitaly.FindMergeBaseResponse) |  |
| CreateFork | [CreateForkRequest](#gitaly.CreateForkRequest) | [CreateForkResponse](#gitaly.CreateForkResponse) |  |
| IsRebaseInProgress | [IsRebaseInProgressRequest](#gitaly.IsRebaseInProgressRequest) | [IsRebaseInProgressResponse](#gitaly.IsRebaseInProgressResponse) |  |
| IsSquashInProgress | [IsSquashInProgressRequest](#gitaly.IsSquashInProgressRequest) | [IsSquashInProgressResponse](#gitaly.IsSquashInProgressResponse) |  |
| CreateRepositoryFromURL | [CreateRepositoryFromURLRequest](#gitaly.CreateRepositoryFromURLRequest) | [CreateRepositoryFromURLResponse](#gitaly.CreateRepositoryFromURLResponse) |  |
| CreateBundle | [CreateBundleRequest](#gitaly.CreateBundleRequest) | [CreateBundleResponse](#gitaly.CreateBundleResponse) stream |  |
| CreateRepositoryFromBundle | [CreateRepositoryFromBundleRequest](#gitaly.CreateRepositoryFromBundleRequest) stream | [CreateRepositoryFromBundleResponse](#gitaly.CreateRepositoryFromBundleResponse) |  |
| SetConfig | [SetConfigRequest](#gitaly.SetConfigRequest) | [SetConfigResponse](#gitaly.SetConfigResponse) |  |
| DeleteConfig | [DeleteConfigRequest](#gitaly.DeleteConfigRequest) | [DeleteConfigResponse](#gitaly.DeleteConfigResponse) |  |
| FindLicense | [FindLicenseRequest](#gitaly.FindLicenseRequest) | [FindLicenseResponse](#gitaly.FindLicenseResponse) |  |
| GetInfoAttributes | [GetInfoAttributesRequest](#gitaly.GetInfoAttributesRequest) | [GetInfoAttributesResponse](#gitaly.GetInfoAttributesResponse) stream |  |
| CalculateChecksum | [CalculateChecksumRequest](#gitaly.CalculateChecksumRequest) | [CalculateChecksumResponse](#gitaly.CalculateChecksumResponse) |  |
| Cleanup | [CleanupRequest](#gitaly.CleanupRequest) | [CleanupResponse](#gitaly.CleanupResponse) |  |
| GetSnapshot | [GetSnapshotRequest](#gitaly.GetSnapshotRequest) | [GetSnapshotResponse](#gitaly.GetSnapshotResponse) stream |  |
| CreateRepositoryFromSnapshot | [CreateRepositoryFromSnapshotRequest](#gitaly.CreateRepositoryFromSnapshotRequest) | [CreateRepositoryFromSnapshotResponse](#gitaly.CreateRepositoryFromSnapshotResponse) |  |
| GetRawChanges | [GetRawChangesRequest](#gitaly.GetRawChangesRequest) | [GetRawChangesResponse](#gitaly.GetRawChangesResponse) stream |  |
| SearchFilesByContent | [SearchFilesByContentRequest](#gitaly.SearchFilesByContentRequest) | [SearchFilesByContentResponse](#gitaly.SearchFilesByContentResponse) stream |  |
| SearchFilesByName | [SearchFilesByNameRequest](#gitaly.SearchFilesByNameRequest) | [SearchFilesByNameResponse](#gitaly.SearchFilesByNameResponse) stream |  |
| RestoreCustomHooks | [RestoreCustomHooksRequest](#gitaly.RestoreCustomHooksRequest) stream | [RestoreCustomHooksResponse](#gitaly.RestoreCustomHooksResponse) |  |
| BackupCustomHooks | [BackupCustomHooksRequest](#gitaly.BackupCustomHooksRequest) | [BackupCustomHooksResponse](#gitaly.BackupCustomHooksResponse) stream |  |
| FetchHTTPRemote | [FetchHTTPRemoteRequest](#gitaly.FetchHTTPRemoteRequest) | [FetchHTTPRemoteResponse](#gitaly.FetchHTTPRemoteResponse) |  |
| GetObjectDirectorySize | [GetObjectDirectorySizeRequest](#gitaly.GetObjectDirectorySizeRequest) | [GetObjectDirectorySizeResponse](#gitaly.GetObjectDirectorySizeResponse) |  |
| CloneFromPool | [CloneFromPoolRequest](#gitaly.CloneFromPoolRequest) | [CloneFromPoolResponse](#gitaly.CloneFromPoolResponse) |  |
| CloneFromPoolInternal | [CloneFromPoolInternalRequest](#gitaly.CloneFromPoolInternalRequest) | [CloneFromPoolInternalResponse](#gitaly.CloneFromPoolInternalResponse) |  |
| RemoveRepository | [RemoveRepositoryRequest](#gitaly.RemoveRepositoryRequest) | [RemoveRepositoryResponse](#gitaly.RemoveRepositoryResponse) |  |
| RenameRepository | [RenameRepositoryRequest](#gitaly.RenameRepositoryRequest) | [RenameRepositoryResponse](#gitaly.RenameRepositoryResponse) |  |
| ReplicateRepository | [ReplicateRepositoryRequest](#gitaly.ReplicateRepositoryRequest) | [ReplicateRepositoryResponse](#gitaly.ReplicateRepositoryResponse) |  |

 



<a name="server.proto"></a>
<p align="right"><a href="#top">Top</a></p>

## server.proto



<a name="gitaly.DiskStatisticsRequest"></a>

### DiskStatisticsRequest







<a name="gitaly.DiskStatisticsResponse"></a>

### DiskStatisticsResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| storage_statuses | [DiskStatisticsResponse.StorageStatus](#gitaly.DiskStatisticsResponse.StorageStatus) | repeated |  |






<a name="gitaly.DiskStatisticsResponse.StorageStatus"></a>

### DiskStatisticsResponse.StorageStatus



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| storage_name | [string](#string) |  | When both available and used fields are equal 0 that means that Gitaly was unable to determine storage stats. |
| available | [int64](#int64) |  |  |
| used | [int64](#int64) |  |  |






<a name="gitaly.ServerInfoRequest"></a>

### ServerInfoRequest







<a name="gitaly.ServerInfoResponse"></a>

### ServerInfoResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| server_version | [string](#string) |  |  |
| git_version | [string](#string) |  |  |
| storage_statuses | [ServerInfoResponse.StorageStatus](#gitaly.ServerInfoResponse.StorageStatus) | repeated |  |






<a name="gitaly.ServerInfoResponse.StorageStatus"></a>

### ServerInfoResponse.StorageStatus



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| storage_name | [string](#string) |  |  |
| readable | [bool](#bool) |  |  |
| writeable | [bool](#bool) |  |  |
| fs_type | [string](#string) |  |  |
| filesystem_id | [string](#string) |  |  |





 

 

 


<a name="gitaly.ServerService"></a>

### ServerService


| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| ServerInfo | [ServerInfoRequest](#gitaly.ServerInfoRequest) | [ServerInfoResponse](#gitaly.ServerInfoResponse) |  |
| DiskStatistics | [DiskStatisticsRequest](#gitaly.DiskStatisticsRequest) | [DiskStatisticsResponse](#gitaly.DiskStatisticsResponse) |  |

 



<a name="shared.proto"></a>
<p align="right"><a href="#top">Top</a></p>

## shared.proto



<a name="gitaly.Branch"></a>

### Branch
Corresponds to Gitlab::Git::Branch


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [bytes](#bytes) |  |  |
| target_commit | [GitCommit](#gitaly.GitCommit) |  |  |






<a name="gitaly.CommitAuthor"></a>

### CommitAuthor



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [bytes](#bytes) |  |  |
| email | [bytes](#bytes) |  |  |
| date | [google.protobuf.Timestamp](#google.protobuf.Timestamp) |  |  |
| timezone | [bytes](#bytes) |  |  |






<a name="gitaly.ExitStatus"></a>

### ExitStatus



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| value | [int32](#int32) |  |  |






<a name="gitaly.GitCommit"></a>

### GitCommit
Corresponds to Gitlab::Git::Commit


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [string](#string) |  |  |
| subject | [bytes](#bytes) |  |  |
| body | [bytes](#bytes) |  |  |
| author | [CommitAuthor](#gitaly.CommitAuthor) |  |  |
| committer | [CommitAuthor](#gitaly.CommitAuthor) |  |  |
| parent_ids | [string](#string) | repeated |  |
| body_size | [int64](#int64) |  | If body exceeds a certain threshold, it will be nullified, but its size will be set in body_size so we can know if a commit had a body in the first place. |
| signature_type | [SignatureType](#gitaly.SignatureType) |  |  |






<a name="gitaly.ObjectPool"></a>

### ObjectPool



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  |  |






<a name="gitaly.Repository"></a>

### Repository



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| storage_name | [string](#string) |  |  |
| relative_path | [string](#string) |  |  |
| git_object_directory | [string](#string) |  | Sets the GIT_OBJECT_DIRECTORY envvar on git commands to the value of this field. It influences the object storage directory the SHA1 directories are created underneath. See [Git object quarantine](../doc/object_quarantine.md). |
| git_alternate_object_directories | [string](#string) | repeated | Sets the GIT_ALTERNATE_OBJECT_DIRECTORIES envvar on git commands to the values of this field. It influences the list of Git object directories which can be used to search for Git objects. |
| gl_repository | [string](#string) |  | Used in callbacks to GitLab so that it knows what repository the event is associated with. May be left empty on RPC&#39;s that do not perform callbacks. During project creation, `gl_repository` may not be known. |
| gl_project_path | [string](#string) |  | The human-readable GitLab project path (e.g. gitlab-org/gitlab-ce). When hashed storage is use, this associates a project path with its path on disk. The name can change over time (e.g. when a project is renamed). This is primarily used for logging/debugging at the moment. |






<a name="gitaly.Tag"></a>

### Tag



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [bytes](#bytes) |  |  |
| id | [string](#string) |  |  |
| target_commit | [GitCommit](#gitaly.GitCommit) |  |  |
| message | [bytes](#bytes) |  | If message exceeds a certain threshold, it will be nullified, but its size will be set in message_size so we can know if a tag had a message in the first place. |
| message_size | [int64](#int64) |  |  |
| tagger | [CommitAuthor](#gitaly.CommitAuthor) |  |  |
| signature_type | [SignatureType](#gitaly.SignatureType) |  |  |






<a name="gitaly.User"></a>

### User



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| gl_id | [string](#string) |  |  |
| name | [bytes](#bytes) |  |  |
| email | [bytes](#bytes) |  |  |
| gl_username | [string](#string) |  |  |





 


<a name="gitaly.ObjectType"></a>

### ObjectType


| Name | Number | Description |
| ---- | ------ | ----------- |
| UNKNOWN | 0 |  |
| COMMIT | 1 |  |
| BLOB | 2 |  |
| TREE | 3 |  |
| TAG | 4 |  |



<a name="gitaly.SignatureType"></a>

### SignatureType


| Name | Number | Description |
| ---- | ------ | ----------- |
| NONE | 0 |  |
| PGP | 1 |  |
| X509 | 2 | maybe add X509&#43;TSA or other combinations at a later step |


 

 

 



<a name="smarthttp.proto"></a>
<p align="right"><a href="#top">Top</a></p>

## smarthttp.proto



<a name="gitaly.InfoRefsRequest"></a>

### InfoRefsRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  |  |
| git_config_options | [string](#string) | repeated | Parameters to use with git -c (key=value pairs) |
| git_protocol | [string](#string) |  | Git protocol version |






<a name="gitaly.InfoRefsResponse"></a>

### InfoRefsResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| data | [bytes](#bytes) |  |  |






<a name="gitaly.PostReceivePackRequest"></a>

### PostReceivePackRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  | repository should only be present in the first message of the stream |
| data | [bytes](#bytes) |  | Raw data to be copied to stdin of &#39;git receive-pack&#39; |
| gl_id | [string](#string) |  | gl_id, gl_repository, and gl_username become env variables, used by the Git {pre,post}-receive hooks. They should only be present in the first message of the stream. |
| gl_repository | [string](#string) |  |  |
| gl_username | [string](#string) |  |  |
| git_protocol | [string](#string) |  | Git protocol version |
| git_config_options | [string](#string) | repeated | Parameters to use with git -c (key=value pairs) |






<a name="gitaly.PostReceivePackResponse"></a>

### PostReceivePackResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| data | [bytes](#bytes) |  | Raw data from stdout of &#39;git receive-pack&#39; |






<a name="gitaly.PostUploadPackRequest"></a>

### PostUploadPackRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  | repository should only be present in the first message of the stream |
| data | [bytes](#bytes) |  | Raw data to be copied to stdin of &#39;git upload-pack&#39; |
| git_config_options | [string](#string) | repeated | Parameters to use with git -c (key=value pairs) |
| git_protocol | [string](#string) |  | Git protocol version |






<a name="gitaly.PostUploadPackResponse"></a>

### PostUploadPackResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| data | [bytes](#bytes) |  | Raw data from stdout of &#39;git upload-pack&#39; |





 

 

 


<a name="gitaly.SmartHTTPService"></a>

### SmartHTTPService


| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| InfoRefsUploadPack | [InfoRefsRequest](#gitaly.InfoRefsRequest) | [InfoRefsResponse](#gitaly.InfoRefsResponse) stream | The response body for GET /info/refs?service=git-upload-pack Will be invoked when the user executes a `git fetch`, meaning the server will upload the packs to that user. The user doesn&#39;t upload new objects. |
| InfoRefsReceivePack | [InfoRefsRequest](#gitaly.InfoRefsRequest) | [InfoRefsResponse](#gitaly.InfoRefsResponse) stream | The response body for GET /info/refs?service=git-receive-pack Will be invoked when the user executes a `git push`, meaning the server will receive new objects in the pack from the user. |
| PostUploadPack | [PostUploadPackRequest](#gitaly.PostUploadPackRequest) stream | [PostUploadPackResponse](#gitaly.PostUploadPackResponse) stream | Request and response body for POST /upload-pack |
| PostReceivePack | [PostReceivePackRequest](#gitaly.PostReceivePackRequest) stream | [PostReceivePackResponse](#gitaly.PostReceivePackResponse) stream | Request and response body for POST /receive-pack |

 



<a name="ssh.proto"></a>
<p align="right"><a href="#top">Top</a></p>

## ssh.proto



<a name="gitaly.SSHReceivePackRequest"></a>

### SSHReceivePackRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  | &#39;repository&#39; must be present in the first message. |
| stdin | [bytes](#bytes) |  | A chunk of raw data to be copied to &#39;git upload-pack&#39; standard input |
| gl_id | [string](#string) |  | Contents of GL_ID, GL_REPOSITORY, and GL_USERNAME environment variables for &#39;git receive-pack&#39; |
| gl_repository | [string](#string) |  |  |
| gl_username | [string](#string) |  |  |
| git_protocol | [string](#string) |  | Git protocol version |
| git_config_options | [string](#string) | repeated | Parameters to use with git -c (key=value pairs) |






<a name="gitaly.SSHReceivePackResponse"></a>

### SSHReceivePackResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| stdout | [bytes](#bytes) |  | A chunk of raw data from &#39;git receive-pack&#39; standard output |
| stderr | [bytes](#bytes) |  | A chunk of raw data from &#39;git receive-pack&#39; standard error |
| exit_status | [ExitStatus](#gitaly.ExitStatus) |  | This field may be nil. This is intentional: only when the remote command has finished can we return its exit status. |






<a name="gitaly.SSHUploadArchiveRequest"></a>

### SSHUploadArchiveRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  | &#39;repository&#39; must be present in the first message. |
| stdin | [bytes](#bytes) |  | A chunk of raw data to be copied to &#39;git upload-archive&#39; standard input |






<a name="gitaly.SSHUploadArchiveResponse"></a>

### SSHUploadArchiveResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| stdout | [bytes](#bytes) |  | A chunk of raw data from &#39;git upload-archive&#39; standard output |
| stderr | [bytes](#bytes) |  | A chunk of raw data from &#39;git upload-archive&#39; standard error |
| exit_status | [ExitStatus](#gitaly.ExitStatus) |  | This value will only be set on the last message |






<a name="gitaly.SSHUploadPackRequest"></a>

### SSHUploadPackRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  | &#39;repository&#39; must be present in the first message. |
| stdin | [bytes](#bytes) |  | A chunk of raw data to be copied to &#39;git upload-pack&#39; standard input |
| git_config_options | [string](#string) | repeated | Parameters to use with git -c (key=value pairs) |
| git_protocol | [string](#string) |  | Git protocol version |






<a name="gitaly.SSHUploadPackResponse"></a>

### SSHUploadPackResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| stdout | [bytes](#bytes) |  | A chunk of raw data from &#39;git upload-pack&#39; standard output |
| stderr | [bytes](#bytes) |  | A chunk of raw data from &#39;git upload-pack&#39; standard error |
| exit_status | [ExitStatus](#gitaly.ExitStatus) |  | This field may be nil. This is intentional: only when the remote command has finished can we return its exit status. |





 

 

 


<a name="gitaly.SSHService"></a>

### SSHService


| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| SSHUploadPack | [SSHUploadPackRequest](#gitaly.SSHUploadPackRequest) stream | [SSHUploadPackResponse](#gitaly.SSHUploadPackResponse) stream | To forward &#39;git upload-pack&#39; to Gitaly for SSH sessions |
| SSHReceivePack | [SSHReceivePackRequest](#gitaly.SSHReceivePackRequest) stream | [SSHReceivePackResponse](#gitaly.SSHReceivePackResponse) stream | To forward &#39;git receive-pack&#39; to Gitaly for SSH sessions |
| SSHUploadArchive | [SSHUploadArchiveRequest](#gitaly.SSHUploadArchiveRequest) stream | [SSHUploadArchiveResponse](#gitaly.SSHUploadArchiveResponse) stream | To forward &#39;git upload-archive&#39; to Gitaly for SSH sessions |

 



<a name="wiki.proto"></a>
<p align="right"><a href="#top">Top</a></p>

## wiki.proto



<a name="gitaly.WikiCommitDetails"></a>

### WikiCommitDetails



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [bytes](#bytes) |  |  |
| email | [bytes](#bytes) |  |  |
| message | [bytes](#bytes) |  |  |
| user_id | [int32](#int32) |  |  |
| user_name | [bytes](#bytes) |  |  |






<a name="gitaly.WikiDeletePageRequest"></a>

### WikiDeletePageRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  |  |
| page_path | [bytes](#bytes) |  |  |
| commit_details | [WikiCommitDetails](#gitaly.WikiCommitDetails) |  |  |






<a name="gitaly.WikiDeletePageResponse"></a>

### WikiDeletePageResponse







<a name="gitaly.WikiFindFileRequest"></a>

### WikiFindFileRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  |  |
| name | [bytes](#bytes) |  |  |
| revision | [bytes](#bytes) |  | Optional: revision |






<a name="gitaly.WikiFindFileResponse"></a>

### WikiFindFileResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [bytes](#bytes) |  | If &#39;name&#39; is empty, the file was not found. |
| mime_type | [string](#string) |  |  |
| raw_data | [bytes](#bytes) |  |  |
| path | [bytes](#bytes) |  |  |






<a name="gitaly.WikiFindPageRequest"></a>

### WikiFindPageRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  |  |
| title | [bytes](#bytes) |  |  |
| revision | [bytes](#bytes) |  |  |
| directory | [bytes](#bytes) |  |  |






<a name="gitaly.WikiFindPageResponse"></a>

### WikiFindPageResponse
WikiFindPageResponse is a stream because we need multiple WikiPage
messages to send the raw_data field.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| page | [WikiPage](#gitaly.WikiPage) |  |  |






<a name="gitaly.WikiGetAllPagesRequest"></a>

### WikiGetAllPagesRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  |  |
| limit | [uint32](#uint32) |  | Passing 0 means no limit is applied |
| direction_desc | [bool](#bool) |  |  |
| sort | [WikiGetAllPagesRequest.SortBy](#gitaly.WikiGetAllPagesRequest.SortBy) |  |  |






<a name="gitaly.WikiGetAllPagesResponse"></a>

### WikiGetAllPagesResponse
The WikiGetAllPagesResponse stream is a concatenation of WikiPage streams


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| page | [WikiPage](#gitaly.WikiPage) |  |  |
| end_of_page | [bool](#bool) |  | When end_of_page is true it signals a change of page for the next Response message (if any) |






<a name="gitaly.WikiGetPageVersionsRequest"></a>

### WikiGetPageVersionsRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  |  |
| page_path | [bytes](#bytes) |  |  |
| page | [int32](#int32) |  |  |
| per_page | [int32](#int32) |  |  |






<a name="gitaly.WikiGetPageVersionsResponse"></a>

### WikiGetPageVersionsResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| versions | [WikiPageVersion](#gitaly.WikiPageVersion) | repeated |  |






<a name="gitaly.WikiListPagesRequest"></a>

### WikiListPagesRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  |  |
| limit | [uint32](#uint32) |  | Passing 0 means no limit is applied |
| direction_desc | [bool](#bool) |  |  |
| sort | [WikiListPagesRequest.SortBy](#gitaly.WikiListPagesRequest.SortBy) |  |  |
| offset | [uint32](#uint32) |  |  |






<a name="gitaly.WikiListPagesResponse"></a>

### WikiListPagesResponse
The WikiListPagesResponse stream is a concatenation of WikiPage streams without content


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| page | [WikiPage](#gitaly.WikiPage) |  |  |






<a name="gitaly.WikiPage"></a>

### WikiPage



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| version | [WikiPageVersion](#gitaly.WikiPageVersion) |  | These fields are only present in the first message of a WikiPage stream |
| format | [string](#string) |  |  |
| title | [bytes](#bytes) |  |  |
| url_path | [string](#string) |  |  |
| path | [bytes](#bytes) |  |  |
| name | [bytes](#bytes) |  |  |
| historical | [bool](#bool) |  |  |
| raw_data | [bytes](#bytes) |  | This field is present in all messages of a WikiPage stream |






<a name="gitaly.WikiPageVersion"></a>

### WikiPageVersion



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| commit | [GitCommit](#gitaly.GitCommit) |  |  |
| format | [string](#string) |  |  |






<a name="gitaly.WikiUpdatePageRequest"></a>

### WikiUpdatePageRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  | There fields are only present in the first message of the stream |
| page_path | [bytes](#bytes) |  |  |
| title | [bytes](#bytes) |  |  |
| format | [string](#string) |  |  |
| commit_details | [WikiCommitDetails](#gitaly.WikiCommitDetails) |  |  |
| content | [bytes](#bytes) |  | This field is present in all messages |






<a name="gitaly.WikiUpdatePageResponse"></a>

### WikiUpdatePageResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| error | [bytes](#bytes) |  |  |






<a name="gitaly.WikiWritePageRequest"></a>

### WikiWritePageRequest
This message is sent in a stream because the &#39;content&#39; field may be large.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| repository | [Repository](#gitaly.Repository) |  | These following fields are only present in the first message. |
| name | [bytes](#bytes) |  |  |
| format | [string](#string) |  |  |
| commit_details | [WikiCommitDetails](#gitaly.WikiCommitDetails) |  |  |
| content | [bytes](#bytes) |  | This field is present in all messages. |






<a name="gitaly.WikiWritePageResponse"></a>

### WikiWritePageResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| duplicate_error | [bytes](#bytes) |  |  |





 


<a name="gitaly.WikiGetAllPagesRequest.SortBy"></a>

### WikiGetAllPagesRequest.SortBy


| Name | Number | Description |
| ---- | ------ | ----------- |
| TITLE | 0 |  |
| CREATED_AT | 1 |  |



<a name="gitaly.WikiListPagesRequest.SortBy"></a>

### WikiListPagesRequest.SortBy


| Name | Number | Description |
| ---- | ------ | ----------- |
| TITLE | 0 |  |
| CREATED_AT | 1 |  |


 

 


<a name="gitaly.WikiService"></a>

### WikiService


| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| WikiGetPageVersions | [WikiGetPageVersionsRequest](#gitaly.WikiGetPageVersionsRequest) | [WikiGetPageVersionsResponse](#gitaly.WikiGetPageVersionsResponse) stream |  |
| WikiWritePage | [WikiWritePageRequest](#gitaly.WikiWritePageRequest) stream | [WikiWritePageResponse](#gitaly.WikiWritePageResponse) |  |
| WikiUpdatePage | [WikiUpdatePageRequest](#gitaly.WikiUpdatePageRequest) stream | [WikiUpdatePageResponse](#gitaly.WikiUpdatePageResponse) |  |
| WikiDeletePage | [WikiDeletePageRequest](#gitaly.WikiDeletePageRequest) | [WikiDeletePageResponse](#gitaly.WikiDeletePageResponse) |  |
| WikiFindPage | [WikiFindPageRequest](#gitaly.WikiFindPageRequest) | [WikiFindPageResponse](#gitaly.WikiFindPageResponse) stream | WikiFindPage returns a stream because the page&#39;s raw_data field may be arbitrarily large. |
| WikiFindFile | [WikiFindFileRequest](#gitaly.WikiFindFileRequest) | [WikiFindFileResponse](#gitaly.WikiFindFileResponse) stream |  |
| WikiGetAllPages | [WikiGetAllPagesRequest](#gitaly.WikiGetAllPagesRequest) | [WikiGetAllPagesResponse](#gitaly.WikiGetAllPagesResponse) stream |  |
| WikiListPages | [WikiListPagesRequest](#gitaly.WikiListPagesRequest) | [WikiListPagesResponse](#gitaly.WikiListPagesResponse) stream |  |

 



## Scalar Value Types

| .proto Type | Notes | C++ | Java | Python | Go | C# | PHP | Ruby |
| ----------- | ----- | --- | ---- | ------ | -- | -- | --- | ---- |
| <a name="double" /> double |  | double | double | float | float64 | double | float | Float |
| <a name="float" /> float |  | float | float | float | float32 | float | float | Float |
| <a name="int32" /> int32 | Uses variable-length encoding. Inefficient for encoding negative numbers – if your field is likely to have negative values, use sint32 instead. | int32 | int | int | int32 | int | integer | Bignum or Fixnum (as required) |
| <a name="int64" /> int64 | Uses variable-length encoding. Inefficient for encoding negative numbers – if your field is likely to have negative values, use sint64 instead. | int64 | long | int/long | int64 | long | integer/string | Bignum |
| <a name="uint32" /> uint32 | Uses variable-length encoding. | uint32 | int | int/long | uint32 | uint | integer | Bignum or Fixnum (as required) |
| <a name="uint64" /> uint64 | Uses variable-length encoding. | uint64 | long | int/long | uint64 | ulong | integer/string | Bignum or Fixnum (as required) |
| <a name="sint32" /> sint32 | Uses variable-length encoding. Signed int value. These more efficiently encode negative numbers than regular int32s. | int32 | int | int | int32 | int | integer | Bignum or Fixnum (as required) |
| <a name="sint64" /> sint64 | Uses variable-length encoding. Signed int value. These more efficiently encode negative numbers than regular int64s. | int64 | long | int/long | int64 | long | integer/string | Bignum |
| <a name="fixed32" /> fixed32 | Always four bytes. More efficient than uint32 if values are often greater than 2^28. | uint32 | int | int | uint32 | uint | integer | Bignum or Fixnum (as required) |
| <a name="fixed64" /> fixed64 | Always eight bytes. More efficient than uint64 if values are often greater than 2^56. | uint64 | long | int/long | uint64 | ulong | integer/string | Bignum |
| <a name="sfixed32" /> sfixed32 | Always four bytes. | int32 | int | int | int32 | int | integer | Bignum or Fixnum (as required) |
| <a name="sfixed64" /> sfixed64 | Always eight bytes. | int64 | long | int/long | int64 | long | integer/string | Bignum |
| <a name="bool" /> bool |  | bool | boolean | boolean | bool | bool | boolean | TrueClass/FalseClass |
| <a name="string" /> string | A string must always contain UTF-8 encoded or 7-bit ASCII text. | string | String | str/unicode | string | string | string | String (UTF-8) |
| <a name="bytes" /> bytes | May contain any arbitrary sequence of bytes. | string | ByteString | str | []byte | ByteString | string | String (ASCII-8BIT) |

