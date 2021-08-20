module GitalyServer
  class RemoteService < Gitaly::RemoteService::Service
    include Utils

    # Maximum number of divergent refs to return in UpdateRemoteMirrorResponse
    DIVERGENT_REF_LIMIT = 100

    def update_remote_mirror(call)
      request_enum = call.each_remote_read
      first_request = request_enum.next

      only_branches_matching = first_request.only_branches_matching.to_a
      only_branches_matching += request_enum.flat_map(&:only_branches_matching)

      remote_mirror = Gitlab::Git::RemoteMirror.new(
        Gitlab::Git::Repository.from_gitaly(first_request.repository, call),
        first_request.ref_name,
        ssh_auth: Gitlab::Git::SshAuth.from_gitaly(first_request),
        only_branches_matching: only_branches_matching,
        keep_divergent_refs: first_request.keep_divergent_refs
      )

      remote_mirror.update

      Gitaly::UpdateRemoteMirrorResponse.new(
        divergent_refs: remote_mirror.divergent_refs.take(DIVERGENT_REF_LIMIT)
      )
    end
  end
end
