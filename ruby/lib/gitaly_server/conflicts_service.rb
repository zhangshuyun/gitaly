module GitalyServer
  class ConflictsService < Gitaly::ConflictsService::Service
    include Utils

    def resolve_conflicts(call)
      header = nil
      files_json = ""

      call.each_remote_read.each_with_index do |request, index|
        if index.zero?
          header = request.header
        else
          files_json << request.files_json
        end
      end

      repo = Gitlab::Git::Repository.from_gitaly(header.repository, call)
      remote_repo = Gitlab::Git::GitalyRemoteRepository.new(header.target_repository, call)
      resolver = Gitlab::Git::Conflict::Resolver.new(remote_repo, header.our_commit_oid, header.their_commit_oid)
      user = Gitlab::Git::User.from_gitaly(header.user)
      files = JSON.parse(files_json).map(&:with_indifferent_access)

      begin
        resolution = Gitlab::Git::Conflict::Resolution.new(user, files, header.commit_message.dup)
        params = {
          source_branch: header.source_branch,
          target_branch: header.target_branch
        }
        resolver.resolve_conflicts(repo, resolution, params)

        Gitaly::ResolveConflictsResponse.new
      rescue Gitlab::Git::Conflict::Resolver::ResolutionError => e
        Gitaly::ResolveConflictsResponse.new(resolution_error: e.message)
      end
    end
  end
end
