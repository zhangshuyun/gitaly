module GitalyServer
  class OperationsService < Gitaly::OperationService::Service
    include Utils

    def user_apply_patch(call)
      stream = call.each_remote_read
      first_request = stream.next

      header = first_request.header
      user = Gitlab::Git::User.from_gitaly(header.user)
      target_branch = header.target_branch
      patches = stream.lazy.map(&:patches)

      branch_update = Gitlab::Git::Repository.from_gitaly_with_block(header.repository, call) do |repo|
        begin
          Gitlab::Git::CommitPatches.new(user, repo, target_branch, patches, header.timestamp).commit
        rescue Gitlab::Git::PatchError => e
          raise GRPC::FailedPrecondition.new(e.message)
        end
      end

      Gitaly::UserApplyPatchResponse.new(branch_update: branch_update_result(branch_update))
    end

    private

    def branch_update_result(gitlab_update_result)
      return if gitlab_update_result.nil?

      Gitaly::OperationBranchUpdate.new(
        commit_id: gitlab_update_result.newrev,
        repo_created: gitlab_update_result.repo_created,
        branch_created: gitlab_update_result.branch_created
      )
    end
  end
end
