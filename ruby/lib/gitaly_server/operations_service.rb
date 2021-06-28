module GitalyServer
  class OperationsService < Gitaly::OperationService::Service
    include Utils

    # rubocop:disable Metrics/AbcSize
    def user_rebase_confirmable(session, call)
      Enumerator.new do |y|
        header = session.next.header
        transaction = Praefect::Transaction.from_metadata(call.metadata)

        repo = Gitlab::Git::Repository.from_gitaly(header.repository, call)
        user = Gitlab::Git::User.from_gitaly(header.user)
        remote_repository = Gitlab::Git::GitalyRemoteRepository.new(header.remote_repository, call)

        begin
          repo.rebase(
            user,
            header.rebase_id,
            branch: header.branch,
            branch_sha: header.branch_sha,
            remote_repository: remote_repository,
            remote_branch: header.remote_branch,
            push_options: Gitlab::Git::PushOptions.new(header.git_push_options),
            timestamp: header.timestamp,
            transaction: transaction
          ) do |rebase_sha|
            y << Gitaly::UserRebaseConfirmableResponse.new(rebase_sha: rebase_sha)

            raise GRPC::FailedPrecondition.new('rebase aborted by client') unless session.next.apply
          end

          y << Gitaly::UserRebaseConfirmableResponse.new(rebase_applied: true)
        rescue Gitlab::Git::PreReceiveError => e
          y << Gitaly::UserRebaseConfirmableResponse.new(pre_receive_error: set_utf8!(e.message))
        rescue Gitlab::Git::Repository::GitError => e
          y << Gitaly::UserRebaseConfirmableResponse.new(git_error: set_utf8!(e.message))
        rescue Gitlab::Git::CommitError => e
          raise GRPC::FailedPrecondition.new(e.message)
        end
      end
    end
    # rubocop:enable Metrics/AbcSize

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

    def get_param!(request, name)
      value = request[name.to_s]

      return value if value.present?

      field_name = name.to_s.tr('_', ' ')
      raise GRPC::InvalidArgument.new("empty #{field_name}")
    end
  end
end
