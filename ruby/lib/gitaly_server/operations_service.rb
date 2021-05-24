module GitalyServer
  class OperationsService < Gitaly::OperationService::Service
    include Utils

    def user_update_branch(request, call)
      repo = Gitlab::Git::Repository.from_gitaly(request.repository, call)
      branch_name = get_param!(request, :branch_name)
      newrev = get_param!(request, :newrev)
      oldrev = get_param!(request, :oldrev)
      gitaly_user = get_param!(request, :user)
      transaction = Praefect::Transaction.from_metadata(call.metadata)

      user = Gitlab::Git::User.from_gitaly(gitaly_user)
      repo.update_branch(branch_name, user: user, newrev: newrev, oldrev: oldrev, transaction: transaction)

      Gitaly::UserUpdateBranchResponse.new
    rescue Gitlab::Git::Repository::InvalidRef, Gitlab::Git::CommitError => ex
      raise GRPC::FailedPrecondition.new(ex.message)
    rescue Gitlab::Git::PreReceiveError => ex
      Gitaly::UserUpdateBranchResponse.new(pre_receive_error: set_utf8!(ex.message))
    end

    def user_cherry_pick(request, call)
      repo = Gitlab::Git::Repository.from_gitaly(request.repository, call)
      user = Gitlab::Git::User.from_gitaly(request.user)
      commit = Gitlab::Git::Commit.new(repo, request.commit)
      start_repository = Gitlab::Git::GitalyRemoteRepository.new(request.start_repository || request.repository, call)

      result = repo.cherry_pick(
        user: user,
        commit: commit,
        branch_name: request.branch_name,
        message: request.message.dup,
        start_branch_name: request.start_branch_name.presence,
        start_repository: start_repository,
        dry_run: request.dry_run,
        timestamp: request.timestamp
      )

      branch_update = branch_update_result(result)
      Gitaly::UserCherryPickResponse.new(branch_update: branch_update)
    rescue Gitlab::Git::Repository::CreateTreeError => e
      Gitaly::UserCherryPickResponse.new(
        create_tree_error: set_utf8!(e.message),
        create_tree_error_code: e.error.upcase
      )
    rescue Gitlab::Git::CommitError => e
      Gitaly::UserCherryPickResponse.new(commit_error: set_utf8!(e.message))
    rescue Gitlab::Git::PreReceiveError => e
      Gitaly::UserCherryPickResponse.new(pre_receive_error: set_utf8!(e.message))
    end

    def user_revert(request, call)
      repo = Gitlab::Git::Repository.from_gitaly(request.repository, call)
      user = Gitlab::Git::User.from_gitaly(request.user)
      commit = Gitlab::Git::Commit.new(repo, request.commit)
      start_repository = Gitlab::Git::GitalyRemoteRepository.new(request.start_repository || request.repository, call)

      result = repo.revert(
        user: user,
        commit: commit,
        branch_name: request.branch_name,
        message: request.message.dup,
        start_branch_name: request.start_branch_name.presence,
        start_repository: start_repository,
        dry_run: request.dry_run,
        timestamp: request.timestamp
      )

      branch_update = branch_update_result(result)
      Gitaly::UserRevertResponse.new(branch_update: branch_update)
    rescue Gitlab::Git::Repository::CreateTreeError => e
      Gitaly::UserRevertResponse.new(
        create_tree_error: set_utf8!(e.message),
        create_tree_error_code: e.error.upcase
      )
    rescue Gitlab::Git::CommitError => e
      Gitaly::UserRevertResponse.new(commit_error: set_utf8!(e.message))
    rescue Gitlab::Git::PreReceiveError => e
      Gitaly::UserRevertResponse.new(pre_receive_error: set_utf8!(e.message))
    end

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
