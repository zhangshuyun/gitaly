module GitalyServer
  class OperationsService < Gitaly::OperationService::Service
    include Utils

    def user_create_tag(request, call)
      bridge_exceptions do
        begin
          repo = Gitlab::Git::Repository.from_gitaly(request.repository, call)

          gitaly_user = request.user
          raise GRPC::InvalidArgument.new('empty user') unless gitaly_user
          user = Gitlab::Git::User.from_gitaly(gitaly_user)

          tag_name = request.tag_name
          raise GRPC::InvalidArgument.new('empty tag name') unless tag_name.present?

          target_revision = request.target_revision
          raise GRPC::InvalidArgument.new('empty target revision') unless target_revision.present?

          created_tag = repo.add_tag(tag_name, user: user, target: target_revision, message: request.message.presence)
          return Gitaly::UserCreateTagResponse.new unless created_tag

          rugged_commit = created_tag.dereferenced_target.rugged_commit
          commit = gitaly_commit_from_rugged(rugged_commit)
          tag = Gitaly::Tag.new(
            name: tag_name.b,
            id: created_tag.target,
            target_commit: commit,
            message: created_tag.message.to_s.b
          )

          Gitaly::UserCreateTagResponse.new(tag: tag)
        rescue Gitlab::Git::Repository::InvalidRef => e
          raise GRPC::FailedPrecondition.new(e.message)
        rescue Gitlab::Git::Repository::TagExistsError
          return Gitaly::UserCreateTagResponse.new(exists: true)
        rescue Gitlab::Git::HooksService::PreReceiveError => e
          return Gitaly::UserCreateTagResponse.new(pre_receive_error: e.message)
        end
      end
    end

    def user_delete_tag(request, call)
      bridge_exceptions do
        begin
          repo = Gitlab::Git::Repository.from_gitaly(request.repository, call)

          gitaly_user = request.user
          raise GRPC::InvalidArgument.new('empty user') unless gitaly_user
          user = Gitlab::Git::User.from_gitaly(gitaly_user)

          tag_name = request.tag_name
          raise GRPC::InvalidArgument.new('empty tag name') if tag_name.blank?

          repo.rm_tag(tag_name, user: user)

          Gitaly::UserDeleteTagResponse.new
        rescue Gitlab::Git::HooksService::PreReceiveError => e
          Gitaly::UserDeleteTagResponse.new(pre_receive_error: e.message)
        end
      end
    end

    def user_create_branch(request, call)
      bridge_exceptions do
        begin
          repo = Gitlab::Git::Repository.from_gitaly(request.repository, call)
          target = request.start_point
          raise GRPC::InvalidArgument.new('empty start_point') if target.empty?
          gitaly_user = request.user
          raise GRPC::InvalidArgument.new('empty user') unless gitaly_user

          branch_name = request.branch_name
          user = Gitlab::Git::User.from_gitaly(gitaly_user)
          created_branch = repo.add_branch(branch_name, user: user, target: target)
          return Gitaly::UserCreateBranchResponse.new unless created_branch

          rugged_commit = created_branch.dereferenced_target.rugged_commit
          commit = gitaly_commit_from_rugged(rugged_commit)
          branch = Gitaly::Branch.new(name: branch_name, target_commit: commit)
          Gitaly::UserCreateBranchResponse.new(branch: branch)
        rescue Gitlab::Git::Repository::InvalidRef, Gitlab::Git::CommitError => ex
          raise GRPC::FailedPrecondition.new(ex.message)
        rescue Gitlab::Git::HooksService::PreReceiveError => ex
          return Gitaly::UserCreateBranchResponse.new(pre_receive_error: ex.message)
        end
      end
    end

    def user_delete_branch(request, call)
      bridge_exceptions do
        begin
          repo = Gitlab::Git::Repository.from_gitaly(request.repository, call)
          user = Gitlab::Git::User.from_gitaly(request.user)

          repo.rm_branch(request.branch_name, user: user)

          Gitaly::UserDeleteBranchResponse.new
        rescue Gitlab::Git::HooksService::PreReceiveError => e
          Gitaly::UserDeleteBranchResponse.new(pre_receive_error: e.message)
        end
      end
    end

    def user_merge_branch(session, call)
      Enumerator.new do |y|
        bridge_exceptions do
          first_request = session.next

          repository = Gitlab::Git::Repository.from_gitaly(first_request.repository, call)
          user = Gitlab::Git::User.from_gitaly(first_request.user)
          source_sha = first_request.commit_id.dup
          target_branch = first_request.branch.dup
          message = first_request.message.dup

          result = repository.merge(user, source_sha, target_branch, message) do |commit_id|
            y << Gitaly::UserMergeBranchResponse.new(commit_id: commit_id)

            second_request = session.next
            unless second_request.apply
              raise GRPC::FailedPrecondition.new('merge aborted by client')
            end
          end

          y << Gitaly::UserMergeBranchResponse.new(branch_update: branch_update_result(result))
        end
      end
    end

    def user_ff_branch(request, call)
      bridge_exceptions do
        begin
          repo = Gitlab::Git::Repository.from_gitaly(request.repository, call)
          user = Gitlab::Git::User.from_gitaly(request.user)

          result = repo.ff_merge(user, request.commit_id, request.branch)
          branch_update = branch_update_result(result)

          Gitaly::UserFFBranchResponse.new(branch_update: branch_update)
        rescue Gitlab::Git::CommitError => e
          raise GRPC::FailedPrecondition.new(e.to_s)
        rescue ArgumentError => e
          raise GRPC::InvalidArgument.new(e.to_s)
        rescue Gitlab::Git::HooksService::PreReceiveError => e
          Gitaly::UserFFBranchResponse.new(pre_receive_error: e.message)
        end
      end
    end

    def user_cherry_pick(request, call)
      bridge_exceptions do
        begin
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
            start_repository: start_repository
          )

          branch_update = branch_update_result(result)
          Gitaly::UserCherryPickResponse.new(branch_update: branch_update)
        rescue Gitlab::Git::Repository::CreateTreeError => e
          Gitaly::UserCherryPickResponse.new(create_tree_error: e.message)
        rescue Gitlab::Git::CommitError => e
          Gitaly::UserCherryPickResponse.new(commit_error: e.message)
        rescue Gitlab::Git::HooksService::PreReceiveError => e
          Gitaly::UserCherryPickResponse.new(pre_receive_error: e.message)
        end
      end
    end

    def user_revert(request, call)
      bridge_exceptions do
        begin
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
            start_repository: start_repository
          )

          branch_update = branch_update_result(result)
          Gitaly::UserRevertResponse.new(branch_update: branch_update)
        rescue Gitlab::Git::Repository::CreateTreeError => e
          Gitaly::UserRevertResponse.new(create_tree_error: e.message)
        rescue Gitlab::Git::CommitError => e
          Gitaly::UserRevertResponse.new(commit_error: e.message)
        rescue Gitlab::Git::HooksService::PreReceiveError => e
          Gitaly::UserRevertResponse.new(pre_receive_error: e.message)
        end
      end
    end

    def user_rebase(request, call)
      bridge_exceptions do
        begin
          repo = Gitlab::Git::Repository.from_gitaly(request.repository, call)
          user = Gitlab::Git::User.from_gitaly(request.user)
          remote_repository = Gitlab::Git::GitalyRemoteRepository.new(request.remote_repository, call)
          rebase_sha = repo.rebase(user, request.rebase_id,
                                   branch: request.branch,
                                   branch_sha: request.branch_sha,
                                   remote_repository: remote_repository,
                                   remote_branch: request.remote_branch)

          Gitaly::UserRebaseResponse.new(rebase_sha: rebase_sha)
        rescue Gitlab::Git::HooksService::PreReceiveError => e
          return Gitaly::UserRebaseResponse.new(pre_receive_error: e.message)
        rescue Gitlab::Git::Repository::GitError => e
          return Gitaly::UserRebaseResponse.new(git_error: e.message)
        end
      end
    end

    def user_commit_files(call)
      bridge_exceptions do
        begin
          actions = []
          request_enum = call.each_remote_read
          header = request_enum.next.header

          loop do
            action = request_enum.next.action

            if action.header
              actions << commit_files_action_from_gitaly_request(action.header)
            else
              actions.last[:content] << action.content
            end
          end

          repo = Gitlab::Git::Repository.from_gitaly(header.repository, call)
          user = Gitlab::Git::User.from_gitaly(header.user)
          opts = commit_files_opts(call, header, actions)

          branch_update = branch_update_result(repo.multi_action(user, opts))

          Gitaly::UserCommitFilesResponse.new(branch_update: branch_update)
        rescue Gitlab::Git::Index::IndexError => e
          Gitaly::UserCommitFilesResponse.new(index_error: e.message)
        rescue Gitlab::Git::HooksService::PreReceiveError => e
          Gitaly::UserCommitFilesResponse.new(pre_receive_error: e.message)
        end
      end
    end

    def user_squash(request, call)
      bridge_exceptions do
        repo = Gitlab::Git::Repository.from_gitaly(request.repository, call)
        user = Gitlab::Git::User.from_gitaly(request.user)
        author = Gitlab::Git::User.from_gitaly(request.author)

        begin
          squash_sha = repo.squash(user, request.squash_id,
                                   branch: request.branch,
                                   start_sha: request.start_sha,
                                   end_sha: request.end_sha,
                                   author: author,
                                   message: request.commit_message)

          Gitaly::UserSquashResponse.new(squash_sha: squash_sha)
        rescue Gitlab::Git::Repository::GitError => e
          Gitaly::UserSquashResponse.new(git_error: e.message)
        end
      end
    end

    private

    def commit_files_opts(call, header, actions)
      opts = {
        branch_name: header.branch_name,
        message: header.commit_message.b,
        actions: actions
      }

      if header.start_repository
        opts[:start_repository] = Gitlab::Git::GitalyRemoteRepository.new(header.start_repository, call)
      end

      optional_fields = {
        start_branch_name: 'start_branch_name',
        author_name: 'commit_author_name',
        author_email: 'commit_author_email'
      }.transform_values { |v| header[v].presence }

      opts.merge(optional_fields)
    end

    def commit_files_action_from_gitaly_request(header)
      {
        action: header.action.downcase,
        file_path: header.file_path,
        previous_path: header.previous_path,
        encoding: header.base64_content ? 'base64' : '',
        content: ''
      }
    end

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
