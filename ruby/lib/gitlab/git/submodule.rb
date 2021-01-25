module Gitlab
  module Git
    class Submodule
      attr_reader :user, :repository, :submodule_path, :branch_name

      def initialize(user, repository, submodule_path, branch_name)
        @user = user
        @repository = repository
        @branch_name = branch_name

        begin
          @submodule_path = Gitlab::Git::PathHelper.normalize_path!(submodule_path.dup)
        rescue Gitlab::Git::PathHelper::InvalidPath => e
          raise ArgumentError, e.message
        end
      end

      def update(commit_sha, message, timestamp = nil)
        validate!

        OperationService.new(user, repository).with_branch(branch_name, start_branch_name: branch_name) do
          committer = repository.user_to_committer(user, timestamp)

          repository.update_submodule(submodule_path, commit_sha, branch_name, committer, message)
        end
      end

      private

      def validate!
        raise ArgumentError, 'User cannot be empty' unless user
        raise ArgumentError, 'Submodule can not be empty' unless submodule_path.present?
        raise CommitError, 'Repository is empty' if repository.empty?
      end
    end
  end
end
