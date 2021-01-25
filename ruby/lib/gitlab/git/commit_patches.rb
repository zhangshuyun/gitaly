module Gitlab
  module Git
    class CommitPatches
      attr_reader :user, :repository, :branch_name, :patches

      def initialize(user, repository, branch_name, patches, timestamp = nil)
        @user = user
        @branch_name = branch_name
        @patches = patches
        @repository = repository
        @timestamp = timestamp
      end

      def commit
        start_point = repository.find_branch(branch_name)&.target || repository.root_ref

        OperationService.new(user, repository).with_branch(branch_name) do
          env = user.git_env
          if @timestamp
            env = env.merge(
              {
                'GIT_AUTHOR_DATE' => "#{@timestamp.seconds} +0000",
                'GIT_COMMITTER_DATE' => "#{@timestamp.seconds} +0000"
              }
            )
          end

          repository.commit_patches(start_point, patches, extra_env: env)
        end
      end
    end
  end
end
