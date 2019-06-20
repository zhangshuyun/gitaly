module Gitlab
  module Git
    #
    # When a Gitaly call involves two repositories instead of one we cannot
    # assume that both repositories are on the same Gitaly server. In this
    # case we need to make a distinction between the repository that the
    # call is being made on (a Repository instance), and the "other"
    # repository (a RemoteRepository instance). This is the reason why we
    # have the RemoteRepository class in Gitlab::Git.
    class RemoteRepository
      attr_reader :relative_path, :gitaly_repository

      def initialize(repository)
        @relative_path = repository.relative_path
        @gitaly_repository = repository.gitaly_repository

        @repository = repository
      end

      def empty?
        !@repository.exists? || @repository.empty?
      end

      def commit_id(revision)
        @repository.commit(revision)&.sha
      end

      def branch_exists?(name)
        @repository.branch_exists?(name)
      end

      # Compares self to a Gitlab::Git::Repository
      def same_repository?(other_repository)
        gitaly_repository.storage_name == other_repository.storage &&
          gitaly_repository.relative_path == other_repository.relative_path
      end

      def fetch_env
        gitaly_ssh = File.absolute_path(File.join(Gitlab.config.gitaly.bin_dir, 'gitaly-ssh'))
        gitaly_address = gitaly_client.address(storage)
        gitaly_token = gitaly_client.token(storage)

        request = Gitaly::SSHUploadPackRequest.new(repository: gitaly_repository)
        env = {
          'GITALY_ADDRESS' => gitaly_address,
          'GITALY_PAYLOAD' => request.to_json,
          'GITALY_WD' => Dir.pwd,
          'GIT_SSH_COMMAND' => "#{gitaly_ssh} upload-pack"
        }
        env['GITALY_TOKEN'] = gitaly_token if gitaly_token.present?

        env
      end

      def path
        @repository.path
      end

      private

      # Must return an object that responds to 'address' and 'storage'.
      def gitaly_client
        raise NotImplementedError.new("Can't perform remote operations on superclass")
      end

      def storage
        gitaly_repository.storage_name
      end
    end
  end
end
