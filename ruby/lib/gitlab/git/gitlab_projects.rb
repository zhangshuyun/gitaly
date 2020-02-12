require 'tempfile'

module Gitlab
  module Git
    class GitlabProjects
      include Gitlab::Git::Popen
      include Gitlab::Utils::StrongMemoize

      BRANCHES_PER_PUSH = 10

      # Relative path is a directory name for repository with .git at the end.
      # Example: gitlab-org/gitlab-test.git
      attr_reader :repository_relative_path

      # This is the path at which the gitlab-shell hooks directory can be found.
      # It's essential for integration between git and GitLab proper. All new
      # repositories should have their hooks directory symlinked here.
      attr_reader :global_hooks_path

      attr_reader :logger

      def self.from_gitaly(gitaly_repository, call)
        storage_path = GitalyServer.storage_path(call)

        Gitlab::Git::GitlabProjects.new(
          storage_path,
          gitaly_repository.relative_path,
          global_hooks_path: Gitlab::Git::Hook.directory,
          logger: Rails.logger
        )
      end

      def initialize(shard_path, repository_relative_path, global_hooks_path:, logger:)
        @shard_path = shard_path
        @repository_relative_path = repository_relative_path

        @logger = logger
        @global_hooks_path = global_hooks_path
        @output = StringIO.new
      end

      def shard_path
        @shard_path
      end

      def output
        io = @output.dup
        io.rewind
        io.read
      end

      # Absolute path to the repository.
      # Example: /home/git/repositorities/gitlab-org/gitlab-test.git
      # Probably will be removed when we fully migrate to Gitaly, part of
      # https://gitlab.com/gitlab-org/gitaly/issues/1124.
      def repository_absolute_path
        strong_memoize(:repository_absolute_path) do
          File.join(shard_path, repository_relative_path)
        end
      end

      def fetch_remote(name, timeout, force:, tags:, env: {}, prune: true)
        logger.info "Fetching remote #{name} for repository #{repository_absolute_path}."
        cmd = fetch_remote_command(name, tags, prune, force)

        run_with_timeout(cmd, timeout, repository_absolute_path, env).tap do |success|
          logger.error "Fetching remote #{name} for repository #{repository_absolute_path} failed." unless success
        end
      end

      def push_branches(remote_name, timeout, force, branch_names, env: {})
        branch_names.each_slice(BRANCHES_PER_PUSH) do |branches|
          logger.info "Pushing #{branches.count} branches from #{repository_absolute_path} to remote #{remote_name}"

          cmd = %W(#{Gitlab.config.git.bin_path} push)
          cmd << '--porcelain'
          cmd << '--force' if force
          cmd += %W(-- #{remote_name}).concat(branches)

          unless run_with_timeout(cmd, timeout, repository_absolute_path, env)
            logger.error("Pushing branches to remote #{remote_name} failed.")
            return false
          end
        end

        true
      end

      def delete_remote_branches(remote_name, branch_names, env: {})
        branch_names.each_slice(BRANCHES_PER_PUSH) do |branches|
          logger.info "Pushing #{branches.count} deleted branches from #{repository_absolute_path} to remote #{remote_name}"

          cmd = %W(#{Gitlab.config.git.bin_path} push -- #{remote_name})
          branches.each { |branch| cmd << ":#{branch}" }

          unless run(cmd, repository_absolute_path, env)
            logger.error("Pushing deleted branches to remote #{remote_name} failed.")
            return false
          end
        end

        true
      end

      protected

      def run(*args)
        output, exitstatus = popen(*args)
        @output << output

        exitstatus&.zero?
      end

      def run_with_timeout(*args)
        output, exitstatus = popen_with_timeout(*args)
        @output << output

        exitstatus&.zero?
      rescue Timeout::Error
        @output.puts('Timed out')

        false
      end

      def remove_origin_in_repo
        cmd = %W(#{Gitlab.config.git.bin_path} remote rm origin)
        run(cmd, repository_absolute_path)
      end

      private

      def fetch_remote_command(name, tags, prune, force)
        %W(#{Gitlab.config.git.bin_path} -c http.followRedirects=false fetch #{name} --quiet).tap do |cmd|
          cmd << '--prune' if prune
          cmd << '--force' if force
          cmd << (tags ? '--tags' : '--no-tags')
        end
      end
    end
  end
end
