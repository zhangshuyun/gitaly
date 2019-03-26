# frozen_string_literal: true

module Gitlab
  module Git
    class Hook
      def self.directory
        Gitlab.config.git.hooks_directory
      end

      def self.legacy_hooks_directory
        File.join(Gitlab.config.gitlab_shell.path, 'hooks')
      end

      GL_PROTOCOL = 'web'
      ERROR_LOG_FORMAT = '%s hook error in repository %s: %s'

      attr_reader :name, :path, :repository

      def initialize(name, repository)
        @name = name
        @repository = repository
        @path = File.join(self.class.directory, name)
      end

      def repo_path
        repository.path
      end

      def exists?
        File.exist?(path)
      end

      def trigger(gl_id, gl_username, oldrev, newrev, ref)
        return [true, nil] unless exists?

        Bundler.with_clean_env do
          case name
          when "pre-receive", "post-receive"
            call_receive_hook(gl_id, gl_username, oldrev, newrev, ref)
          when "update"
            call_update_hook(gl_id, gl_username, oldrev, newrev, ref)
          end
        end
      end

      private

      def call_receive_hook(gl_id, gl_username, oldrev, newrev, ref)
        vars = env_base_vars(gl_id, gl_username)
        options = {
          chdir: repo_path,
          stdin_data: [oldrev, newrev, ref].join(' ')
        }

        stdout, stderr, exit_status = Open3.capture3(vars, path, options)

        log_errors(stderr)

        exit_message = retrieve_output(stdout, stderr, exit_status)

        [exit_status.success?, exit_message]
      end

      def call_update_hook(gl_id, gl_username, oldrev, newrev, ref)
        vars = env_base_vars(gl_id, gl_username)
        args = [ref, oldrev, newrev]
        options = { chdir: repo_path }

        stdout, stderr, exit_status = Open3.capture3(vars, path, *args, options)

        log_errors(stderr)

        exit_message = retrieve_output(stdout, stderr, exit_status)

        [exit_status.success?, exit_message]
      end

      def retrieve_output(stdout, stderr, exit_status)
        return if exit_status.success?

        (stdout + stderr).strip
      end

      def log_errors(errors)
        return unless errors.present?

        errors.split("\n").each do |error|
          message = format(
            ERROR_LOG_FORMAT,
            name,
            repository.relative_path,
            error
          )

          Gitlab::GitLogger.error(message)
        end
      end

      def env_base_vars(gl_id, gl_username)
        {
          'GITLAB_SHELL_DIR' => Gitlab.config.gitlab_shell.path,
          'GL_ID' => gl_id,
          'GL_USERNAME' => gl_username,
          'GL_REPOSITORY' => repository.gl_repository,
          'GL_PROTOCOL' => GL_PROTOCOL,
          'PWD' => repo_path,
          'GIT_DIR' => repo_path
        }
      end
    end
  end
end
