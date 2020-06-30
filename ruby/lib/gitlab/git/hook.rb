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

      def trigger(gl_id, gl_username, oldrev, newrev, ref, push_options: nil)
        return [true, nil] unless exists?

        Bundler.with_clean_env do
          case name
          when "pre-receive", "post-receive"
            call_receive_hook(gl_id, gl_username, oldrev, newrev, ref, push_options)
          when "update"
            call_update_hook(gl_id, gl_username, oldrev, newrev, ref)
          end
        end
      end

      private

      def call_receive_hook(gl_id, gl_username, oldrev, newrev, ref, push_options)
        changes = [oldrev, newrev, ref].join(" ")

        exit_status = false
        exit_message = nil

        vars = env_base_vars(gl_id, gl_username)
        vars.merge!(push_options.env_data) if push_options

        options = {
          chdir: repo_path
        }

        Open3.popen3(vars, path, options) do |stdin, stdout, stderr, wait_thr|
          exit_status = true
          stdin.sync = true

          # in git, pre- and post- receive hooks may just exit without
          # reading stdin. We catch the exception to avoid a broken pipe
          # warning
          begin
            # inject all the changes as stdin to the hook
            changes.lines do |line|
              stdin.puts line
            end
          rescue Errno::EPIPE
          end

          stdin.close

          unless wait_thr.value == 0
            exit_status = false
            exit_message = retrieve_error_message(stderr, stdout)
          end
        end

        [exit_status, exit_message]
      end

      def call_update_hook(gl_id, gl_username, oldrev, newrev, ref)
        options = {
          chdir: repo_path
        }

        args = [ref, oldrev, newrev]

        vars = env_base_vars(gl_id, gl_username)

        stdout, stderr, status = Open3.capture3(vars, path, *args, options)
        [status.success?, stderr.presence || stdout]
      end

      def retrieve_error_message(stderr, stdout)
        err_message = stderr.read
        err_message = err_message.blank? ? stdout.read : err_message
        err_message
      end

      def env_base_vars(gl_id, gl_username)
        {
          'GITALY_GITLAB_SHELL_DIR' => Gitlab.config.gitlab_shell.path,
          'GITLAB_SHELL_DIR' => Gitlab.config.gitlab_shell.path,
          'GITALY_LOG_DIR' => Gitlab.config.logging.dir,
          'GITALY_LOG_LEVEL' => Gitlab.config.logging.level,
          'GITALY_LOG_FORMAT' => Gitlab.config.logging.format,
          'GITALY_RUBY_DIR' => Gitlab.config.gitaly.ruby_dir,
          'GITALY_BIN_DIR' => Gitlab.config.gitaly.bin_dir,
          'GL_ID' => gl_id,
          'GL_USERNAME' => gl_username,
          'GL_REPOSITORY' => repository.gl_repository,
          'GL_PROJECT_PATH' => repository.gl_project_path,
          'GL_PROTOCOL' => GL_PROTOCOL,
          'PWD' => repo_path,
          'GIT_DIR' => repo_path,
          'GITALY_REPO' => repository.gitaly_repository.to_json,
          'GITALY_SOCKET' => Gitlab.config.gitaly.internal_socket,
          'GITALY_GO_UPDATE' => repository.feature_enabled?('go-update-hook').to_s,
          'GITALY_GO_PRERECEIVE' => repository.feature_enabled?('go-prereceive-hook').to_s
        }
      end
    end
  end
end
