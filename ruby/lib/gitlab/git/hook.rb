# frozen_string_literal: true

module Gitlab
  module Git
    class Hook
      def self.directory
        Gitlab.config.git.hooks_directory
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

      def trigger(gl_id, gl_username, oldrev, newrev, ref, push_options: nil, transaction: nil)
        return [true, nil] unless exists?

        Bundler.with_unbundled_env do
          case name
          when "pre-receive", "post-receive"
            call_receive_hook(gl_id, gl_username, oldrev, newrev, ref, push_options, transaction)
          when "reference-transaction"
            call_reference_transaction_hook(gl_id, gl_username, oldrev, newrev, ref, transaction)
          when "update"
            call_update_hook(gl_id, gl_username, oldrev, newrev, ref, transaction)
          end
        end
      end

      private

      def call_stdin_hook(args, input, env)
        exit_status = false
        exit_message = nil

        options = {
          chdir: repo_path
        }

        Open3.popen3(env, path, *args, options) do |stdin, stdout, stderr, wait_thr|
          exit_status = true
          stdin.sync = true

          # in git, hooks may just exit without reading stdin. We catch the
          # exception to avoid a broken pipe warning
          begin
            input.lines do |line|
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

      def call_receive_hook(gl_id, gl_username, oldrev, newrev, ref, push_options, transaction)
        changes = [oldrev, newrev, ref].join(" ")

        vars = env_base_vars(gl_id, gl_username, transaction)
        vars.merge!(push_options.env_data) if push_options

        call_stdin_hook([], changes, vars)
      end

      def call_reference_transaction_hook(gl_id, gl_username, oldrev, newrev, ref, transaction)
        changes = [oldrev, newrev, ref].join(" ")

        vars = env_base_vars(gl_id, gl_username, transaction)

        call_stdin_hook(["prepared"], changes, vars)
      end

      def call_update_hook(gl_id, gl_username, oldrev, newrev, ref, transaction)
        options = {
          chdir: repo_path
        }

        args = [ref, oldrev, newrev]

        vars = env_base_vars(gl_id, gl_username, transaction)

        stdout, stderr, status = Open3.capture3(vars, path, *args, options)
        [status.success?, stderr.presence || stdout]
      end

      def retrieve_error_message(stderr, stdout)
        err_message = stderr.read
        err_message = err_message.blank? ? stdout.read : err_message
        err_message
      end

      def hooks_payload(gl_id, gl_username, transaction)
        payload = {
          repository: repository.gitaly_repository.to_json,
          binary_directory: Gitlab.config.gitaly.bin_dir,
          internal_socket: Gitlab.config.gitaly.internal_socket,
          internal_socket_token: ENV['GITALY_TOKEN'],
          receive_hooks_payload: {
            userid: gl_id,
            username: gl_username,
            protocol: GL_PROTOCOL
          }
        }

        payload.merge!(transaction.payload) if transaction

        Base64.strict_encode64(payload.to_json)
      end

      def env_base_vars(gl_id, gl_username, transaction = nil)
        {
          'GITALY_HOOKS_PAYLOAD' => hooks_payload(gl_id, gl_username, transaction),
          'GITALY_LOG_DIR' => Gitlab.config.logging.dir,
          'GITALY_BIN_DIR' => Gitlab.config.gitaly.bin_dir,
          'PWD' => repo_path,
          'GIT_DIR' => repo_path
        }
      end
    end
  end
end
