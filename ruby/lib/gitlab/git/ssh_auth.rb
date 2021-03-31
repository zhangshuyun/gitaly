require 'shellwords'

module Gitlab
  module Git
    # SshAuth writes custom identity and known_hosts files to temporary files
    # and builds a `GIT_SSH_COMMAND` environment variable to allow git
    # operations over SSH to take advantage of them.
    #
    # To use:
    #     SshAuth.from_gitaly(request).setup do |env|
    #       # Run commands here with the provided environment
    #     end
    class SshAuth
      class Option
        def initialize(key, value)
          if key.include?('=') || needs_escape?(key)
            raise ArgumentError, "invalid SSH config key: #{key.inspect}"
          end

          if needs_escape?(value)
            raise ArgumentError, "invalid SSH config value: #{value.inspect}"
          end

          @key = key
          @value = value
        end

        def to_s
          "-o#{@key}=#{@value}"
        end

        private

        def needs_escape?(str)
          Shellwords.shellescape(str) != str
        end
      end

      attr_reader :ssh_key, :known_hosts

      def self.from_gitaly(request)
        new(request.ssh_key, request.known_hosts)
      end

      def initialize(ssh_key, known_hosts)
        @ssh_key = ssh_key
        @known_hosts = known_hosts
      end

      def setup
        options = []

        if ssh_key.present?
          key_file = write_tempfile('gitlab-shell-key-file', 0o400, ssh_key)

          options << Option.new('IdentityFile', key_file.path)
          options << Option.new('IdentitiesOnly', 'yes')
        end

        if known_hosts.present?
          known_hosts_file = write_tempfile('gitlab-shell-known-hosts', 0o400, known_hosts)

          options << Option.new('StrictHostKeyChecking', 'yes')
          options << Option.new('CheckHostIP', 'no')
          options << Option.new('UserKnownHostsFile', known_hosts_file.path)
        end

        yield custom_environment(options)
      ensure
        key_file&.close!
        known_hosts_file&.close!
      end

      private

      def write_tempfile(name, mode, data)
        Tempfile.open(name) do |tempfile|
          tempfile.chmod(mode)
          tempfile.write(data)

          # Return the tempfile instance so it can be unlinked
          tempfile
        end
      end

      # Constructs an environment that will make SSH, as invoked by git, respect
      # the given options. To achieve this, we use the GIT_SSH_COMMAND envvar.
      def custom_environment(options)
        return {} unless options.present?

        { 'GIT_SSH_COMMAND' => %(ssh #{options.join(' ')}) }
      end
    end
  end
end
