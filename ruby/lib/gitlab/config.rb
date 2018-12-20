module Gitlab

  module SpecSetup
    def setup_ivar_for_specs(name, value)
      instance_variable_set("@#{name}".to_sym, value)
    end
  end

  class Config
    class Git
      include SpecSetup

      def bin_path
        @bin_path ||= ENV['GITALY_RUBY_GIT_BIN_PATH']
      end

      def hooks_directory
        @hooks_directory ||= ENV['GITALY_GIT_HOOKS_DIR']
      end

      def write_buffer_size
        @write_buffer_size ||= ENV['GITALY_RUBY_WRITE_BUFFER_SIZE'].to_i
      end

      def max_commit_or_tag_message_size
        @max_commit_or_tag_message_size ||= ENV['GITALY_RUBY_MAX_COMMIT_OR_TAG_MESSAGE_SIZE'].to_i
      end
    end

    class GitlabShell
      include SpecSetup

      def path
        @path ||= ENV['GITALY_RUBY_GITLAB_SHELL_PATH']
      end

      def git_timeout
        @git_timeout ||= 10800 # TODO make this configurable or eliminate otherwise https://gitlab.com/gitlab-org/gitaly/issues/885
      end
    end

    class Gitaly
      include SpecSetup

      def client_path
        @client_path ||= ENV['GITALY_RUBY_GITALY_BIN_DIR']
      end
    end

    def git
      @git ||= Git.new
    end

    def gitlab_shell
      @gitlab_shell ||= GitlabShell.new
    end

    def gitaly
      @gitaly ||= Gitaly.new
    end
  end

  def self.config
    @config ||= Config.new
  end
end
