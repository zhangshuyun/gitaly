# External dependencies of Gitlab::Git
require 'rugged'
require 'linguist'

# Ruby on Rails mix-ins that GitLab::Git code relies on
require 'active_support/core_ext/object/blank'
require 'active_support/core_ext/numeric/bytes'
require 'active_support/core_ext/module/delegation'
require 'active_support/core_ext/enumerable'

# We split our mock implementation of Gitlab::GitalyClient into a separate file
require_relative 'gitaly_client.rb'

vendor_gitlab_git = '../../vendor/gitlab_git/'

# Some later requires are order-sensitive. Manually require whatever we need.
require_relative File.join(vendor_gitlab_git, 'lib/gitlab/encoding_helper.rb')
require_relative File.join(vendor_gitlab_git, 'lib/gitlab/git.rb')
require_relative File.join(vendor_gitlab_git, 'lib/gitlab/git/ref.rb')

# Require all .rb files we can find in the vendored gitlab/git directory
dir = File.expand_path(File.join('..', vendor_gitlab_git, 'lib/gitlab/'), __FILE__)
Dir["#{dir}/git/**/*.rb"].each do |ruby_file|
  require_relative ruby_file.sub(dir, File.join(vendor_gitlab_git, 'lib/gitlab/')).sub(%r{^/*}, '')
end

module Gitlab
  # Config lets Gitlab::Git do mock config lookups.
  class Config
    class Git
      def bin_path
        ENV['GITALY_RUBY_GIT_BIN_PATH']
      end
    end

    def git
      Git.new
    end
  end

  def self.config
    Config.new
  end
end

module Gitlab
  module Git
    class Repository
      def self.from_call(_call)
        new(GitalyServer.repo_path(_call))
      end

      def initialize(path)
        @path = path
        @rugged = Rugged::Repository.new(path)
        @attributes = Gitlab::Git::Attributes.new(path)
      end

      # Bypass the CircuitBreaker class which needs Redis
      def rugged
        @rugged
      end
    end
  end
end
