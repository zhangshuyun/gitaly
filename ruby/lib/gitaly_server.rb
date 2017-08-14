require 'gitaly'

require_relative 'gitlab/git.rb'

require_relative 'gitaly_server/commit_service.rb'

module GitalyServer
  REPO_PATH_HEADER = 'gitaly-repo-path'.freeze

  def self.repo_path(_call)
    _call.metadata.fetch(REPO_PATH_HEADER)
  end

  def self.register_handlers(server)
    server.handle(CommitService.new)
  end
end
