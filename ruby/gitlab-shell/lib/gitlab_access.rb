require_relative 'gitlab_init'
require_relative 'gitlab_net'
require_relative 'gitlab_access_status'
require_relative 'gitlab_metrics'
require_relative 'object_dirs_helper'
require 'json'

class GitlabAccess
  class AccessDeniedError < StandardError; end

  MAX_NUMBER_OF_REFS = 1000

  attr_reader :config, :gl_repository, :repo_path, :changes, :protocol

  def initialize(gl_repository, repo_path, gl_id, changes, protocol)
    @config = GitlabConfig.new
    @gl_repository = gl_repository
    @repo_path = repo_path.strip
    @gl_id = gl_id
    @changes = changes.lines
    @protocol = protocol
  end

  def exec
    validate_refs_size!

    status = GitlabMetrics.measure('check-access:git-receive-pack') do
      api.check_access('git-receive-pack', @gl_repository, @repo_path, @gl_id, @changes, @protocol, env: ObjectDirsHelper.all_attributes.to_json)
    end

    raise AccessDeniedError, status.message unless status.allowed?

    true
  rescue GitlabNet::ApiUnreachableError
    $stderr.puts "GitLab: Failed to authorize your Git request: internal API unreachable"
    false
  rescue AccessDeniedError => ex
    $stderr.puts "GitLab: #{ex.message}"
    false
  end

  protected

  def api
    GitlabNet.new
  end

  private

  def validate_refs_size!
    return if changes.size <= MAX_NUMBER_OF_REFS

    raise AccessDeniedError, 'Exceeded the max number of allowed refs to push'
  end
end
