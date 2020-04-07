require 'net/http'
require 'openssl'
require 'json'

require_relative 'gitlab_config'
require_relative 'gitlab_access'
require_relative 'http_helper'

class GitlabNet # rubocop:disable Metrics/ClassLength
  include HTTPHelper

  CHECK_TIMEOUT = 5
  API_INACCESSIBLE_MESSAGE = 'API is not accessible'.freeze

  def check_access(cmd, gl_repository, repo, gl_id, changes, protocol, env: {})
    changes = changes.join("\n") unless changes.is_a?(String)

    params = {
      action: cmd,
      changes: changes,
      gl_repository: gl_repository,
      project: sanitize_path(repo),
      protocol: protocol,
      env: env
    }

    gl_id_sym, gl_id_value = self.class.parse_gl_id(gl_id)
    params[gl_id_sym] = gl_id_value

    url = "#{internal_api_endpoint}/allowed"
    resp = post(url, params)

    case resp
    when Net::HTTPSuccess, Net::HTTPMultipleChoices, Net::HTTPUnauthorized,
         Net::HTTPNotFound, Net::HTTPServiceUnavailable
      if resp.content_type == CONTENT_TYPE_JSON
        return GitAccessStatus.create_from_json(resp.body, resp.code)
      end
    end

    GitAccessStatus.new(false, resp.code, API_INACCESSIBLE_MESSAGE)
  end

  def check
    get("#{internal_api_endpoint}/check", options: { read_timeout: CHECK_TIMEOUT })
  end

  def post_receive(gl_repository, gl_id, changes, push_options)
    params = {
      gl_repository: gl_repository,
      identifier: gl_id,
      changes: changes,
      :'push_options[]' => push_options,	# rubocop:disable Style/HashSyntax
    }
    resp = post("#{internal_api_endpoint}/post_receive", params)

    raise NotFound if resp.code == '404'

    JSON.parse(resp.body) if resp.code == '200'
  end

  def pre_receive(gl_repository)
    resp = post("#{internal_api_endpoint}/pre_receive", gl_repository: gl_repository)

    raise NotFound if resp.code == '404'

    JSON.parse(resp.body) if resp.code == '200'
  end

  def self.parse_gl_id(gl_id)
    if gl_id.start_with?('key-')
      value = gl_id.gsub('key-', '')
      raise ArgumentError, "gl_id='#{gl_id}' is invalid!" unless value =~ /\A[0-9]+\z/
      [:key_id, value]
    elsif gl_id.start_with?('user-')
      value = gl_id.gsub('user-', '')
      raise ArgumentError, "gl_id='#{gl_id}' is invalid!" unless value =~ /\A[0-9]+\z/
      [:user_id, value]
    elsif gl_id.start_with?('username-')
      [:username, gl_id.gsub('username-', '')]
    else
      raise ArgumentError, "gl_id='#{gl_id}' is invalid!"
    end
  end

  protected

  def sanitize_path(repo)
    repo.delete("'")
  end
end
