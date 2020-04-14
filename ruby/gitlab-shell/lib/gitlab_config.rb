require 'yaml'
require 'json'

class GitlabConfig
  def secret_file
    fetch_from_config('secret_file', fetch_from_legacy_config('secret_file', File.join(ROOT_PATH, '.gitlab_shell_secret')))
  end

  # Pass a default value because this is called from a repo's context; in which
  # case, the repo's hooks directory should be the default.
  #
  def custom_hooks_dir(default: nil)
    fetch_from_config('custom_hooks_dir', fetch_from_legacy_config('custom_hooks_dir', File.join(ROOT_PATH, 'hooks')))
  end

  def gitlab_url
    fetch_from_config('gitlab_url', fetch_from_legacy_config('gitlab_url',"http://localhost:8080").sub(%r{/*$}, ''))
  end

  def http_settings
    fetch_from_config('http_settings', fetch_from_legacy_config('http_settings', {}))
  end

  def log_file
    log_path = fetch_from_config('log_path', LOG_PATH)

    return File.join(log_path, 'gitlab-shell.log') unless log_path.empty?

    File.join(ROOT_PATH, 'gitlab-shell.log')
  end

  def log_level
    log_level = fetch_from_config('log_level', LOG_LEVEL)

    log_level = LOG_LEVEL if log_level.empty?

    return log_level unless log_level.empty?

    'INFO'
  end

  def log_format
    log_format = fetch_from_config('log_format', LOG_FORMAT)

    return log_format unless log_format.empty?

    'text'
  end

  def to_json
    {
      secret_file: secret_file,
      custom_hooks_dir: custom_hooks_dir,
      gitlab_url: gitlab_url,
      http_settings: http_settings,
      log_file: log_file,
      log_level: log_level,
      log_format: log_format,
    }.to_json
  end

  def fetch_from_legacy_config(key, default)
    legacy_config[key] || default
  end

  private

  def fetch_from_config(key, default='')
    value = config[key]
    return value unless value.nil? || value.empty?

    default
  end

  def config
    @config ||= JSON.parse(ENV.fetch('GITALY_GITLAB_SHELL_CONFIG', '{}'))
  end

  def legacy_config
    # TODO: deprecate @legacy_config that is parsing the gitlab-shell config.yml
    legacy_file = File.join(ROOT_PATH, 'config.yml')
    return {} unless File.exist?(legacy_file)

    @legacy_config ||= YAML.load_file(legacy_file)
  end
end
