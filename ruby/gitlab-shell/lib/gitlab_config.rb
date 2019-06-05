require 'yaml'

class GitlabConfig
  def secret_file
    fetch_from_legacy_config('secret_file',File.join(ROOT_PATH, '.gitlab_shell_secret'))
  end

  # Pass a default value because this is called from a repo's context; in which
  # case, the repo's hooks directory should be the default.
  #
  def custom_hooks_dir(default: nil)
    fetch_from_legacy_config('custom_hooks_dir', File.join(ROOT_PATH, 'hooks'))
  end

  def gitlab_url
    fetch_from_legacy_config('gitlab_url',"http://localhost:8080").sub(%r{/*$}, '')
  end

  def http_settings
    fetch_from_legacy_config('http_settings', {})
  end

  def log_file
    return File.join(LOG_PATH, 'gitlab-shell.log') unless LOG_PATH.empty?

    fetch_from_legacy_config('log_file', File.join(ROOT_PATH, 'gitlab-shell.log'))
  end

  def log_level
    return LOG_LEVEL unless LOG_LEVEL.empty?

    fetch_from_legacy_config('log_level', 'INFO')
  end

  def log_format
    return LOG_FORMAT unless LOG_FORMAT.empty?

    fetch_from_legacy_config('log_format', 'text')
  end

  def metrics_log_file
    fetch_from_legacy_config('metrics_log_file', File.join(ROOT_PATH, 'gitlab-shell-metrics.log'))
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
      metrics_log_file: metrics_log_file
    }.to_json
  end

  def fetch_from_legacy_config(key, default)
    legacy_config.fetch(key, default)
  end

  private

  def legacy_config
    # TODO: deprecate @old_config that is parsing the gitlab-shell config.yml
    @old_config ||= YAML.load_file(File.join(ROOT_PATH, 'config.yml'))
  end
end
