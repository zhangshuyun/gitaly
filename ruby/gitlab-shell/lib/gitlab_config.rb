class GitlabConfig
  def secret_file
    fetch_from_config('secret_file',  File.join(gitlab_shell_dir, '.gitlab_shell_secret'))
  end

  # Pass a default value because this is called from a repo's context; in which
  # case, the repo's hooks directory should be the default.
  #
  def gitlab_shell_dir
    fetch_from_config('dir', File.dirname(__dir__))
  end

  def custom_hooks_dir(default: nil)
    fetch_from_config('custom_hooks_dir', File.join(gitlab_shell_dir, 'hooks'))
  end

  def gitlab_url
    fetch_from_config('gitlab_url', "http://localhost:8080").sub(%r{/*$}, '')
  end

  class HTTPSettings
    DEFAULT_TIMEOUT = 300

    attr_reader :settings
    def initialize(settings)
      @settings = settings || {}
    end

    def user
      fetch_from_settings('user')
    end

    def password
      fetch_from_settings('password')
    end

    def read_timeout
      read_timeout = fetch_from_settings('read_timeout').to_i

      return read_timeout unless read_timeout == 0

      DEFAULT_TIMEOUT
    end

    def ca_file
      fetch_from_settings('ca_file')
    end

    def ca_path
      fetch_from_settings('ca_path')
    end

    def self_signed_cert
      fetch_from_settings('self_signed_cert')
    end

    private

    def fetch_from_settings(key)
      settings[key]
    end
  end

  def http_settings
    @http_settings ||= GitlabConfig::HTTPSettings.new(fetch_from_config('http_settings', {}))
  end

  def log_file
    File.join(fetch_from_config('log_path', gitlab_shell_dir), 'gitlab-shell.log')
  end

  def log_level
    fetch_from_config('log_level', 'INFO')
  end

  def log_format
    fetch_from_config('log_format', 'text')
  end

  def to_json
    {
      secret_file: secret_file,
      custom_hooks_dir: custom_hooks_dir,
      gitlab_url: gitlab_url,
      http_settings: http_settings.settings,
      log_file: log_file,
      log_level: log_level,
      log_format: log_format,
      gitlab_shell_dir: gitlab_shell_dir,
    }.to_json
  end

  private

  def fetch_from_config(key, default)
    value = config[key]

    return default if value.nil? || value.empty?

    value
  end

  def config
    @config ||= JSON.parse(ENV.fetch('GITALY_GITLAB_SHELL_CONFIG', '{}'))
  end
end
