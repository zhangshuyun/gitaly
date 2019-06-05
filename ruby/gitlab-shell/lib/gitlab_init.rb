ROOT_PATH = ENV.fetch('GITALY_GITLAB_SHELL_DIR', File.expand_path('..', __dir__))
LOG_PATH = ENV.fetch('GITALY_LOG_DIR', "")
LOG_LEVEL = ENV.fetch('GITALY_LOG_LEVEL', "")
LOG_FORMAT = ENV.fetch('GITALY_LOG_FORMAT', "")

# We are transitioning parts of gitlab-shell into the gitaly project. In
# gitaly, GITALY_EMBEDDED will be true.
GITALY_EMBEDDED = true

require_relative 'gitlab_config'
