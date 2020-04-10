require 'spec_helper'

GITALY_RUBY_DIR = File.expand_path('../../..', __dir__).freeze
TMP_DIR_NAME = 'tmp'.freeze
TMP_DIR = File.join(GITALY_RUBY_DIR, TMP_DIR_NAME).freeze
GITLAB_SHELL_DIR = File.join(TMP_DIR, 'gitlab-shell').freeze

# overwrite GIT_CONFIG so user .gitconfig doesn't influence tests
ENV["GIT_CONFIG"] = File.join(File.dirname(__FILE__), "/testdata/home/.gitconfig")

module GitlabShellHelper
  def self.setup_gitlab_shell
    Gitlab.config.gitlab_shell.test_global_ivar_override(:path, GITLAB_SHELL_DIR)

    FileUtils.mkdir_p([TMP_DIR, File.join(GITLAB_SHELL_DIR, 'hooks')])
  end
end
