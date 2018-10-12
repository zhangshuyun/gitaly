require 'spec_helper'

GITALY_RUBY_DIR = File.expand_path('../../../..', __FILE__)
TMP_DIR = File.join(GITALY_RUBY_DIR, 'tmp')
GITLAB_SHELL_DIR = File.join(TMP_DIR, 'gitlab-shell')

module GitlabShellHelper
  def self.setup_gitlab_shell
    ENV['GITALY_RUBY_GITLAB_SHELL_PATH'] = GITLAB_SHELL_DIR

    FileUtils.mkdir_p([TMP_DIR, File.join(GITLAB_SHELL_DIR, 'hooks')])
  end
end
