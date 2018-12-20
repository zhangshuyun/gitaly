require_relative '../lib/gitaly_server.rb'
require_relative '../lib/gitlab/git.rb'
require_relative 'support/sentry.rb'
require 'timecop'
require 'test_repo_helper'
require 'rspec-parameterized'
require 'factory_bot'

# Load these helpers first, since they're required by other helpers
require File.join(__dir__, 'support/helpers/gitlab_shell_helper.rb')

Dir[File.join(__dir__, 'support/helpers/*.rb')].each { |f| require f }

Gitlab.config.git.setup_ivar_for_specs(:bin_path, 'git')
Gitlab.config.git.setup_ivar_for_specs(:hooks_directory, File.join(Gitlab.config.gitlab_shell.path.to_s, "hooks"))
Gitlab.config.gitaly.setup_ivar_for_specs(:client_path, __dir__)

RSpec.configure do |config|
  config.include FactoryBot::Syntax::Methods

  config.before(:suite) do
    FactoryBot.find_definitions
  end
end
