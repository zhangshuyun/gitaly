require_relative '../lib/gitaly_server.rb'
require_relative '../lib/gitlab/git.rb'
require_relative 'support/sentry.rb'
require 'timecop'
require 'rspec-parameterized'
require 'factory_bot'
require 'pry'

GITALY_RUBY_DIR = File.expand_path('..', __dir__).freeze
TMP_DIR = Dir.mktmpdir('gitaly-ruby-tests-').freeze
GITLAB_SHELL_DIR = File.join(TMP_DIR, 'gitlab-shell').freeze

# overwrite HOME env variable so user global .gitconfig doesn't influence tests
ENV["HOME"] = File.join(File.dirname(__FILE__), "/support/helpers/testdata/home")

require 'test_repo_helper'

Dir[File.join(__dir__, 'support/helpers/*.rb')].each { |f| require f }

RSpec.configure do |config|
  config.include FactoryBot::Syntax::Methods

  config.before(:suite) do
    FactoryBot.find_definitions
  end

  config.after(:suite) do
    FileUtils.rm_rf(TMP_DIR)
  end
end
