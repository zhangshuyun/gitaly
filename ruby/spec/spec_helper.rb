require_relative '../lib/gitaly_server.rb'
require_relative '../lib/gitlab/git.rb'
require_relative 'support/sentry.rb'
require 'test_repo_helper'

Dir[File.join(__dir__, 'support/helpers/*.rb')].each { |f| require f }

ENV['GITALY_RUBY_GIT_BIN_PATH'] ||= 'git'
