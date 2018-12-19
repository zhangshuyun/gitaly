require 'rspec-parameterized'
require 'simplecov'
SimpleCov.start

ENV['GITLAB_SHELL_DIR'] = File.expand_path('..', __dir__)

require 'gitlab_init'

Dir[File.expand_path('support/**/*.rb', __dir__)].each { |f| require f }

RSpec.configure do |config|
  config.run_all_when_everything_filtered = true
  config.filter_run :focus
end
