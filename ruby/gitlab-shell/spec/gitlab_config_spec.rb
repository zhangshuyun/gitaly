require_relative 'spec_helper'
require_relative '../lib/gitlab_config'
require 'json'

describe GitlabConfig do
  let(:config) { GitlabConfig.new }
  let(:config_data) { {'secret_file' => 'path/to/secret/file',
          'custom_hooks_dir' => '/path/to/custom_hooks',
          'gitlab_url' => 'http://localhost:123454',
          'http_settings' => {'user' => 'user_123', 'password' =>'password123', 'ca_file' => '/path/to/ca_file', 'ca_path' => 'path/to/ca_path', 'read_timeout' => 200, 'self_signed' => true},
          'log_path' => '/path/to/log',
          'log_level' => 'myloglevel',
          'log_format' => 'log_format'} }

  before do
   allow(ENV).to receive(:fetch).with('GITALY_GITLAB_SHELL_CONFIG', '{}').and_return(config_data.to_json)
  end

  describe '#secret_file' do
    it 'returns the correct secret_file' do
      expect(config.secret_file).to eq(config_data['secret_file'])
    end
  end

  describe '#custom_hooks_dir' do
    it 'returns the correct custom_hooks_dir' do
      expect(config.custom_hooks_dir).to eq(config_data['custom_hooks_dir'])
    end
  end

  describe '#http_settings' do
    it 'returns the correct http_settings' do
      expect(config.http_settings.settings).to eq(config_data['http_settings'])
    end
  end

  describe '#gitlab_url' do
    it 'returns the correct gitlab_url' do
      expect(config.gitlab_url).to eq(config_data['gitlab_url'])
    end
  end

  describe '#log_path' do
    it 'returns the correct log_path' do
      expect(config.log_file).to eq(File.join(config_data['log_path'], 'gitlab-shell.log'))
    end
  end

  describe '#log_level' do
    it 'returns the correct log_level' do
      expect(config.log_level).to eq(config_data['log_level'])
    end
  end

  describe '#log_format' do
    it 'returns the correct log_format' do
      expect(config.log_format).to eq(config_data['log_format'])
    end
  end
end
