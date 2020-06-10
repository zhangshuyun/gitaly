require_relative 'spec_helper'
require_relative '../lib/http_helper'

class GitlabNet
  include HTTPHelper

  def client(uri)
    http_client_for(uri)
  end
end

describe HTTPHelper do
  let(:config_data) { {'http_settings' => { 'user' => 'user_123', 'password' =>'password123', 'ca_file' => '', 'ca_path' => '', 'read_timeout' => 200, 'self_signed' => true } } }

  before do
    allow(ENV).to receive(:fetch).with('GITALY_GITLAB_SHELL_CONFIG', '{}').and_return(config_data.to_json)
  end

  it 'creates an https client when ca_file and ca_path are empty' do
    gitlab_net = GitlabNet.new

    expect { gitlab_net.client(URI('https://localhost:8080')) }.not_to raise_error
  end
end
