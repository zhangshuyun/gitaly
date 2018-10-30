require 'spec_helper'

describe Gitlab::Git::GitalyRemoteRepository do
  include TestRepo

  let(:gitaly_repository) { git_test_repo_read_only }
  let(:token) { 'foobar' }
  let(:gitaly_servers) do
    Base64.strict_encode64({ 'default' => { 'token' => token } }.to_json)
  end
  let(:call) { double(:call, metadata: { 'gitaly-servers' => gitaly_servers }) }
  let(:gitaly_remote_repository) do
    described_class.new(gitaly_repository, call)
  end

  describe '#request_kwargs' do
    subject(:auth_token) do
      gitaly_remote_repository.send(:request_kwargs)[:metadata]['authorization']
    end

    it 'generates a v2 authentication token' do
      expect(auth_token).to match(/Bearer v2\..+/)
    end

    context 'when the token is already a v2 authentication token' do
      let(:token) { 'v2.foo.bar' }

      it 'includes the authentication token' do
        expect(auth_token).to match(token)
      end
    end
  end
end
