require 'spec_helper'

describe Gitaly::RepositoryService do
  include IntegrationClient
  include TestRepo

  subject { gitaly_stub(:RepositoryService) }

  describe 'RepositoryExists' do
    it 'returns false if the repository does not exist' do
      request = Gitaly::RepositoryExistsRequest.new(repository: gitaly_repo('default', 'foobar.git'))
      response = subject.repository_exists(request)
      expect(response.exists).to eq(false)
    end

    it 'returns true if the repository exists' do
      request = Gitaly::RepositoryExistsRequest.new(repository: test_repo_read_only)
      response = subject.repository_exists(request)
      expect(response.exists).to eq(true)
    end
  end
end
