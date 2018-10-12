require 'securerandom'
require 'spec_helper'

describe Gitaly::RefService do
  include IntegrationClient
  include TestRepo

  let(:service_stub) { gitaly_stub(:RefService) }

  describe 'CreateBranch' do
    it 'can create a branch' do
      repo = new_mutable_test_repo
      branch_name = 'branch-' + SecureRandom.hex(10)
      request = Gitaly::CreateBranchRequest.new(
        repository: repo,
        name: branch_name,
        start_point: 'master'
      )

      response = service_stub.create_branch(request)

      expect(response.status).to eq(:OK)

      # Intentionally instatiate this Rugged::Repository after we performed
      # the RPC, to ensure we don't see stale repository state.
      rugged = rugged_from_gitaly(repo)

      expect(response.branch.target_commit.id).to eq(rugged.branches[branch_name].target_id)
    end
  end
end
