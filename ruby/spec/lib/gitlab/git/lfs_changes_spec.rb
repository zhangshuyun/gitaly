require 'spec_helper'

describe Gitlab::Git::LfsChanges do
  include TestRepo

  let(:repository) { gitlab_git_from_gitaly(new_mutable_test_repo) }
  let(:newrev) { '54fcc214b94e78d7a41a9a8fe6d87a5e59500e51' }
  let(:blob_object_id) { '0c304a93cb8430108629bbbcaa27db3343299bc0' }

  subject { described_class.new(repository, newrev) }

  describe '#new_pointers' do
    it 'filters new objects to find lfs pointers' do
      expect(subject.new_pointers(not_in: []).first.id).to eq(blob_object_id)
    end

    it 'limits new_objects using object_limit' do
      expect(subject.new_pointers(object_limit: 1)).to eq([])
    end
  end
end
