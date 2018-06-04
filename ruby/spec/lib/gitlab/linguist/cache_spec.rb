require 'spec_helper'

describe Gitlab::Linguist::Cache do
  include TestRepo

  let(:repository) { gitlab_git_from_gitaly(new_mutable_test_repo) }
  let(:old_stats) { [{ 'foo.rb' => 'Ruby'}, { 'bar.go' => 'Go' }] }
  let(:linguist) { double('linguist', cache: old_stats) }

  subject { described_class.new(repository.path) }

  describe '#write' do
    it 'writes the cache in the Gitaly cache directory' do
      subject.write(linguist, '0' * 40)

      expect(File.exist?(File.join(repository.path, 'gitaly', 'linguist-cache'))).to be(true)
    end
  end

  describe 'old_stats' do
    context 'when there is no cache yet' do
      it 'returns nil' do
        expect(subject.old_stats).to be_nil
      end

    end

    context 'when the cache has been written' do
      before do
        subject.write(linguist, '0' * 40)

        expect(subject.old_stats).not_to be_nil
        expect(subject.old_stats).to eq('0' * 40)
      end
    end
  end
end
