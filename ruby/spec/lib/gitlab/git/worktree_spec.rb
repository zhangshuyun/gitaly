# frozen_string_literal: true

require 'spec_helper'

describe Gitlab::Git::Worktree do
  context '#initialize' do
    let(:repo_path) { '/tmp/test' }
    let(:prefix) { 'rebase' }

    it 'generates valid path' do
      worktree = described_class.new(repo_path, prefix, 12345)

      expect(worktree.path).to eq('/tmp/test/gitlab-worktree/rebase-12345')
    end

    it 'rejects bad IDs' do
      expect { described_class.new(repo_path, prefix, '') }.to raise_error(ArgumentError)
      expect { described_class.new(repo_path, prefix, '/test/me') }.to raise_error(ArgumentError)
    end
  end
end
