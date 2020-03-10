# frozen_string_literal: true

require 'spec_helper'

describe Gitlab::Git::PushResults do
  it 'parses porcelain output' do
    output = <<~OUTPUT
      To gitlab.com:gitlab-org/security/gitlab-foss.git
      =\trefs/heads/12-5-stable:refs/heads/12-5-stable\t[up to date]
      =\trefs/heads/12-6-stable:refs/heads/12-6-stable\t[up to date]
      *\trefs/heads/rs-some-new-branch:refs/heads/rs-some-new-branch\t[new branch]
       \trefs/heads/rs-fast-forward:refs/heads/rs-fast-forward\t[fast-forward]
      -\trefs/heads/rs-deleted:refs/heads/rs-deleted\t[deleted]
      +\trefs/heads/rs-forced:refs/heads/rs-forced\t[forced]
      !\trefs/heads/12-7-stable:refs/heads/12-7-stable\t[rejected] (fetch first)
      *\trefs/tags/v1.2.3:refs/tags/v1.2.3\t[new tag]
      Done
      error: failed to push some refs to 'git@gitlab.com:gitlab-org/security/gitlab-foss.git'
      hint: Updates were rejected because the remote contains work that you do
      hint: not have locally. This is usually caused by another repository pushing
      hint: to the same ref. You may want to first integrate the remote changes
      hint: (e.g., 'git pull ...') before pushing again.
      hint: See the 'Note about fast-forwards' in 'git push --help' for details.
    OUTPUT

    results = described_class.new(output)

    expect(results.all.size).to eq(8)
    expect(results.accepted_refs).to contain_exactly(
      'rs-some-new-branch',
      'rs-fast-forward',
      'rs-forced',
      'rs-deleted',
      'v1.2.3'
    )
    expect(results.rejected_refs).to contain_exactly('12-7-stable')
  end

  it 'ignores non-porcelain output' do
    output = <<~OUTPUT
      remote: GitLab: You are not allowed to force push code to a protected branch on this project.
      To
      ! [remote rejected]         12-5-stable -> 12-5-stable (pre-receive hook declined)
      ! [remote rejected]         12-6-stable -> 12-6-stable (pre-receive hook declined)
      ! [remote rejected]         12-7-stable -> 12-7-stable (pre-receive hook declined)
      ! [remote rejected]         master -> master (pre-receive hook declined)
      error: failed to push some refs to '[FILTERED]@gitlab.com/gitlab-org/security/gitlab-foss.git'
    OUTPUT

    expect(described_class.new(output).all).to eq([])
  end

  it 'handles output without any recognizable flags' do
    output = <<~OUTPUT
      To gitlab.com:gitlab-org/security/gitlab-foss.git
      Done
      hint: Updates were rejected because the remote contains work that you do
      hint: not have locally. This is usually caused by another repository pushing
      hint: to the same ref. You may want to first integrate the remote changes
      hint: (e.g., 'git pull ...') before pushing again.
      hint: See the 'Note about fast-forwards' in 'git push --help' for details.
    OUTPUT

    expect(described_class.new(output).all).to eq([])
  end

  it 'handles invalid output' do
    output = 'You get nothing!'

    expect(described_class.new(output).all).to eq([])
  end

  describe Gitlab::Git::PushResults::Result do
    describe '#ref_name' do
      it 'deletes only one prefix' do
        # It's valid (albeit insane) for a branch to be named `refs/tags/foo`
        ref_name = 'refs/heads/refs/tags/branch'
        result = described_class.new('!', ref_name, ref_name, '')

        expect(result.ref_name).to eq('refs/tags/branch')
      end
    end
  end
end
