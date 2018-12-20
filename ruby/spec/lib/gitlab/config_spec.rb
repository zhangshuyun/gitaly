require 'spec_helper'

describe Gitlab::Config do
  describe '#gitlab_shell' do
    subject { described_class.new.gitlab_shell }

    let(:gitlab_shell_path) { '/foo/bar/gitlab-shell' }

    before do
      allow(ENV).to receive(:[]).with('GITALY_RUBY_GITLAB_SHELL_PATH').and_return(gitlab_shell_path)
    end

    it { expect(subject.path).to eq(gitlab_shell_path) }
  end
end
