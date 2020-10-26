require 'spec_helper'

describe Gitlab::Config do
  describe 'Logging' do
    subject { described_class::Logging.new }

    describe '#dir' do
      context 'when GITALY_LOG_DIR is set' do
        it 'uses the setting' do
          allow(ENV).to receive(:[]).with('GITALY_LOG_DIR').and_return('/path/to/logs')

          expect(subject.dir).to eq('/path/to/logs')
        end
      end

      context 'when GITALY_LOG_DIR is blank' do
        it 'uses a default directory' do
          allow(ENV).to receive(:[]).with('GITALY_LOG_DIR').and_return('')

          expect(subject.dir).not_to eq('')
        end
      end
    end
  end

  describe '#gitlab_shell' do
    subject { described_class.new.gitlab_shell }

    let(:gitlab_shell_path) { '/foo/bar/gitlab-shell' }

    context 'when GITALY_GITLAB_SHELL_DIR is set' do
      before do
        allow(ENV).to receive(:[]).with('GITALY_GITLAB_SHELL_DIR').and_return(gitlab_shell_path)
        allow(ENV).to receive(:[]).with('GITLAB_SHELL_DIR').and_return(nil)
      end

      specify { expect(subject.path).to eq(gitlab_shell_path) }
    end

    context 'when GITLAB_SHELL_DIR is set' do
      before do
        allow(ENV).to receive(:[]).with('GITALY_GITLAB_SHELL_DIR').and_return(nil)
        allow(ENV).to receive(:[]).with('GITLAB_SHELL_DIR').and_return(gitlab_shell_path)
      end

      specify { expect(subject.path).to eq(gitlab_shell_path) }
    end
  end
end
