# frozen_string_literal: true

require 'spec_helper'

describe Gitlab::Git::RepositoryMirroring do
  class FakeRepository
    include Gitlab::Git::RepositoryMirroring

    def initialize(projects_stub)
      @gitlab_projects = projects_stub
    end

    def gitlab_projects_error
      raise Gitlab::Git::CommandError, @gitlab_projects.output
    end
  end

  describe '#push_remote_branches' do
    let(:projects_stub) { double.as_null_object }

    subject(:repository) { FakeRepository.new(projects_stub) }

    context 'with a successful first push' do
      it 'returns true' do
        expect(projects_stub).to receive(:push_branches)
          .with('remote_a', anything, true, %w[master], env: {})
          .once
          .and_return(true)

        expect(projects_stub).not_to receive(:output)

        expect(repository.push_remote_branches('remote_a', %w[master])).to eq(true)
      end
    end

    context 'with a failed push' do
      it 'logs a list of branch results and raises CommandError' do
        output = "Oh no, push mirroring failed!"
        logger = spy

        allow(projects_stub).to receive(:output).once.and_return(output)
        allow(projects_stub).to receive(:logger).and_return(logger)

        push_results = double(
          accepted_branches: %w[develop],
          rejected_branches: %w[master]
        )
        expect(Gitlab::Git::PushResults).to receive(:new)
          .with(output)
          .and_return(push_results)

        # Cause an exception via gitlab_projects_error
        expect(projects_stub).to receive(:push_branches).and_return(false)

        # The CommandError gets re-raised, matching existing behavior
        expect { repository.push_remote_branches('remote_a', %w[master develop]) }
          .to raise_error(Gitlab::Git::CommandError, output)

        # Ensure we logged a message with the PushResults info
        expect(logger).to have_received(:info).with(%r{Accepted: develop / Rejected: master})
      end
    end
  end
end
